package com.lostsidewalk.buffy.discovery;

import com.google.common.hash.Hashing;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import com.lostsidewalk.buffy.RenderedCatalogDao;
import com.lostsidewalk.buffy.RenderedThumbnailDao;
import com.lostsidewalk.buffy.model.RenderedFeedDiscoveryInfo;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.GZIPInputStream;

import static com.lostsidewalk.buffy.discovery.FeedDiscoveryInfo.FeedDiscoveryException;
import static com.lostsidewalk.buffy.rss.RssDiscovery.*;
import static java.net.URLEncoder.encode;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.apache.commons.codec.binary.Base64.encodeBase64URLSafeString;
import static org.apache.commons.collections4.CollectionUtils.isNotEmpty;
import static org.apache.commons.collections4.CollectionUtils.size;
import static org.apache.commons.lang3.StringUtils.*;

@SuppressWarnings("unused")
@Slf4j
@Component
public class Cataloger {

    @Autowired
    FeedDiscoveryInfoDao feedDiscoveryInfoDao;

    @Autowired
    RenderedCatalogDao renderedCatalogDao;

    @Autowired
    RenderedThumbnailDao renderedThumbnailDao;

    @Value("${newsgears.userAgent")
    String feedGearsUserAgent;

    private final BlockingQueue<FeedDiscoveryInfo> discoveryQueue = new LinkedBlockingQueue<>();

    private Thread discoveryProcessor;

    private ExecutorService discoveryThreadPool;

    @PostConstruct
    public void postConstruct() {
        log.info("Cataloger constructed");
        //
        // start thread process successful imports
        //
        startDiscoveryProcessor();
        int processorCt = Runtime.getRuntime().availableProcessors() - 1;
        processorCt = processorCt > 0 ? processorCt : 1;
        log.info("Starting discovery thread pool: processCount={}", processorCt);
        this.discoveryThreadPool = newFixedThreadPool(processorCt, new ThreadFactoryBuilder().setNameFormat("cataloger-%d").build());
    }

    @SuppressWarnings("unused")
    public Health health() {
        boolean processorIsRunning = this.discoveryProcessor.isAlive();
        boolean discoveryPoolIsShutdown = this.discoveryThreadPool.isShutdown();

        if (processorIsRunning && !discoveryPoolIsShutdown) {
            return Health.up().build();
        } else {
            return Health.down()
                    .withDetail("discoveryProcessorIsRunning", processorIsRunning)
                    .withDetail("discoveryPoolIsShutdown", discoveryPoolIsShutdown)
                    .build();
        }
    }

    public void update() throws DataAccessException {
        List<FeedDiscoveryInfo> currentCatalog = feedDiscoveryInfoDao.findDiscoverable();
        CountDownLatch latch = new CountDownLatch(size(currentCatalog));
        log.info("Catalog update countdown latch size initialized to: {}", latch.getCount());
        currentCatalog.forEach(discoverable -> discoveryThreadPool.submit(() -> {
            try {
                FeedDiscoveryInfo updated = discoverUrl(discoverable.getFeedUrl(), feedGearsUserAgent);
                updated.setId(discoverable.getId());
                if (isPermanentRedirect(updated.getHttpStatusCode())) {
                    log.info("Feed permanently redirected, old URL={}, new URL={}", updated.getFeedUrl(), updated.getRedirectFeedUrl());
                    // copy redirect URL to feed URL
                    updated.setFeedUrl(updated.getRedirectFeedUrl());
                    // copy redirect HTTP status code to HTTP status code
                    updated.setHttpStatusCode(updated.getRedirectHttpStatusCode());
                    // copy redirect HTTP status message to HTTP status message
                    updated.setHttpStatusMessage(updated.getRedirectHttpStatusMessage());
                    // clear redirect fields
                    updated.setRedirectFeedUrl(null);
                    updated.setRedirectHttpStatusCode(null);
                    updated.setRedirectHttpStatusMessage(null);
                }
                if (updated.isUrlUpgradable) {
                    updated.setFeedUrl(replaceOnce(updated.getFeedUrl(), "http", "https"));
                    updated.setUrlUpgradable(false);
                    log.info("Feed upgraded to https: {}", updated.getFeedUrl());
                }
                if (!discoveryQueue.offer(updated)) {
                    log.warn("Discovery queue is at capacity; updates are being dropped");
                }
            } catch (FeedDiscoveryException e) {
                log.error("Something horrible happened while discovering URL={} due to: {}", discoverable.getFeedUrl(), e.getMessage());
                discoverable.setErrorType(e.exceptionType);
                discoverable.setErrorDetail(e.getMessage());
                discoverable.setHttpStatusCode(e.httpStatusCode);
                discoverable.setHttpStatusMessage(e.httpStatusMessage);
                discoverable.setRedirectFeedUrl(e.redirectUrl);
                discoverable.setRedirectHttpStatusCode(e.redirectHttpStatusCode);
                discoverable.setRedirectHttpStatusMessage(e.redirectHttpStatusMessage);
                if (!discoveryQueue.offer(discoverable)) {
                    log.warn("Discovery queue is at capacity; updates are being dropped");
                }
            } catch (Exception e) {
                log.error("Something unusually horrible happened while discovering URL={} due to: {}", discoverable.getFeedUrl(), e.getMessage());
                if (!discoveryQueue.offer(discoverable)) {
                    log.warn("Discovery queue is at capacity; updates are being dropped");
                }
            }
            latch.countDown();
            if (latch.getCount() % 50 == 0) {
                log.info("Catalog update latch currently at: {}", latch.getCount());
            }
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            log.error("Discovery process interrupted due to: {}", e.getMessage());
        }
    }
    //
    // discovery success processing
    //
    // TODO: this needs to be multi-threaded
    //
    private static final Logger discoveryProcessorLog = LoggerFactory.getLogger("discoveryProcessor");
    private void startDiscoveryProcessor() {
        log.info("Starting discovery processor at {}", Instant.now());
        this.discoveryProcessor = new Thread(() -> {
            int totalCt = 0;
            while (true) {
                try {
                    FeedDiscoveryInfo fd = discoveryQueue.take();
                    logForeignMarkup(fd);
                    logRedirect(fd);
                    try {
                        //
                        FeedDiscoveryImageInfo feedImage = fd.getImage();
                        secureFeedDiscoveryImageInfo(feedImage);
                        //
                        FeedDiscoveryImageInfo feedIcon = fd.getIcon();
                        secureFeedDiscoveryImageInfo(feedIcon);
                        //
                        ThumbnailedFeedDiscovery tfd = ThumbnailedFeedDiscovery.from(fd, feedImage, feedIcon, null);
                        //
                        persistFeedDiscoveryInfo(fd);
                        //
                        if (fd.errorType == null) {
                            deployFeedDiscoveryInfo(tfd);
                        }
                        //
                        totalCt++;
                    } catch (Exception e) {
                        discoveryProcessorLog.error("Something horrible happened while performing discovery on URL={}: {}", fd.getFeedUrl(), e.getMessage());
                    }
                } catch (InterruptedException ignored) {
                    // ignored
                }
                discoveryProcessorLog.debug("Discovery processor metrics: total={}", totalCt);
            }
        });
        this.discoveryProcessor.start();
    }

    private void secureFeedDiscoveryImageInfo(FeedDiscoveryImageInfo feedDiscoveryImageInfo) {
        if (feedDiscoveryImageInfo != null) {
            feedDiscoveryImageInfo.setUrl(
                    rewriteImageUrl(feedDiscoveryImageInfo.getUrl()));
        }
    }

    @Value("${newsgears.imageProxyUrlTemplate}")
    String imageProxyUrlTemplate;

    private String rewriteImageUrl(String imgUrl) {
        if (startsWith(imgUrl, "http")) {
            String imgToken = encodeBase64URLSafeString(sha256(imgUrl, UTF_8).getBytes()); // SHA-256 + B64 the URL
            return String.format(this.imageProxyUrlTemplate, strip(imgToken, "="), encode(imgUrl, UTF_8));
        }

        return EMPTY;
    }

    public static String sha256(String str, Charset charset) {
        return Hashing.sha256().hashString(str, charset).toString();
    }

    private void logForeignMarkup(FeedDiscoveryInfo feedDiscoveryInfo) {
        List<String> feedForeignMarkupStrs = feedDiscoveryInfo.getFeedForeignMarkupStrs();
        Set<String> postForeignMarkupStrs = feedDiscoveryInfo.getPostForeignMarkupStrs();
        if (isNotEmpty(feedForeignMarkupStrs) || isNotEmpty(postForeignMarkupStrs)) {
            log.warn("Foreign markup: feedTitle={}, url={}, feedForeignMarkup={}, postForeignMarkup={}",
                    feedDiscoveryInfo.getTitle(), feedDiscoveryInfo.getFeedUrl(),
                    feedForeignMarkupStrs, postForeignMarkupStrs);
        }
    }

    private void logRedirect(FeedDiscoveryInfo feedDiscoveryInfo) {
        Integer httpStatusCode = feedDiscoveryInfo.getHttpStatusCode();
        if (httpStatusCode != null && isRedirect(httpStatusCode)) {
            log.info("Discovery redirected: feedUrl={}, httpStatusCode={}, httpStatusMessage={}, redirectUrl={}, redirectHttpStatusCode={}, redirectHttpStatusMessage={}, isPermanent={}",
                    feedDiscoveryInfo.getFeedUrl(),
                    feedDiscoveryInfo.getHttpStatusCode(),
                    feedDiscoveryInfo.getHttpStatusMessage(),
                    feedDiscoveryInfo.getRedirectFeedUrl(),
                    feedDiscoveryInfo.getRedirectHttpStatusCode(),
                    feedDiscoveryInfo.getRedirectHttpStatusMessage(),
                    isPermanentRedirect(httpStatusCode)
            );
        }
    }

    private byte[] fetch(String url) throws IOException {
        URL feedUrl = new URL(url);
        URLConnection feedConnection = feedUrl.openConnection();
        // TODO: make this property-configurable
        String userAgent = "Lost Sidewalk FeedGears RSS Aggregator v.0.4 periodic feed catalog update";
        feedConnection.setRequestProperty("User-Agent", userAgent);
        feedConnection.setRequestProperty("Accept-Encoding", "gzip");
        try (InputStream is = feedConnection.getInputStream()) {
            InputStream toRead;
            if (containsIgnoreCase(feedConnection.getContentEncoding(), "gzip")) {
                toRead = new GZIPInputStream(is);
            } else {
                toRead = is;
            }
            return toRead.readAllBytes();
        }
    }

    private void persistFeedDiscoveryInfo(FeedDiscoveryInfo feedDiscoveryInfo) throws DataAccessException, DataUpdateException {
        log.debug("Persisting feed discovery info from URL={}, errorType={}, errorDetail={}", feedDiscoveryInfo.getFeedUrl(), feedDiscoveryInfo.getErrorType(), feedDiscoveryInfo.getErrorDetail());
        feedDiscoveryInfoDao.update(feedDiscoveryInfo);
    }

    private void deployFeedDiscoveryInfo(ThumbnailedFeedDiscovery thumbnailedFeedDiscovery) throws DataAccessException {
        log.debug("Deploying feed discovery info from URL={}", thumbnailedFeedDiscovery.getFeedUrl());
        renderedCatalogDao.update(RenderedFeedDiscoveryInfo.from(thumbnailedFeedDiscovery));
    }
}
