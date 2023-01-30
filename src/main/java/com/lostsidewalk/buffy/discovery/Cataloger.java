package com.lostsidewalk.buffy.discovery;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.lostsidewalk.buffy.DataAccessException;
import com.lostsidewalk.buffy.DataUpdateException;
import com.lostsidewalk.buffy.RenderedCatalogDao;
import com.lostsidewalk.buffy.RenderedThumbnailDao;
import com.lostsidewalk.buffy.discovery.utils.ThumbnailUtils;
import com.lostsidewalk.buffy.model.RenderedFeedDiscoveryInfo;
import com.lostsidewalk.buffy.model.RenderedThumbnail;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.actuate.health.Health;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;

import static com.lostsidewalk.buffy.discovery.FeedDiscoveryInfo.*;
import static java.util.Optional.ofNullable;
import static java.util.concurrent.Executors.newFixedThreadPool;
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

    @Value("${newsgears.thumbnail.size}")
    int thumbnailSize;

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
                FeedDiscoveryInfo updated = discoverUrl(discoverable.getFeedUrl());
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
                discoverable.setError(e.exceptionType);
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
                        ThumbnailedFeedDiscoveryImage feedImage = addThumbnail(fd.getImage());
                        ThumbnailedFeedDiscoveryImage feedIcon = addThumbnail(fd.getIcon());
                        ThumbnailedFeedDiscovery tfd = ThumbnailedFeedDiscovery.from(fd, feedImage, feedIcon);
                        persistFeedDiscoveryInfo(fd);
                        if (fd.error == null) {
                            deployFeedDiscoveryInfo(tfd);
                        }
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

    private ThumbnailedFeedDiscoveryImage addThumbnail(FeedDiscoveryImageInfo imageInfo) throws DataAccessException {
        if (imageInfo != null) {
            byte[] image = buildThumbnail(imageInfo);
            return image == null ? null : ThumbnailedFeedDiscoveryImage.from(imageInfo, image);
        }

        return null;
    }

    private byte[] buildThumbnail(FeedDiscoveryImageInfo imageInfo) throws DataAccessException {
        if (isNotBlank(imageInfo.getUrl())) {
            String transportIdent = imageInfo.getTransportIdent();
            byte[] image = ofNullable(getThumbnail(transportIdent)).map(RenderedThumbnail::getImage).orElse(null);
            if (image == null) {
                image = ofNullable(refreshThumbnail(transportIdent, imageInfo.getUrl()))
                        .map(RenderedThumbnail::getImage)
                        .orElse(null);
            }
            return image;
        }

        return null;
    }

    private RenderedThumbnail getThumbnail(String transportIdent) throws DataAccessException {
        if (isBlank(transportIdent)) {
            return null;
        }
        log.debug("Attempting to locate thumbnail at transportIdent={}", transportIdent);
        RenderedThumbnail thumbnail = renderedThumbnailDao.findThumbnailByTransportIdent(transportIdent);
        if (thumbnail != null) {
            log.debug("Thumbnail located at transportIdent={}", transportIdent);
        } else {
            log.debug("Unable to locate thumbnail at transportIdent={}", transportIdent);
        }

        return thumbnail;
    }

    private RenderedThumbnail refreshThumbnail(String transportIdent, String imgUrl) {
        log.info("Refreshing thumbnail cache, imgUrl={} @ transportIdent={}", imgUrl, transportIdent);
        RenderedThumbnail thumbnail = null;
        try {
            byte[] imageBytes = ThumbnailUtils.getImage(imgUrl, fetch(imgUrl), this.thumbnailSize);
            if (imageBytes == null) {
                log.error("Failed to decode image at imgUrl={} @ transportIdent={} due to unknown format", imgUrl, transportIdent);
            }
            thumbnail = RenderedThumbnail.from(transportIdent, imageBytes);
            renderedThumbnailDao.putThumbnailAtTransportIdent(transportIdent, thumbnail);
            log.debug("Thumbnail cache updated for imgUrl={} @ transportIdent={}", imgUrl, transportIdent);
        } catch (Exception e) {
            log.warn("Failed to update thumbnail cache for imgUrl={} @ transportIdent={} due to: {}",
                    imgUrl, transportIdent, e.getMessage());
        }

        return thumbnail;
    }

    private byte[] fetch(String url) throws IOException {
        URL feedUrl = new URL(url);
        URLConnection feedConnection = feedUrl.openConnection();
        // TODO: make this property-configurable
        String userAgent = "Lost Sidewalk FeedGears RSS Aggregator v.0.2 periodic feed catalog update";
        feedConnection.setRequestProperty("User-Agent", userAgent);
        return feedConnection.getInputStream().readAllBytes();
    }

    private void persistFeedDiscoveryInfo(FeedDiscoveryInfo feedDiscoveryInfo) throws DataAccessException, DataUpdateException {
        log.debug("Persisting feed discovery info from URL={}, error={}", feedDiscoveryInfo.getFeedUrl(), feedDiscoveryInfo.getError());
        feedDiscoveryInfoDao.update(feedDiscoveryInfo);
    }

    private void deployFeedDiscoveryInfo(ThumbnailedFeedDiscovery thumbnailedFeedDiscovery) throws DataAccessException {
        log.debug("Deploying feed discovery info from URL={}", thumbnailedFeedDiscovery.getFeedUrl());
        renderedCatalogDao.update(RenderedFeedDiscoveryInfo.from(thumbnailedFeedDiscovery));
    }
}
