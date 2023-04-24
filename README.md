[![Contributors][contributors-shield]][contributors-url]
[![Forks][forks-shield]][forks-url]
[![Stargazers][stars-shield]][stars-url]
[![Issues][issues-shield]][issues-url]
[![License][license-shield]][license-url]

<a name="readme-top"></a>

<!-- PROJECT LOGO -->
<br />
<div align="center">
  <a href="https://github.com/lostsidewalk/newsgears-cataloger">
    <img src="images/logo.png" alt="Logo" width="144" height="144" style="box-shadow: 2px 2px 2px rgba(64,64,64,0.7)">
  </a>

<h3 align="center">FeedGears RSS</h3>
 <p align="center">
    FeedGears RSS is a web-based RSS aggregator/reader platform.
    <br />
    <br />
    <a href="https://github.com/lostsidewalk/newsgears-cataloger/issues">Report Bug</a>
    ·
    <a href="https://github.com/lostsidewalk/newsgears-cataloger/issues">Request Feature</a>
  </p>
</div>



<!-- TABLE OF CONTENTS -->
<details>
  <summary>Table of Contents</summary>
  <ol>
    <li>
      <a href="#about-the-project">About The Project</a>
      <ul>
        <li><a href="#built-with">Built With</a></li>
      </ul>
    </li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#contributing">Contributing</a></li>
    <li><a href="#license">License</a></li>
    <li><a href="#contact">Contact</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>


<!-- ABOUT THE PROJECT -->
## About The Project

https://www.feedgears.com

FeedGears is a modern, free/libre, web-based RSS reader and aggregator platform. I built FeedGears with the goal of maintaining and advancing public interest in RSS, a unique and fun way of exploring the Internet. The world deserves free, secure, private, and inclusive access to RSS. This project is funded entirely by users, such as myself, and donors like you! FeedGears will always be free as in, and free as in freedom.

What is RSS?

RSS (RDF Site Summary or Really Simple Syndication) is a web feed that allows users and applications to access updates to websites in a standardized, computer-readable format. Subscribing to RSS feeds can allow a user to keep track of many different websites in a single news aggregator, which constantly monitors sites for new content, removing the need for the user to manually check them.

FeedGears is an RSS aggregator

The cloud-hosted version, www.feedgears.com, tracks thousands of feeds daily on behalf of our users. We import tens of thousands or articles, and organize and present them to people around the world to read, search, filter, etc. in a highly customizable and accessible way. Since FeedGears is entirely free, you can host your own instance using pre-built containers, using the instructions located here.

This repository contains the cataloger.  The cataloger is a back-end component that runs periodically to track the state/availability of RSS feeds that are listed in the core DB table, `feed_discovery_info`.  Information about the last known state of these feeds is written back to `feed_discovery_info`, and this table is subsequently used to display a 'catalog' of subscribe-able feeds to the front-end users. 

For more information about FeedGears, see the parent project repository at: https://www.github.com/lostsidewalk/newsgears-app.

<p align="right">(<a href="#readme-top">back to top</a>)</p>


### Built With

newsgears-cataloger is a Java 19 library package, built with the following dependencies:

API Dependencies:
<ul>
    <li>com.lostsidewalk.newsgears:newsgears-cataloger</li>
</ul>

Implementation Dependencies:
<ul>
    <li>com.lostsidewalk.newsgears:newsgears-rss-importer</li>
    <li>org.slf4j:slf4j-api:2.0.5</li>
    <li>org.springframework.boot:spring-boot-starter-actuator:3.0.4</li>
    <li>com.fasterxml.jackson.core:jackson-annotations:2.14.2</li>
    <li>org.apache.commons:commons-collections4:4.4</li>
    <li>org.apache.commons:commons-lang3:3.12.0</li>
    <li>commons-codec:commons-codec:1.15</li>
    <li>com.google.guava:guava:31.1-jre</li>
    <li>javax.annotation:javax.annotation-api:1.3.2</li>
    <li>org.imgscalr:imgscalr-lib:4.2</li>
    <li>org.apache.httpcomponents:httpclient:4.5.14</li>
</ul>

<p align="right">(<a href="#readme-top">back to top</a>)</p>


<!-- USAGE EXAMPLES -->
## Usage

```
dependencies {
    api 'com.lostsidewalk.newsgears:newsgears-cataloger:0.4'
}
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>


See the [open issues](https://github.com/lostsidewalk/newsgears-cataloger/issues) for a full list of known issues/proposed features.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- CONTRIBUTING -->
## Contributing

Contributions are what make the open source community such an amazing place to learn, inspire, and create. Any contributions you make are **greatly appreciated**.

If you have a suggestion that would make this better, please fork the repo and create a pull request. You can also simply open an issue with the tag "enhancement".


1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/newFeature`)
3. Commit your Changes (`git commit -m 'Add some newFeature'`)
4. Push to the Branch (`git push origin feature/newFeature`)
5. Open a Pull Request

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- LICENSE -->
## License

Distributed under the GPL V3 License. See `LICENSE` for more information.

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- CONTACT -->
## Contact

meh - [@lostsidewalkllc](https://twitter.com/lostsidewalkllc) - meh@lostsidewalk.com

Project Link: [https://github.com/lostsidewalk/newsgears-cataloger](https://github.com/lostsidewalk/newsgears-cataloger)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- ACKNOWLEDGMENTS -->
## Acknowledgments

* [Project ROME](https://github.com/rometools)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

<!-- MARKDOWN LINKS & IMAGES -->
<!-- https://www.markdownguide.org/basic-syntax/#reference-style-links -->
[contributors-shield]: https://img.shields.io/github/contributors/lostsidewalk/newsgears-cataloger.svg?style=for-the-badge
[contributors-url]: https://github.com/lostsidewalk/newsgears-cataloger/graphs/contributors
[forks-shield]: https://img.shields.io/github/forks/lostsidewalk/newsgears-cataloger.svg?style=for-the-badge
[forks-url]: https://github.com/lostsidewalk/newsgears-cataloger/network/members
[stars-shield]: https://img.shields.io/github/stars/lostsidewalk/newsgears-cataloger.svg?style=for-the-badge
[stars-url]: https://github.com/lostsidewalk/newsgears-cataloger/stargazers
[issues-shield]: https://img.shields.io/github/issues/lostsidewalk/newsgears-cataloger.svg?style=for-the-badge
[issues-url]: https://github.com/lostsidewalk/newsgears-cataloger/issues
[license-shield]: https://img.shields.io/github/license/lostsidewalk/newsgears-cataloger.svg?style=for-the-badge
[license-url]: https://github.com/lostsidewalk/newsgears-cataloger/blob/master/LICENSE
