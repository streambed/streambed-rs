# streambed-rs - An efficient and curated toolkit for writing event-driven services

_Being event-driven closes the gap between understanding a problem domain and expressing that problem in code._

Streambed is a curated set of dependencies and a toolkit for writing asynchronous event-driven services that aim to run on the 
smallest "std" targets supported by Rust. Streambed components use a single core MIPS32 OpenWrt device running at around 500MHz 
and 128MiB as a baseline target.

A commit log modelled on Apache Kafka is provided, along with partial and extended support for the Kafka HTTP API.

A secret store modelled on Hashicorp Vault is provided along with partial support for the Vault HTTP API.

Production services using the commit log and secret store have been shown to use less than 3MiB of resident memory
while also offering good performance.

## Streambed's characteristics
### Event-driven services

Event-driven services promote responsiveness as events can be pushed to where they need to be consumed; by the user
of a system. Event-driven services are also resilient to failure as they can use "event sourcing" to 
quickly rebuild their state through replaying events.

### Efficient

Streambed based applications are designed to run at the edge on embedded computers as well as in the cloud and so efficient CPU and memory usage are a primary concern.

### Secure

Security is a primary consideration throughout the design of Streambed. For example, in the world of the Internet of Things, if an individual sensor becomes compromised then its effects can be minimized.

### Built for integration

Streambed is a toolkit that promotes the consented sharing of data between many third-party applications. No more silos of data. Improved data availability leads to better decision making, which leads to better business.

### Standing on the shoulders of giants, leveraging existing communities

Streambed is an assemblage of proven approaches and technologies that already have strong communities. Should you have a problem there are many people and resources you can call on.

### Open source and open standards

Streambed is entirely open source providing cost benefits, fast-time-to-market, the avoidance of vendor lock-in, improved security and more.

### Rust

Streambed leverages Rust's characteristics of writing fast and efficient software correctly.

## A brief history and why Rust

Streambed-jvm first manifested itself as a Scala based-project with similar goals to streambed-rs and 
targeted on platforms that could run a JVM. Cisco Inc. sponsored Titan Class Pty Ltd with the development of 
streambed-jvm targeting edge-based routers on farms. Titan Class continues to run streambed at 
the edge on several Australian farms and has done so now for several years. This experience has proven out the 
event-driven approach that manifests in Streambed. It also highlighted that more energy-efficient solutions needed 
to be sought given that power at the edge is a challenge. Hence the re-writing of streambed-jvm in Rust. 

## Contribution policy

Contributions via GitHub pull requests are gladly accepted from their original author. Along with any pull requests, please state that the contribution is your original work and that you license the work to the project under the project's open source license. Whether or not you state this explicitly, by submitting any copyrighted material via pull request, email, or other means you agree to license the material under the project's open source license and warrant that you have the legal authority to do so.

## License

This code is open source software licensed under the [Apache-2.0 license](./LICENSE).

© Copyright [Titan Class P/L](https://www.titanclass.com.au/), 2022
