# Boomerang Eventing Library <!-- omit in toc -->

## Table of Contents

- [Table of Contents](#table-of-contents)
- [Description](#description)
- [Dependencies](#dependencies)
  - [Compile Maven Dependencies](#compile-maven-dependencies)
  - [Test Maven Dependencies](#test-maven-dependencies)
- [Install](#install)
- [Known Issues](#known-issues)
- [Contributing](#contributing)
- [License](#license)

## Description

Boomerang Eventing Library is a Maven plugin that implements event streaming technologies, currently supporting [NATS Jetstream][1] only. This plugin is meant to be integrated into [Spring Boot][2] enabled applications.

The plugin has been built on top of [`jnats`][4] client library targeting an easier integration with NATS Jetstream for the purposes of Boomerang platform.

## Dependencies

### Compile Maven Dependencies

1. Spring Context ([`spring-context`][3])
2. Jnats ([`jnats`][4])
3. Apache Log4j Core ([`log4j-core`][5])

### Test Maven Dependencies

1. JUnit ([`junit`][6])

## Install

Boomerang Eventing plugin is published to [GitHub Packages][7]. To install the package from GitHub's Apache Maven registry, edit the `pom.xml` file to include the package as a dependency:

```xml
<dependency>
  <groupId>io.boomerang</groupId>
  <artifactId>lib-jetstream</artifactId>
  <version>0.0.2</version>
</dependency>
```

Additionally, you need to authenticate to GitHub Packages. For more information, see ["Authenticating to GitHub Packages"][8].

Once you have set up the authentication to GitHub Packages and the plugin package has been added to your project's `pom.xml` file, you can install the package with:

```bash
mvn install
```

## Known Issues

N/A.

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

## License

All of our work is licenses under the [Apache License Version 2.0][9] unless specified otherwise due to constraints by dependencies.

[1]: https://docs.nats.io/jetstream/jetstream "About JetStream"
[2]: https://spring.io/projects/spring-boot "Spring Boot"
[3]: https://mvnrepository.com/artifact/org.springframework/spring-context "Maven Repository - Spring Context"
[4]: https://mvnrepository.com/artifact/io.nats/jnats "Maven Repository - Jnats"
[5]: https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-core "Maven Repository - The Apache Log4j Implementation"
[6]: https://mvnrepository.com/artifact/junit/junit "Maven Repository – JUnit is a unit testing framework for Java"
[7]: https://docs.github.com/en/packages "GitHub Packages"
[8]: https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry#authenticating-to-github-packages "Authenticating to GitHub Packages"
[9]: https://www.apache.org/licenses/LICENSE-2.0 "Apache License Version 2.0"
