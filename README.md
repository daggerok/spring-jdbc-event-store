# spring-jdbc-event-store [![Java CI with Maven](https://github.com/daggerok/spring-jdbc-event-store/actions/workflows/ci.yaml/badge.svg)](https://github.com/daggerok/spring-jdbc-event-store/actions/workflows/ci.yaml)
Checkpoint app based on CQRS and Event sourcing using Spring Boot, Spring MVC, NamedParameterJDBCTemplate, H2 and integration testing based on Java 11 builtin Http Client

## Test

```bash
./mvnw clean test
```

## Build and Run

```bash
./mvnw -DskipTests

java -jar target/spring-jdbc-event-store-0.0.0-SNAPSHOT.jar

http :8080/api/v1/register-visitor aggregateId=1-1-1-1-1 name=test ; \
  http :8080/api/v1/deliver-pass-card aggregateId=1-1-1-1-1 ; \
  http :8080/api/v1/enter-the-door aggregateId=1-1-1-1-1 doorId=1 ; \
  http :8080/api/v1/enter-the-door aggregateId=1-1-1-1-1 doorId=2 ; \
  http :8080/api/v1/enter-the-door aggregateId=1-1-1-1-1 doorId=3 ; \
  http :8080/api/v1/enter-the-door aggregateId=1-1-1-1-1 doorId=4 ; \
  http :8080/api/v1/enter-the-door aggregateId=1-1-1-1-1 doorId=5 ; \
  http :8080/api/v1/enter-the-door aggregateId=1-1-1-1-1 doorId=4 ; \
  http :8080/api/v1/enter-the-door aggregateId=1-1-1-1-1 doorId=3 ; \
  http :8080/api/v1/enter-the-door aggregateId=1-1-1-1-1 doorId=2 ; \
  http :8080/api/v1/enter-the-door aggregateId=1-1-1-1-1 doorId=1 ;
```

## H2 console

Open http://127.0.0.1:8080/h2-console/

## RTFM

### Reference Documentation

For further reference, please consider the following sections:

* [Official Apache Maven documentation](https://maven.apache.org/guides/index.html)
* [Spring Boot Maven Plugin Reference Guide](https://docs.spring.io/spring-boot/docs/2.4.4/maven-plugin/reference/html/)
* [Create an OCI image](https://docs.spring.io/spring-boot/docs/2.4.4/maven-plugin/reference/html/#build-image)
* [Spring Configuration Processor](https://docs.spring.io/spring-boot/docs/2.4.4/reference/htmlsingle/#configuration-metadata-annotation-processor)
* [JDBC API](https://docs.spring.io/spring-boot/docs/2.4.4/reference/htmlsingle/#boot-features-sql)

### Guides

The following guides illustrate how to use some features concretely:

* [Accessing Relational Data using JDBC with Spring](https://spring.io/guides/gs/relational-data-access/)
* [Managing Transactions](https://spring.io/guides/gs/managing-transactions/)
