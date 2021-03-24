package com.github.daggerok.jdbceventstore;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.log4j.Log4j2;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.parallel.Isolated;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Log4j2
@Isolated
@DisplayName("A ERST API tests")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS) // non-static @BeforeAll
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class) // sequential tests execution
class RestApiTests {

  @LocalServerPort
  Integer port;

  @Autowired
  ObjectMapper json;

  // state
  UUID aggregateIdState;

  @Test
  @Order(1)
  @SneakyThrows
  void should_not_register_with_no_name_present() {
    // given
    var url = String.format("http://127.0.0.1:%d/api/v1/register-visitor", port);
    var payload = Map.of();
    var json = this.json.writeValueAsString(payload);
    var body = HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8);
    var request = HttpRequest.newBuilder()
                             .uri(URI.create(url))
                             .header("Content-Type", "application/json")
                             .POST(body)
                             .timeout(Duration.ofSeconds(5))
                             .build();
    // when
    var client = HttpClient.newBuilder()
                           .version(HttpClient.Version.HTTP_1_1)
                           .build();
    var responseBodyHandler = HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8);
    var response = client.send(request, responseBodyHandler);
    // then
    var map = this.json.readValue(response.body(), Map.class);
    log.info("{} {}", () -> response, () -> map);
    assertThat(map).isNotNull()
                   .isNotEmpty()
                   .doesNotContainKeys("result")
                   .containsKeys("error");
    var o = map.get("error");
    assertThat(o).isExactlyInstanceOf(String.class);
    var error = String.valueOf(o);
    assertThat(error).isNotNull()
                     .isNotEmpty()
                     .isEqualTo("Name may not be null");
  }

  @Test
  @Order(2)
  @SneakyThrows
  void should_register_visitor() {
    // given
    var url = String.format("http://127.0.0.1:%d/api/v1/register-visitor", port);
    var payload = Map.of("name", "REST API test");
    var json = this.json.writeValueAsString(payload);
    var body = HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8);
    var request = HttpRequest.newBuilder()
                             .uri(URI.create(url))
                             .header("Content-Type", "application/json")
                             .POST(body)
                             .timeout(Duration.ofSeconds(5))
                             .build();
    // when
    var client = HttpClient.newBuilder()
                           .version(HttpClient.Version.HTTP_1_1)
                           .build();
    var responseBodyHandler = HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8);
    var response = client.send(request, responseBodyHandler);
    // then
    var map = this.json.readValue(response.body(), Map.class);
    log.info("{} {}", () -> response, () -> map);
    assertThat(map).isNotNull()
                   .isNotEmpty()
                   .containsKeys("result")
                   .doesNotContainKeys("error");
    var o = map.get("result");
    assertThat(o).isExactlyInstanceOf(String.class);
    var aggregateIdStr = String.valueOf(o);
    this.aggregateIdState = UUID.fromString(aggregateIdStr);
  }

  @Test
  @Order(3)
  @SneakyThrows
  void should_not_register_already_registered_visitor() {
    // given
    var url = String.format("http://127.0.0.1:%d/api/v1/register-visitor", port);
    var payload = Map.of("name", "ololo",
                         "aggregateId", this.aggregateIdState);
    var json = this.json.writeValueAsString(payload);
    var body = HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8);
    var request = HttpRequest.newBuilder()
                             .uri(URI.create(url))
                             .header("Content-Type", "application/json")
                             .POST(body)
                             .timeout(Duration.ofSeconds(5))
                             .build();
    // when
    var client = HttpClient.newBuilder()
                           .version(HttpClient.Version.HTTP_1_1)
                           .build();
    var responseBodyHandler = HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8);
    var response = client.send(request, responseBodyHandler);
    // then
    var map = this.json.readValue(response.body(), Map.class);
    log.info("{} {}", () -> response, () -> map);
    assertThat(map).isNotNull()
                   .isNotEmpty()
                   .doesNotContainKeys("result")
                   .containsKeys("error");
    var o = map.get("error");
    assertThat(o).isExactlyInstanceOf(String.class);
    var error = String.valueOf(o);
    assertThat(error).isNotNull()
                     .isNotEmpty()
                     .isEqualTo("Visitor already registered");
  }

  @Test
  @Order(4)
  @SneakyThrows
  void should_not_enter_the_door_if_pass_card_has_not_been_delivered() {
    // given
    var url = String.format("http://127.0.0.1:%d/api/v1/enter-the-door", port);
    var payload = Map.of("aggregateId", this.aggregateIdState,
                         "doorId", "IN-1");
    var json = this.json.writeValueAsString(payload);
    var body = HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8);
    var request = HttpRequest.newBuilder()
                             .uri(URI.create(url))
                             .header("Content-Type", "application/json")
                             .POST(body)
                             .timeout(Duration.ofSeconds(5))
                             .build();
    // when
    var client = HttpClient.newBuilder()
                           .version(HttpClient.Version.HTTP_1_1)
                           .build();
    var responseBodyHandler = HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8);
    var response = client.send(request, responseBodyHandler);
    // then
    var map = this.json.readValue(response.body(), Map.class);
    log.info("{} {}", () -> response, () -> map);
    assertThat(map).isNotNull()
                   .isNotEmpty()
                   .doesNotContainKeys("result")
                   .containsKeys("error");
    var o = map.get("error");
    assertThat(o).isExactlyInstanceOf(String.class);
    var error = String.valueOf(o);
    assertThat(error).isNotNull()
                     .isNotEmpty()
                     .isEqualTo("Pass card hasn't been delivered to " + this.aggregateIdState);
  }

  @Test
  @Order(5)
  @SneakyThrows
  void should_deliver_pass_card() {
    // given
    var url = String.format("http://127.0.0.1:%d/api/v1/deliver-pass-card", port);
    var payload = Map.of("aggregateId", this.aggregateIdState);
    var json = this.json.writeValueAsString(payload);
    var body = HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8);
    var request = HttpRequest.newBuilder()
                             .uri(URI.create(url))
                             .header("Content-Type", "application/json")
                             .POST(body)
                             .timeout(Duration.ofSeconds(5))
                             .build();
    // when
    var client = HttpClient.newBuilder()
                           .version(HttpClient.Version.HTTP_1_1)
                           .build();
    var responseBodyHandler = HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8);
    var response = client.send(request, responseBodyHandler);
    // then
    var map = this.json.readValue(response.body(), Map.class);
    log.info("{} {}", () -> response, () -> map);
    assertThat(map).isNotNull()
                   .isNotEmpty()
                   .containsKeys("result")
                   .doesNotContainKeys("error");
    var o = map.get("result");
    assertThat(o).isExactlyInstanceOf(String.class);
    var aggregateId = UUID.fromString(String.valueOf(o));
    assertThat(aggregateId).isNotNull()
                           .isEqualTo(this.aggregateIdState);
  }

  @Test
  @Order(6)
  @SneakyThrows
  void should_enter_the_door() {
    // given
    var url = String.format("http://127.0.0.1:%d/api/v1/enter-the-door", port);
    var payload = Map.of("aggregateId", this.aggregateIdState,
                         "doorId", "IN-1");
    var json = this.json.writeValueAsString(payload);
    var body = HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8);
    var request = HttpRequest.newBuilder()
                             .uri(URI.create(url))
                             .header("Content-Type", "application/json")
                             .POST(body)
                             .timeout(Duration.ofSeconds(5))
                             .build();
    // when
    var client = HttpClient.newBuilder()
                           .version(HttpClient.Version.HTTP_1_1)
                           .build();
    var responseBodyHandler = HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8);
    var response = client.send(request, responseBodyHandler);
    // then
    var map = this.json.readValue(response.body(), Map.class);
    log.info("{} {}", () -> response, () -> map);
    assertThat(map).isNotNull()
                   .isNotEmpty()
                   .containsKeys("result")
                   .doesNotContainKeys("error");
    var o = map.get("result");
    assertThat(o).isExactlyInstanceOf(String.class);
    var aggregateId = UUID.fromString(String.valueOf(o));
    assertThat(aggregateId).isNotNull()
                           .isEqualTo(this.aggregateIdState);
  }
}
