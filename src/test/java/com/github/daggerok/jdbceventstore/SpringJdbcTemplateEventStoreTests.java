package com.github.daggerok.jdbceventstore;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.junit.platform.commons.util.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

@Transactional
@SpringBootTest
@DisplayName("A JDBC template event store tests")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SpringJdbcTemplateEventStoreTests {

  @Autowired
  JdbcEventStore eventStore;

  @Test
  void should_append_and_stream_event() {
    // given
    var aggregateId = UUID.randomUUID();
    var expireAt = LocalDateTime.now().plus(1, ChronoUnit.DAYS);
    var visitorRegisteredEvent = VisitorRegisteredEvent.of(aggregateId, "test", expireAt);
    // when
    eventStore.appendEvents(visitorRegisteredEvent);
    // then
    var domainEvents = eventStore.streamEvents(aggregateId);
    assertThat(domainEvents).isNotNull()
                            .isNotEmpty()
                            .hasSize(1);
    var event = domainEvents.iterator().next();
    assertThat(event.getEventType()).isEqualTo(VisitorRegisteredEvent.class.getSimpleName());
  }

  @Test
  void should_append_and_stream_events() {
    // given
    var aggregateId = UUID.randomUUID();
    var expireAt = LocalDateTime.now().plus(1, ChronoUnit.DAYS);
    // when
    eventStore.appendEvents(
        VisitorRegisteredEvent.of(aggregateId, "another test", expireAt),
        PassCardDeliveredEvent.of(aggregateId)
    );
    // then
    var domainEvents = eventStore.streamEvents(aggregateId);
    assertThat(domainEvents).isNotNull();
    var iterator = domainEvents.iterator();
    assertThat(iterator.hasNext()).isTrue();
    var event1 = iterator.next();
    assertThat(event1.getEventType()).isEqualTo(VisitorRegisteredEvent.class.getSimpleName());
    assertThat(iterator.hasNext()).isTrue();
    var event2 = iterator.next();
    assertThat(event2.getEventType()).isEqualTo(PassCardDeliveredEvent.class.getSimpleName());
    assertThat(iterator.hasNext()).isFalse();
  }

  @Test
  void should_stream_events_for_snapshot() {
    // given
    var aggregateId = UUID.randomUUID();
    var expireAt = LocalDateTime.now().plus(1, ChronoUnit.DAYS);
    // and
    eventStore.appendEvents(
        VisitorRegisteredEvent.of(aggregateId, "another test", expireAt),
        PassCardDeliveredEvent.of(aggregateId)
    );
    // and
    var domainEvents = eventStore.streamEvents(aggregateId);
    var list = domainEvents.stream().collect(CollectionUtils.toUnmodifiableList());
    var last = list.get(list.size() - 1);
    assertThat(last.getSequenceNumber()).isGreaterThan(BigInteger.ZERO);
    // when
    var lastSequenceNumber = last.getSequenceNumber().subtract(BigInteger.ONE);
    var events = eventStore.streamEventsForSnapshot(aggregateId, lastSequenceNumber)
                           .collect(Collectors.toUnmodifiableList());
    assertThat(events).isNotNull()
                      .isNotEmpty()
                      .hasSize(1);
    assertThat(events.get(0)).isEqualTo(last);
  }
}
