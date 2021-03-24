package com.github.daggerok.jdbceventstore;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Transactional
@SpringBootTest
@DisplayName("A JDBC repository tests")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class CheckpointRepositoryTests {

  @Autowired
  CheckpointRepository repository;

  @Test
  void should_save_aggregate() {
    // given
    var aggregateId = UUID.randomUUID();
    var expireAt = LocalDateTime.now().plus(1, ChronoUnit.YEARS);
    // when
    var one = new CheckpointAggregate().registerVisitor(aggregateId, "A test", expireAt);
    var two = one.deliverPassCard(aggregateId);
    var three = two.enterTheDoor(aggregateId, "IN-1")
                   .enterTheDoor(aggregateId, "OUT-1");
    // then
    repository.save(three);
    assertThat(three.getEventStream()).isNotNull()
                                      .isEmpty();
    // and
    var maybe = repository.find(aggregateId);
    assertThat(maybe).isNotNull()
                     .isNotEmpty();
    var rebuilt = maybe.get();
    assertThat(rebuilt.getEventStream()).isNotNull()
                                        .isNotEmpty()
                                        .hasSize(4);
    var state = rebuilt.getState();
    assertThat(state.getLastDoorId()).isEqualTo("OUT-1");
    assertThat(state.getSequenceNumber()).isGreaterThan(BigInteger.valueOf(3L));
  }
}
