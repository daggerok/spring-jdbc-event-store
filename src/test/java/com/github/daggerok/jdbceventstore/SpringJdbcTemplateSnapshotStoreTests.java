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
@DisplayName("A JDBC template snapshot store tests")
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
class SpringJdbcTemplateSnapshotStoreTests {

  @Autowired
  JdbcSnapshotStore snapshotStore;

  @Test
  void should_save() {
    // given
    var state = new CheckpointState().setAggregateId(UUID.randomUUID())
                                     .setName("A test")
                                     .setExpireAt(LocalDateTime.now().plus(1, ChronoUnit.DAYS));
    var snapshot = CheckpointStateSnapshot.from(state);
    // when
    var maybeSaved = snapshotStore.save(snapshot);
    // then
    assertThat(maybeSaved).isNotNull()
                          .isNotEmpty();
    var savedSnapshot = maybeSaved.get();
    assertThat(savedSnapshot.getAggregateId()).isNotNull();
    assertThat(savedSnapshot.getName()).isNotNull()
                                       .isEqualTo("A test");
    assertThat(savedSnapshot.getExpireAt()).isNotNull()
                                           .isAfter(LocalDateTime.now());
    assertThat(savedSnapshot.getId()).isNotNull();
  }

  @Test
  public void should_find_first_by_aggregate_id() {
    // given
    var aggregateId = UUID.randomUUID();
    var state = new CheckpointState().setAggregateId(aggregateId)
                                     .setName("Yet another test")
                                     .setExpireAt(LocalDateTime.now().plus(1, ChronoUnit.DAYS))
                                     .setOccurredAt(LocalDateTime.now())
                                     .setSequenceNumber(BigInteger.ONE);
    var anotherState = state.setDeliveredAt(LocalDateTime.now())
                            .setOccurredAt(LocalDateTime.now())
                            .setSequenceNumber(BigInteger.valueOf(2L));
    var yetAnotherState = anotherState.setLastDoorId("IN-1")
                                      .setLastDoorEnteredAt(LocalDateTime.now())
                                      .setOccurredAt(LocalDateTime.now())
                                      .setSequenceNumber(BigInteger.valueOf(3L));
    // when
    var snapshot = CheckpointStateSnapshot.from(yetAnotherState);
    snapshotStore.save(snapshot);
    // then
    var maybeSnapshot = snapshotStore.findFirstByAggregateId(aggregateId);
    assertThat(maybeSnapshot).isNotNull()
                             .isNotEmpty();
    var foundSnapshot = maybeSnapshot.get();
    assertThat(foundSnapshot.getId()).isNotNull();
    assertThat(foundSnapshot.getAggregateId()).isNotNull()
                                              .isEqualTo(aggregateId);
    assertThat(foundSnapshot.getName()).isNotNull()
                                       .isEqualTo("Yet another test");
    assertThat(foundSnapshot.getLastDoorId()).isNotNull()
                                             .isEqualTo("IN-1");
  }

  @Test
  public void should_save_and_update_snapshot() {
    // given
    var aggregateId = UUID.randomUUID();
    var state = new CheckpointState().setAggregateId(aggregateId)
                                     .setName("Another test")
                                     .setExpireAt(LocalDateTime.now().plus(1, ChronoUnit.DAYS))
                                     .setOccurredAt(LocalDateTime.now())
                                     .setSequenceNumber(BigInteger.ONE);
    var savedSnapshot = snapshotStore.update(state);
    // and
    var anotherState = state.setDeliveredAt(LocalDateTime.now())
                            .setOccurredAt(LocalDateTime.now())
                            .setSequenceNumber(BigInteger.valueOf(2L));
    var savedAnotherSnapshot = snapshotStore.update(anotherState);
    // and
    var yetAnotherState = anotherState.setLastDoorId("IN-1")
                                      .setLastDoorEnteredAt(LocalDateTime.now())
                                      .setOccurredAt(LocalDateTime.now())
                                      .setSequenceNumber(BigInteger.valueOf(3L));
    var savedYetAnotherSnapshot = snapshotStore.update(yetAnotherState);
    // and
    // when
    var snapshot = CheckpointStateSnapshot.from(yetAnotherState);
    snapshotStore.save(snapshot);
    // then
    var maybeSnapshot = snapshotStore.findFirstByAggregateId(aggregateId);
    assertThat(maybeSnapshot).isNotNull()
                             .isNotEmpty();
    var foundSnapshot = maybeSnapshot.get();
    assertThat(foundSnapshot.getId()).isNotNull();
    assertThat(foundSnapshot.getId()).isNotNull();
    assertThat(foundSnapshot.getAggregateId()).isNotNull()
                                              .isEqualTo(aggregateId);
    assertThat(foundSnapshot.getName()).isNotNull()
                                       .isEqualTo("Another test");
    assertThat(foundSnapshot.getLastDoorId()).isNotNull()
                                             .isEqualTo("IN-1");
  }

  @Test
  public void should_find_all_snapshots() {
    // given
    var aggregateId = UUID.randomUUID();
    var state = new CheckpointState().setAggregateId(aggregateId)
                                     .setName("Final test")
                                     .setExpireAt(LocalDateTime.now().plus(1, ChronoUnit.DAYS))
                                     .setOccurredAt(LocalDateTime.now())
                                     .setSequenceNumber(BigInteger.ONE);
    var savedSnapshot = snapshotStore.update(state);
    // and
    var anotherState = state.setDeliveredAt(LocalDateTime.now())
                            .setOccurredAt(LocalDateTime.now())
                            .setSequenceNumber(BigInteger.valueOf(2L));
    var savedAnotherSnapshot = snapshotStore.update(anotherState);
    // and
    var yetAnotherState = anotherState.setLastDoorId("IN-1")
                                      .setLastDoorEnteredAt(LocalDateTime.now())
                                      .setOccurredAt(LocalDateTime.now())
                                      .setSequenceNumber(BigInteger.valueOf(3L));
    var savedYetAnotherSnapshot = snapshotStore.update(yetAnotherState);
    // and
    // when
    var all = snapshotStore.findAll();
    // then
    var maybeSnapshot = snapshotStore.findFirstByAggregateId(aggregateId);
    assertThat(maybeSnapshot).isNotNull()
                             .isNotEmpty();
    var foundSnapshot = maybeSnapshot.get();
    assertThat(foundSnapshot.getId()).isNotNull();
    assertThat(foundSnapshot.getAggregateId()).isNotNull()
                                              .isEqualTo(aggregateId);
    assertThat(foundSnapshot.getName()).isNotNull()
                                       .isEqualTo("Final test");
    assertThat(foundSnapshot.getLastDoorId()).isNotNull()
                                             .isEqualTo("IN-1");
  }
}
