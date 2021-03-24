package com.github.daggerok.jdbceventstore;

import lombok.*;
import lombok.experimental.Accessors;
import lombok.extern.log4j.Log4j2;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.namedparam.BeanPropertySqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.EnableTransactionManagement;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.math.BigInteger;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.util.function.Predicate.not;

/**
 * Domain events
 */

interface DomainEvent<ID> {
  ID getAggregateId();
}

interface Identity<T> {
  T getSequenceNumber();
}

interface HistoricallyTrackable {
  LocalDateTime getOccurredAt();
}

interface Typed {
  String getEventType();
}

interface ApplicationDomainEvent extends DomainEvent<UUID>, HistoricallyTrackable, Identity<BigInteger>, Typed {}

@Data
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
class BaseDomainEvent implements ApplicationDomainEvent {

  /* Common: */
  protected UUID aggregateId;
  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
  protected LocalDateTime occurredAt;
  protected BigInteger sequenceNumber;
  protected String eventType;

  /* VisitorRegisteredEvent: */
  protected String name;
  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
  protected LocalDateTime expireAt;

  /* PassCardDeliveredEvent: empty */

  /* EnteredTheDoorEvent: */
  protected String doorId;
}

@With
@Value
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class VisitorRegisteredEvent extends BaseDomainEvent {

  private VisitorRegisteredEvent(UUID aggregateId, String name, LocalDateTime expireAt) {
    super(aggregateId, /* occurred at: */ LocalDateTime.now(), /* sequence number: */ null,
        /* event type: */ VisitorRegisteredEvent.class.getSimpleName(),
          name, expireAt, /* door ID: */ null);
  }

  public static VisitorRegisteredEvent of(UUID aggregateId, String name, LocalDateTime expireAt) {
    return new VisitorRegisteredEvent(aggregateId, name, expireAt);
  }
}

@With
@Value
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class PassCardDeliveredEvent extends BaseDomainEvent {

  private PassCardDeliveredEvent(UUID aggregateId) {
    super(aggregateId, /* occurred at: */ LocalDateTime.now(), /* sequence number: */ null,
        /* event type: */ PassCardDeliveredEvent.class.getSimpleName(),
        /* name: */ null, /* expire at: */ null, /* door ID: */ null);
  }

  public static PassCardDeliveredEvent of(UUID aggregateId) {
    return new PassCardDeliveredEvent(aggregateId);
  }
}

@With
@Value
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
class EnteredTheDoorEvent extends BaseDomainEvent {

  private EnteredTheDoorEvent(UUID aggregateId, String doorId) {
    super(aggregateId, /* occurred at: */ LocalDateTime.now(), /* sequence number: */ null,
        /* event type: */ EnteredTheDoorEvent.class.getSimpleName(),
        /* name: */ null, /* expire at: */ null, doorId);
  }

  public static EnteredTheDoorEvent of(UUID aggregateId, String doorId) {
    return new EnteredTheDoorEvent(aggregateId, doorId);
  }
}

/**
 * Infrastructure
 */

@NoArgsConstructor(access = AccessLevel.PRIVATE)
class Infrastructure {

  public static final Function<String, Supplier<RuntimeException>> error =
      message -> () -> new RuntimeException(message);
}

@Log4j2
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class DomainEvents {

  public static final Predicate<BaseDomainEvent> supported =
      evt -> evt instanceof VisitorRegisteredEvent
          || evt instanceof PassCardDeliveredEvent
          || evt instanceof EnteredTheDoorEvent;
}

/**
 * Event store
 */

interface EventStore<ID> {

  void appendEvents(Iterable<BaseDomainEvent> domainEvents);

  default void appendEvents(BaseDomainEvent... domainEvents) {
    appendEvents(Arrays.asList(domainEvents));
  }

  Collection<BaseDomainEvent> streamEvents(ID identity);
}

interface SnapshotSupport<ID> {
  Stream<BaseDomainEvent> streamEventsForSnapshot(ID snapshotIdentity, BigInteger snapshotSequenceNumber);
}

@Log4j2
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
class JdbcEventStore implements EventStore<UUID>, SnapshotSupport<UUID> {

  final NamedParameterJdbcTemplate namedJdbcTemplate;

  @Override
  @Transactional
  public void appendEvents(Iterable<BaseDomainEvent> domainEvents) {
    Optional.ofNullable(domainEvents)
            .orElseThrow(Infrastructure.error.apply("Domain events may not be null"))
            .forEach(this::appendEvent);
    log.info("added: {}", () -> StreamSupport.stream(domainEvents.spliterator(), false)
                                             .map(Object::getClass)
                                             .map(Class::getSimpleName)
                                             .collect(Collectors.joining(", ")));
  }

  private EventStore<UUID> appendEvent(BaseDomainEvent domainEvent) {
    var event = Optional.ofNullable(domainEvent)
                        .orElseThrow(Infrastructure.error.apply("Domain event may not be null"));
    return Optional.of(event)
                   .filter(DomainEvents.supported)
                   .map(this::onEvent)
                   .orElseGet(onNotSupported(event));
  }

  private Supplier<EventStore<UUID>> onNotSupported(BaseDomainEvent event) {
    return () -> {
      log.warn("Unsupported event {} hasn't been added into event store", event);
      return this;
    };
  }

  private EventStore<UUID> onEvent(BaseDomainEvent event) {
    var anEvent = Optional.ofNullable(event)
                          .orElseThrow(Infrastructure.error.apply("Event may not be null"));
    var paramSource = new BeanPropertySqlParameterSource(anEvent);
    // var hasBeenSaved = namedJdbcTemplate.execute(
    //     " INSERT INTO domain_events (aggregate_id, occurred_at, event_type,  name, expire_at, door_id)   " +
    //         "                    VALUES (:aggregateId, :occurredAt, :eventType, :name, :expireAt, :doorId) ; ",
    //     paramSource,
    //     PreparedStatement::executeUpdate
    // );
    var keyHolder = new GeneratedKeyHolder();
    var hasBeenSaved = namedJdbcTemplate.update(
        " INSERT INTO domain_events (aggregate_id, occurred_at, event_type,  name, expire_at, door_id)   " +
            "                    VALUES (:aggregateId, :occurredAt, :eventType, :name, :expireAt, :doorId) ; ",
        paramSource,
        keyHolder
    );
    var generatedKey = BigInteger.valueOf(Optional.ofNullable(keyHolder.getKeyAs(Long.class))
                                                  .orElse(-1L));
    event.setSequenceNumber(generatedKey);
    log.info("{} item has been added sequence_number({}): {}", hasBeenSaved, generatedKey, anEvent);
    return this;
  }

  // @Override
  // public Collection<BaseDomainEvent> streamEvents(UUID identity) {
  //   var anIdentity = Optional.ofNullable(identity)
  //                            .orElseThrow(error.apply("Identity may not be null"));
  //   var paramSource = Map.of("aggregateId", anIdentity);
  //   var domainEventsRowMapper = BeanPropertyRowMapper.newInstance(BaseDomainEvent.class);
  //   var sql = " SELECT * FROM domain_events        " +
  //     "          WHERE aggregate_id = :aggregateId " +
  //     "       ORDER BY sequence_number ASC ;       ";
  //   return namedJdbcTemplate.queryForStream(sql, paramSource, domainEventsRowMapper)
  //                           .collect(Collectors.toUnmodifiableList());
  // }

  @Override
  public Collection<BaseDomainEvent> streamEvents(UUID identity) {
    return streamEventsForSnapshot(identity, BigInteger.ZERO).collect(Collectors.toUnmodifiableList());
  }

  @Override
  public Stream<BaseDomainEvent> streamEventsForSnapshot(UUID snapshotIdentity, BigInteger snapshotSequenceNumber) {
    var anIdentity = Optional.ofNullable(snapshotIdentity)
                             .orElseThrow(Infrastructure.error.apply("Identity may not be null"));
    var aSequenceNumber = Optional.ofNullable(snapshotSequenceNumber)
                                  .orElse(BigInteger.ZERO);
    var paramSource = Map.of("aggregateId", anIdentity,
                             "sequenceNumber", aSequenceNumber);
    var domainEventsRowMapper = BeanPropertyRowMapper.newInstance(BaseDomainEvent.class);
    var sql = " SELECT aggregate_id,                     " +
        "              occurred_at,                      " +
        "              sequence_number,                  " +
        "              event_type,                       " +
        "              name,                             " +
        "              expire_at,                        " +
        "              door_id                           " +
        "         FROM domain_events                     " +
        "        WHERE aggregate_id = :aggregateId       " +
        "          AND sequence_number > :sequenceNumber " +
        "     ORDER BY sequence_number ASC ;             ";
    return namedJdbcTemplate.queryForStream(sql, paramSource, domainEventsRowMapper);
  }
}

/**
 * Aggregate state
 */

interface State<ID> extends Identity<ID> {}

interface MutableState<ID> {
  MutableState<ID> mutate(BaseDomainEvent domainEvent);
}

@Data
@Log4j2
@NoArgsConstructor
@Accessors(chain = true)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
class CheckpointState implements State<BigInteger>, MutableState<UUID>, HistoricallyTrackable {

  private UUID aggregateId;
  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
  private LocalDateTime occurredAt;
  private BigInteger sequenceNumber;

  private String name;
  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
  private LocalDateTime expireAt;

  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
  private LocalDateTime deliveredAt;

  private String lastDoorId;
  @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
  private LocalDateTime lastDoorEnteredAt;

  @Override
  public CheckpointState mutate(BaseDomainEvent domainEvent) {
    var anEvent = Optional.ofNullable(domainEvent)
                          .orElseThrow(Infrastructure.error.apply("Domain event may not be null"));
    if (anEvent.getEventType().equals(VisitorRegisteredEvent.class.getSimpleName()))
      return onVisitorRegisteredEvent(anEvent);
    if (anEvent.getEventType().equals(PassCardDeliveredEvent.class.getSimpleName()))
      return onPassCardDeliveredEvent(anEvent);
    if (anEvent.getEventType().equals(EnteredTheDoorEvent.class.getSimpleName())) return onEnteredTheDoorEvent(anEvent);
    return onUnsupportedDomainEvent(anEvent);
  }

  private CheckpointState onVisitorRegisteredEvent(BaseDomainEvent event) {
    return this.setAggregateId(event.getAggregateId())
               .setName(event.getName())
               .setExpireAt(event.getExpireAt())
               .setOccurredAt(event.getOccurredAt())
               .setSequenceNumber(event.getSequenceNumber());
  }

  private CheckpointState onPassCardDeliveredEvent(BaseDomainEvent event) {
    return this.setDeliveredAt(event.getOccurredAt())
               .setOccurredAt(event.getOccurredAt())
               .setSequenceNumber(event.getSequenceNumber());
  }

  private CheckpointState onEnteredTheDoorEvent(BaseDomainEvent event) {
    return this.setLastDoorId(event.getDoorId())
               .setLastDoorEnteredAt(event.getOccurredAt())
               .setOccurredAt(event.getOccurredAt())
               .setSequenceNumber(event.getSequenceNumber());
  }

  private CheckpointState onUnsupportedDomainEvent(BaseDomainEvent event) {
    log.warn("Fallback: {}", event);
    return this;
  }
}

/**
 * Snapshot
 */

@Data
@ToString(callSuper = true)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
@AllArgsConstructor(access = AccessLevel.PRIVATE)
class CheckpointStateSnapshot extends CheckpointState {

  private BigInteger id;

  public CheckpointStateSnapshot(BigInteger id, UUID aggregateId, LocalDateTime occurredAt, BigInteger sequenceNumber,
                                 String name, LocalDateTime expireAt, LocalDateTime deliveredAt,
                                 String lastDoorId, LocalDateTime lastDoorEnteredAt) {
    super(aggregateId, occurredAt, sequenceNumber, name, expireAt, deliveredAt, lastDoorId, lastDoorEnteredAt);
    this.id = id;
  }

  public CheckpointStateSnapshot patchWith(CheckpointState state) {

    Optional.ofNullable(state.getAggregateId())
            .filter(that -> !that.equals(this.getAggregateId()))
            .ifPresent(this::setAggregateId);

    Optional.ofNullable(state.getOccurredAt())
            .filter(that -> !that.equals(this.getOccurredAt()))
            .ifPresent(this::setOccurredAt);

    Optional.ofNullable(state.getSequenceNumber())
            .filter(that -> !that.equals(this.getSequenceNumber()))
            .ifPresent(this::setSequenceNumber);

    Optional.ofNullable(state.getName())
            .filter(that -> !that.equals(this.getName()))
            .ifPresent(this::setName);

    Optional.ofNullable(state.getExpireAt())
            .filter(that -> !that.equals(this.getExpireAt()))
            .ifPresent(this::setExpireAt);

    Optional.ofNullable(state.getDeliveredAt())
            .filter(that -> !that.equals(this.getDeliveredAt()))
            .ifPresent(this::setDeliveredAt);

    Optional.ofNullable(state.getLastDoorId())
            .filter(that -> !that.equals(this.getLastDoorId()))
            .ifPresent(this::setLastDoorId);

    Optional.ofNullable(state.getLastDoorEnteredAt())
            .filter(that -> !that.equals(this.getLastDoorEnteredAt()))
            .ifPresent(this::setLastDoorEnteredAt);

    return this;
  }

  public static CheckpointStateSnapshot noop(UUID aggregateId) {
    return new CheckpointStateSnapshot(null, aggregateId, null, null, null, null, null, null, null);
  }

  /* NOTE: this method is package-private only for testing */
  static CheckpointStateSnapshot from(CheckpointState state) {
    return new CheckpointStateSnapshot(null, state.getAggregateId(), state.getOccurredAt(), state.getSequenceNumber(),
                                       state.getName(), state.getExpireAt(), state.getDeliveredAt(),
                                       state.getLastDoorId(), state.getLastDoorEnteredAt());
  }
}

@Log4j2
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
class JdbcSnapshotStore {

  private final NamedParameterJdbcTemplate namedJdbcTemplate;

  @Transactional
  public Optional<CheckpointStateSnapshot> save(CheckpointStateSnapshot snapshot) {
    var sql = " MERGE INTO snapshots ( id, aggregate_id, occurred_at, sequence_number,  name, expire_at, delivered_at, last_door_id, last_door_entered_at) " +
        "            KEY (id) VALUES (:id, :aggregateId, :occurredAt, :sequenceNumber, :name, :expireAt, :deliveredAt, :lastDoorId, :lastDoorEnteredAt) ;  ";
    var paramSource = new BeanPropertySqlParameterSource(snapshot);
    var keyHolder = new GeneratedKeyHolder();
    var inserted = namedJdbcTemplate.update(sql, paramSource, keyHolder);
    var generated = BigInteger.valueOf(Optional.ofNullable(keyHolder.getKeyAs(Long.class))
                                               .orElse(-1L));
    snapshot.setId(generated);
    log.info("{} row processed: {}", inserted, snapshot);
    return Optional.of(snapshot);
  }

  public Optional<CheckpointStateSnapshot> findFirstByAggregateId(UUID aggregateId) {
    var sql = " SELECT id, aggregate_id, occurred_at, sequence_number, " +
        "              name, expire_at, delivered_at,                  " +
        "              last_door_id, last_door_entered_at              " +
        "         FROM snapshots                                       " +
        "        WHERE aggregate_id = :aggregateId                     " +
        "     ORDER BY occurred_at ASC ;                               ";
    var paramMap = Map.of("aggregateId", aggregateId);
    var rowMapper = BeanPropertyRowMapper.newInstance(CheckpointStateSnapshot.class);
    return namedJdbcTemplate.queryForStream(sql, paramMap, rowMapper)
                            .findFirst();
  }

  public Collection<CheckpointStateSnapshot> findAll() {
    var sql = " SELECT id, aggregate_id, occurred_at, sequence_number, " +
        "              name, expire_at, delivered_at,                  " +
        "              last_door_id, last_door_entered_at              " +
        "         FROM snapshots                                       " +
        "     ORDER BY occurred_at ASC ;                               ";
    var rowMapper = BeanPropertyRowMapper.newInstance(CheckpointStateSnapshot.class);
    return namedJdbcTemplate.queryForStream(sql, new BeanPropertySqlParameterSource(Map.of()), rowMapper)
                            .collect(Collectors.toUnmodifiableList());
  }

  @Transactional
  public Optional<CheckpointStateSnapshot> update(CheckpointState state) {
    var aState = Objects.requireNonNull(state, "State may not be null");
    var aggregateId = Objects.requireNonNull(aState.getAggregateId(), "Aggregate ID may not be null");
    var patched = findFirstByAggregateId(aggregateId).orElse(CheckpointStateSnapshot.noop(aggregateId))
                                                     .patchWith(state);
    return save(patched);
  }
}

/**
 * Aggregate
 */

@Getter
@Log4j2
@ToString
class CheckpointAggregate {

  private final Collection<BaseDomainEvent> eventStream;
  private final CheckpointState state;

  public CheckpointAggregate() {
    this(new CheckpointState(), new CopyOnWriteArrayList<>());
  }

  public CheckpointAggregate(CheckpointState state) {
    this(state, new CopyOnWriteArrayList<>());
  }

  private CheckpointAggregate(CheckpointState state, Collection<BaseDomainEvent> eventStream) {
    this.state = Optional.ofNullable(state).orElseGet(CheckpointState::new);
    this.eventStream = Optional.ofNullable(eventStream).orElseGet(CopyOnWriteArrayList::new);
  }

  // exec commands

  public CheckpointAggregate registerVisitor(UUID aggregateId, String name, LocalDateTime expireAt) {
    if (Objects.nonNull(state.getAggregateId()) && state.getAggregateId().equals(aggregateId))
      throw Infrastructure.error.apply(String.format("Visitor %s already registered", aggregateId))
                                .get();
    var anAggregateId = Optional.ofNullable(aggregateId)
                                .orElseGet(UUID::randomUUID);
    var nonNullName = Optional.ofNullable(name)
                              .orElseThrow(Infrastructure.error.apply("Name may not be null"));
    var aName = Optional.of(nonNullName)
                        .filter(not(String::isBlank))
                        .orElseThrow(Infrastructure.error.apply("Name may not be empty"));
    var nonNullExpireAt = Optional.ofNullable(expireAt)
                                  .orElseThrow(Infrastructure.error.apply("Expire at may not be null"));
    var anExpireAt = Optional.ofNullable(nonNullExpireAt)
                             .filter(localDateTime -> localDateTime.isAfter(LocalDateTime.now()))
                             .orElseThrow(Infrastructure.error.apply("Expire at must be in future"));
    return apply(VisitorRegisteredEvent.of(anAggregateId, aName, anExpireAt));
  }

  public CheckpointAggregate deliverPassCard(UUID aggregateId) {
    if (Objects.isNull(state.getAggregateId()))
      throw Infrastructure.error.apply(String.format("Visitor %s hasn't been registered", aggregateId))
                                .get();
    if (Objects.nonNull(state.getDeliveredAt()))
      throw Infrastructure.error.apply(String.format("Pass card already delivered to %s", aggregateId))
                                .get();
    var anAggregateId = Optional.ofNullable(aggregateId)
                                .orElseThrow(Infrastructure.error.apply("Aggregate ID may not be null"));
    return apply(PassCardDeliveredEvent.of(anAggregateId));
  }

  public CheckpointAggregate enterTheDoor(UUID aggregateId, String doorId) {
    if (Objects.isNull(state.getDeliveredAt()))
      throw Infrastructure.error.apply(String.format("Pass card hasn't been delivered to %s", aggregateId))
                                .get();
    if (Objects.nonNull(state.getLastDoorId()) && state.getLastDoorId().equals(doorId))
      throw Infrastructure.error.apply(String.format("Door %s was already entered by %s", doorId, aggregateId))
                                .get();
    var anAggregateId = Optional.ofNullable(aggregateId)
                                .orElseThrow(Infrastructure.error.apply("Aggregate ID may not be null"));
    var nonNullDoorId = Optional.ofNullable(doorId)
                                .orElseThrow(Infrastructure.error.apply("Door ID may not be null"));
    var aDoorId = Optional.of(nonNullDoorId)
                          .filter(not(String::isBlank))
                          .orElseThrow(Infrastructure.error.apply("Door ID may not be empty"));
    return apply(EnteredTheDoorEvent.of(anAggregateId, aDoorId));
  }

  // apply events

  private CheckpointAggregate apply(BaseDomainEvent event) {
    var anEvent = Optional.ofNullable(event)
                          .orElseThrow(Infrastructure.error.apply("Domain event may not be null"));
    this.state.mutate(anEvent);
    this.eventStream.add(anEvent);
    return this;
  }

  public static CheckpointAggregate rebuild(CheckpointAggregate snapshot, Iterable<BaseDomainEvent> events) {
    var aSnapshot = Optional.ofNullable(snapshot)
                            .orElseGet(CheckpointAggregate::new);
    var domainEvents = Optional.ofNullable(events)
                               .orElseGet(CopyOnWriteArrayList::new);
    domainEvents.forEach(aSnapshot::apply);
    return aSnapshot;
  }
}

/**
 * Aggregate repository
 */

interface Repository<A, ID> {

  void save(A aggregate);

  Optional<A> find(ID aggregateId);
}

@Log4j2
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
class CheckpointRepository implements Repository<CheckpointAggregate, UUID> {

  private final JdbcEventStore eventStore;
  private final JdbcSnapshotStore snapshotStore;

  @Override
  @Transactional
  public void save(CheckpointAggregate aggregate) {
    var anAggregate = Optional.ofNullable(aggregate)
                              .orElseThrow(Infrastructure.error.apply("Aggregate may not be null"));
    var noneNullDomainEvents = Optional.ofNullable(anAggregate.getEventStream())
                                       .orElseThrow(Infrastructure.error.apply("Event stream may not be null"));
    var domainEvents = Optional.of(noneNullDomainEvents)
                               .map(ds -> ds.stream()
                                            .filter(s -> Objects.isNull(s.getSequenceNumber()))
                                            .collect(Collectors.toUnmodifiableList()))
                               .filter(ds -> ds.stream()
                                               .anyMatch(event -> Objects.nonNull(event.getAggregateId())))
                               .orElseThrow(Infrastructure.error.apply("Aggregate ID may not be null"));

    eventStore.appendEvents(domainEvents);
    aggregate.getEventStream().clear();

    // perform snapshot each 10th aggregate state has been changed
    var performSnapshot = domainEvents.stream()
                                      .anyMatch(event -> Objects.nonNull(event.getSequenceNumber())
                                          && event.getSequenceNumber()
                                                  .mod(BigInteger.TEN)
                                                  .equals(BigInteger.ZERO));
    if (performSnapshot)
      Optional.ofNullable(aggregate.getState())
              .map(CheckpointState::getAggregateId)
              .flatMap(this::find)
              .map(CheckpointAggregate::getState)
              .ifPresent(snapshotStore::update);
  }

  @Override
  public Optional<CheckpointAggregate> find(UUID aggregateId) {
    var anAggregateId = Optional.ofNullable(aggregateId);
    if (anAggregateId.isEmpty()) return Optional.empty();

    var maybeStateSnapshot = snapshotStore.findFirstByAggregateId(aggregateId);

    var domainEvents = maybeStateSnapshot.map(CheckpointStateSnapshot::getSequenceNumber)
                                         .map(sequenceNumber -> eventStore.streamEventsForSnapshot(aggregateId, sequenceNumber))
                                         .orElseGet(() -> eventStore.streamEventsForSnapshot(aggregateId, BigInteger.ZERO))
                                         .collect(Collectors.toUnmodifiableList());
    // if (domainEvents.isEmpty()) return Optional.empty();

    var aggregateSnapshot = maybeStateSnapshot.map(CheckpointAggregate::new)
                                              .orElseGet(CheckpointAggregate::new);

    var rebuiltAggregate = CheckpointAggregate.rebuild(aggregateSnapshot, domainEvents);

    // // NOTE: it doesn't work, but could be, if you decide to implement snapshotting business logic in this way...
    // var rebuiltState = rebuiltAggregate.getState();
    // var updatedSnapshot = maybeStateSnapshot.map(VisitorState::getSequenceNumber)
    //                                         .filter(s -> s + 10 < rebuiltState.getSequenceNumber());
    // if (updatedSnapshot.isEmpty()) Publishers.waitOne(snapshotRepository.update(rebuiltState));

    var empty = Optional.ofNullable(rebuiltAggregate)
                        .map(CheckpointAggregate::getState)
                        .map(CheckpointState::getAggregateId)
                        .isEmpty();
    return empty ? Optional.empty() : Optional.of(rebuiltAggregate);
  }
}

/**
 * Not complete implementation, but enough for the demo...
 */
@Log4j2
@RestController
@RequiredArgsConstructor
@RequestMapping(path = "/api/v1")
class RestApi {

  private final CheckpointRepository repository;

  @ExceptionHandler(Throwable.class)
  ResponseEntity onError(Throwable throwable) {
    log.error("REST API Error: {}", throwable.getMessage());
    return ResponseEntity.badRequest()
                         .body(Map.of("error", throwable.getMessage()));
  }

  @PostMapping("/register-visitor")
  ResponseEntity registerVisitor(@RequestBody Map<String, String> request) {
    log.info("register request: {}", request);
    var aggregateId = Optional.ofNullable(request.get("aggregateId"))
                              .map(UUID::fromString)
                              .orElseGet(UUID::randomUUID);
    var alreadyRegistered = repository.find(aggregateId);
    if (alreadyRegistered.isPresent())
      throw Infrastructure.error.apply("Visitor already registered")
                                .get();
    var name = Optional.ofNullable(request.get("name"))
                       .filter(not(String::isBlank))
                       .orElseThrow(Infrastructure.error.apply("Name may not be null"));
    var expireIn = Optional.ofNullable(request.get("expireIn"))
                           .map(Duration::parse) // P2D => 2 days
                           .orElse(Duration.ofDays(365L));
    var expireAt = LocalDateTime.now().plus(expireIn.toDays(), ChronoUnit.DAYS);
    var aggregate = new CheckpointAggregate().registerVisitor(aggregateId, name, expireAt);
    repository.save(aggregate);
    return ResponseEntity.accepted().body(Map.of("result", aggregateId));
  }

  @PostMapping("/deliver-pass-card")
  ResponseEntity deliverPassCard(@RequestBody Map<String, String> request) {
    var aggregateId = Optional.ofNullable(request.get("aggregateId"))
                              .map(UUID::fromString)
                              .orElseThrow(Infrastructure.error.apply("Aggregate ID may not be null"));
    var aggregate = repository.find(aggregateId)
                              .orElseThrow(Infrastructure.error.apply("Aggregate not found"));
    if (Objects.nonNull(aggregate.getState().getDeliveredAt()))
       throw Infrastructure.error.apply("Pass card already delivered")
                                 .get();
    repository.save(aggregate.deliverPassCard(aggregateId));
    return ResponseEntity.accepted().body(Map.of("result", aggregateId));
  }

  @PostMapping("/enter-the-door")
  ResponseEntity enterTheDoor(@RequestBody Map<String, String> request) {
    var aggregateId = Optional.ofNullable(request.get("aggregateId"))
                              .map(UUID::fromString)
                              .orElseThrow(Infrastructure.error.apply("Aggregate ID may not be null"));
    var doorId = Optional.ofNullable(request.get("doorId"))
                         .filter(not(String::isBlank))
                         .orElseThrow(Infrastructure.error.apply("Door ID may not be null"));
    var aggregate = repository.find(aggregateId)
                              .orElseThrow(Infrastructure.error.apply("Aggregate not found"));
    var expireAt = aggregate.getState().getExpireAt();
    if (Objects.isNull(expireAt) || expireAt.isBefore(LocalDateTime.now()))
      throw Infrastructure.error.apply("Visitor not allowed to enter")
                                .get();
    repository.save(aggregate.enterTheDoor(aggregateId, doorId));
    return ResponseEntity.accepted().body(Map.of("result", aggregateId));
  }
}

/*
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
 */
@SpringBootApplication
@EnableTransactionManagement(proxyTargetClass = true)
public class DemoApplication {
  public static void main(String[] args) {
    SpringApplication.run(DemoApplication.class, args);
  }
}
