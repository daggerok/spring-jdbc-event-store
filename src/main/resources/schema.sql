drop table if exists domain_events CASCADE
;

drop table if exists snapshots CASCADE
;

create table domain_events (
    aggregate_id varchar(36) not null,
    occurred_at timestamp,
    sequence_number BIGINT IDENTITY PRIMARY KEY,
    event_type varchar(255) not null,
    name varchar(255),
    expire_at timestamp,
    door_id varchar(255)
)
;

create table snapshots (
    id BIGINT IDENTITY PRIMARY KEY,
    aggregate_id varchar(36) not null,
    occurred_at timestamp,
    sequence_number BIGINT,
    name varchar(255),
    expire_at timestamp,
    delivered_at timestamp,
    last_door_id varchar(255),
    last_door_entered_at timestamp
)
;

-- CREATE TABLE IF NOT EXISTS category (id INTEGER IDENTITY PRIMARY KEY, name VARCHAR(100), description VARCHAR(2000), age_group VARCHAR(20), created DATETIME, inserted BIGINT);
-- CREATE TABLE IF NOT EXISTS Lego_Set (id INTEGER, name VARCHAR(100), min_Age INTEGER, max_Age INTEGER);
-- CREATE TABLE IF NOT EXISTS Handbuch (handbuch_id INTEGER, author VARCHAR(100), text CLOB);
-- CREATE TABLE IF NOT EXISTS Model (name VARCHAR(100), description CLOB, lego_set INTEGER);
