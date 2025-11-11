CREATE KEYSPACE IF NOT EXISTS taxi_tracking
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE taxi_tracking;

CREATE TABLE IF NOT EXISTS positions (
    taxi_id int,
    timestamp timestamp,
    latitude double,
    longitude double,
    altitude double,
    available boolean,
    PRIMARY KEY (taxi_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);

CREATE TABLE IF NOT EXISTS trips (
    trip_id uuid,  
    taxi_id int,
    start_time timestamp,
    end_time timestamp,
    trip_distance double,
    amount double,
    PRIMARY KEY (trip_id)
);
