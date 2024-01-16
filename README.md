# POC-AggregateDistributedEvents
During a recent job interview, a hypothetical situation was presented. A transaction passing through various microservices (`events`) will have various associated logs (`Types`). A means of aggregating these events and persisting them was desired. An "enrichment" process was desired to supplement these events. The following are the assumptions provided:
- A common key `transaction` was present for all `Types` linked to a single event
- This `transaction` was guaranteed to be unique
- No duplicate `Types` for a transaction would be present
- *only* after all `Types` for a correlated event, should the event be persisted, with the aggregated logs from each `Type`
- after a suitable timeout, an `event` with any ommitted `Types` should still be persisted, even if only a partial subset of `Types` exist for the event
- The design should be scalable, and persist perpetually.
- Written in Python

Initially I went with a queue-based approach, using [Celery] (https://celeryq.dev) and a Redis or RabbitMQ backend. The ability to have a fan-out messages and chain events is something I've worked with before and it's worked well. However, when disucssing this I wondered if an event-based design, using [kafka] (https://kafka.apache.org)-something that pre-existed in the company, would be a viable alternative. A [faust-streaming] (https://faust-streaming.github.io/faust/) streaming processor was mentioned as being a possible solution. Having little kafka experience, and new to the faust world, I decided to throw up a small POC to access it's viability.

## Caveat Emptor!
This is a very rudimentary mock-up. I've explcitly set a number of **sub-optimal** configurations. One of these is testing retrieval of co-partitioned events across multiple workers. In production, the event would likely be keyed to a single partition. I've also completely ignored data enrichment (although I'm guessing this would be an out-of-bound process anyway) and the persistence (e.g. NoSQL ingestion). I focused on what was new to me. I'm also not using `RocksDB` as a storage provider, something that would absolutely be necessary for table recovery in production. Unit tests etc. are not present. Again, this is not production ready.

## RUNME
Pre-requisites: [docker] (https://docker.com)
```
docker compose up
```
* you should be able to log in to kafka-ui via http://localhost:8080
* an `event-generator` container will run. It will produce messages on 5 topics, `Events-TypeA, ...Events-TypeE`. 3 events will be published to these topics, 1 in topic order, 1 out-of-order and 1 with an omitted topic. Once this has run 3 times, the container will stop. To re-run `docker compose restart events-generator` will submit new events. This container has a number of `await asyncio.sleep()` to aid observing the topics.
* two workers `event-aggregator-1` & `event-aggregator-2` are present. Partitioned tables are created to hold state and status of the events.
* a number of topics are created, `Events_Aggregated` for completed events matched to all 5 types, and `Events_Expired` for partial events that have some types omitted. A separate worker would poll these events and persist.
