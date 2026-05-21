# Project Specification: Pulsar TableView PoC

## 1. Overview & Goals

This PoC explores a reusable Pulsar TableView pattern for .NET, similar to what exists in Java and C++ ecosystems.

The goal is to use Apache Pulsar Reader, compacted topics, Avro schemas, .NET Aspire, Dekaf, checkpoints, and local materialized views to build a reactive projection pipeline.

The design uses Rx.NET so the Table View can broadcast state changes as observables, and a projector can subscribe to those changes instead of polling state.

The initial scope is limited to three business concepts and three topics:

- Sports
- Categories
- Taxonomy

## 2. Architecture Components

### Pulsar Reader Component

The reader is responsible for consuming compacted Pulsar topics and deserializing Avro payloads into message contracts.

Its job is to:

- read from the configured topic
- understand Pulsar message positions
- deserialize Avro into `SportMessage`, `CategoryMessage`, or `TaxonomyMessage`
- forward records to the Table View in read order
- preserve tombstones as delete events

### State Store Abstraction (`IStateStore<TKey, TValue>`)

The Table View must not depend on a specific storage engine. It must depend on an abstraction instead.

The `IStateStore<TKey, TValue>` interface should support the operations needed by the Table View lifecycle:

- `Get`
- `Upsert`
- `Delete`
- `Clear`

This abstraction lets the Table View manage a local materialized view without knowing whether the storage is in-memory or persistent.

### InMemoryStateStore

The first implementation is an in-memory state store.

It is used for the initial PoC because it is simple and makes the bootstrap and checkpoint behavior easy to observe.

This version is not durable, so a process restart means the local view is lost.

### Future Extensions

A future `TsavoriteStateStore` should fit behind the same `IStateStore<TKey, TValue>` interface.

That keeps the Table View and projector code stable while the storage engine changes underneath.

### TableView Wrapper

The Table View wraps the reader and the state store.

It is responsible for:

- applying incoming messages to the local store
- handling tombstones as deletes
- tracking checkpoints after successful local application
- exposing `IObservable<Change<T>>` streams for consumers
- telling subscribers when the local view changes

The Table View is the boundary between Pulsar IO and reactive projection logic.

### Taxonomy Projector Processor

The taxonomy projector processor subscribes to the Rx observables exposed by the Sports and Categories Table Views.

It joins the local views, computes the derived taxonomy, and publishes the resulting `TaxonomyMessage` to the `persistent://public/tableview-outputs/taxonomy-view` topic.

The processor must react to state changes, not re-read the world from scratch on every update.

## 3. Data Flow & Lifecycle

### Ingestion

The PoC consumes two input topics:

- `sports`
- `categories`

Each topic is compacted and must be read as a source of latest-known state.

### State Updates & Tombstone Handling

Each message is applied to the local Table View state store.

Rules:

- a normal payload means upsert
- a `null` payload with a valid key means tombstone delete
- the local view must reflect the latest compacted state, not just the latest network traffic

### Reactive Event Emission

After the state store is updated, the Table View emits a change event through Rx.NET.

That observable is what the projector consumes.

### Projection & Output Publishing

The taxonomy projector combines the Sports and Categories views and publishes the derived result to the `persistent://public/tableview-outputs/taxonomy-view` topic.

The output topic is also compacted, because it represents the latest projected view.

## 4. Bootstrap vs. Live Streaming

### Phase 1: Bootstrapping

Bootstrapping means reading historical compacted data and rebuilding the local materialized view.

During this phase, the Table View is catching up with the topic state that already exists in Pulsar.

### Reverse High-Watermark

At startup, the Table View captures the latest Pulsar `MessageId` before it begins replay.

This is the reverse high-watermark.

It marks the end of the historical backlog that must be replayed before the view can be considered live.

The Table View keeps applying messages until it reaches that captured high-watermark. Once the applied message position catches up, bootstrap is complete.

### Phase 2: Live Tail

After the reverse high-watermark is reached, the Table View transitions to live streaming.

From that point on, it processes new messages as they arrive and keeps the local view current.

The important order is:

1. capture the reverse high-watermark
2. rebuild or restore local state
3. replay historical compacted data
4. switch to live tail once caught up

## 5. State Management & Resilience

### Checkpointing Semantics

A checkpoint is the last `PulsarMessageId` that was successfully applied to the local view.

It is not the last message read from the network.

That distinction matters because a message can be read but still fail before the state store is updated.

Checkpoint advancement must only happen after the local store has accepted the change.

### Disaster Recovery

If checkpoints exist but the local view is missing, wiped, or otherwise untrusted, the checkpoint must be ignored.

In that case the Table View must rebuild from `Earliest` and replay the compacted topics from scratch.

This is required for the in-memory version after restart and must also apply to any future persistent version if the stored state is corrupted or unavailable.
