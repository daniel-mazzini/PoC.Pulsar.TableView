# Bootstrap and Checkpoints

This PoC uses two startup ideas together:

- bootstrap the local view from compacted history
- keep a checkpoint of the last successfully applied Pulsar message

## Bootstrap Order

Startup should happen in this order:

1. capture the reverse high-watermark for each Table View
2. decide whether the local state is trustworthy
3. restore state or rebuild from `Earliest`
4. replay historical compacted messages
5. switch to live tail once the captured high-watermark is reached

## Reverse High-Watermark

The reverse high-watermark is the latest Pulsar `MessageId` captured at startup.

It is used to define the end of the bootstrap window.

The Table View is not live until it has applied every message up to that captured position.

## Checkpoint Meaning

A checkpoint is the last `PulsarMessageId` that was successfully applied to the local view.

That means the change has already been written into the Table View state store and is safe to resume from later.

A checkpoint must never be advanced just because a message was read from Pulsar.

## What Happens on Restart

For the in-memory version, the local state is lost on restart.

When that happens, any checkpoint value is useless by itself because there is no state to resume from.

The correct behavior is to ignore the checkpoint and rebuild from `Earliest`.

## Future Persistent Stores

If a future persistent state store is added, the same rule still applies:

- resume only when the local state is present and trustworthy
- otherwise rebuild from `Earliest`

This keeps the local materialized view and the checkpoint in sync.
