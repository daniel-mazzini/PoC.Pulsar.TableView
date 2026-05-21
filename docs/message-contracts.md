# Message Contracts

The PoC uses three message contracts only:

- `SportMessage`
- `CategoryMessage`
- `TaxonomyMessage`

All contract names must end with `Message`.

## Shared Rules

- `EntityId` is `payload.Id`
- `MessageKey` is `EntityId.ToString()`
- tombstones use a valid `MessageKey` and a `null` payload
- schemas are Avro-based
- contracts should stay aligned with compacted topic semantics

## SportMessage

Represents a Sport record from the `sports` topic.

This is an input contract for the Sports Table View.

## CategoryMessage

Represents a Category record from the `categories` topic.

It must contain a reference to its parent Sport.

This lets the taxonomy projector join Categories to Sports without inventing additional business entities.

## TaxonomyMessage

Represents the derived projection published to the `taxonomy-view` topic.

This is the output contract produced by the taxonomy projector processor.

## Tombstones

Because the topics are compacted, deletions are represented as tombstones.

A tombstone keeps the key and removes the payload.

The Table View must translate that into a delete in the local state store.
