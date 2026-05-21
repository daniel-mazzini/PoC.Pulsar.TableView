# Topics and Schemas

This PoC uses exactly three Pulsar topics.

## Topic List

| Topic | Direction | Compacted | Contract |
| --- | --- | --- | --- |
| `persistent://public/tableview-inputs/sports` | Input | Yes | `SportMessage` |
| `persistent://public/tableview-inputs/categories` | Input | Yes | `CategoryMessage` |
| `persistent://public/tableview-outputs/taxonomy-view` | Output | Yes | `TaxonomyMessage` |

## Topic Rules

- topic names are strictly plural where they represent business collections
- `persistent://public/tableview-outputs/taxonomy-view` is the only output topic in this PoC
- all topics participate in compacted-state semantics
- readers must treat the latest value for a key as the source of truth

## Schema Rules

- use Avro schemas for all message contracts
- keep the schema contract aligned with the message name
- preserve compatibility with compacted-topic updates and tombstones
- do not add extra business entities to the schema model

## Reader Expectations

The Pulsar Reader is responsible for deserializing the Avro payload and passing typed messages into the Table View.

The Table View then applies the message to its `IStateStore<TKey, TValue>` implementation and emits the corresponding reactive change.

## Projection Flow

The Sports and Categories Table Views feed the taxonomy projector.

The projector produces `TaxonomyMessage` records and publishes them to `persistent://public/tableview-outputs/taxonomy-view`.
