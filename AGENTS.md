# Repository Rules for Future AI Work

This repository is a .NET proof of concept for a Pulsar TableView-style projector.

Follow these rules in future coding tasks:

1. Only work with these business concepts: Sports, Categories, Taxonomy.
2. Do not introduce new business entities, topics, or projections unless the user explicitly asks for them.
3. Topic names must be plural and lowercase only.
4. Message contract names must end with `Message`.
5. Keep `sports`, `categories`, and `taxonomy-view` as the only Pulsar topics in scope for this PoC unless the user expands the spec.
6. Treat `EntityId` as `payload.Id`.
7. Treat `MessageKey` as `EntityId.ToString()`.
8. Treat `PulsarMessageId` as the physical or logical position in Pulsar, not as business data.
9. Handle tombstones by producing a message with a valid `MessageKey` and a `null` payload.
10. Treat checkpoints as the last `PulsarMessageId` successfully applied to the local view.
11. Do not treat a checkpoint as the last message read from the network.
12. If a checkpoint exists but the local state is missing or untrusted, ignore the checkpoint and rebuild the view from `Earliest`.
13. Keep the state store behind `IStateStore<TKey, TValue>`.
14. Start with `InMemoryStateStore` only when implementing the first production code.
15. Leave the interface extensible for a future `TsavoriteStateStore`.
16. The Table View must own bootstrap, replay, and live-tail sequencing.
17. The Table View must expose reactive change streams through Rx.NET observables.
18. The taxonomy projector must listen to the Table View observables and publish joined results to `taxonomy-view`.
19. Do not implement production code until the docs and architecture are clear.
20. Keep changes minimal and aligned to the specification.
