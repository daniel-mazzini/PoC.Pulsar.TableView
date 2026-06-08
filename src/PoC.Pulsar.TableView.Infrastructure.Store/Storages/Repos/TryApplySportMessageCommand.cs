namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;

internal readonly record struct TryApplySportMessageCommand(SpanByte SerializedValue, int IncomingVersion);
