namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos.Functions;

internal readonly record struct TryApplySportMessageCommand(SpanByte SerializedValue, int IncomingVersion);
