namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;

internal readonly record struct TryApplyRawCategoryMessageCommand(SpanByte SerializedValue, int IncomingVersion);
