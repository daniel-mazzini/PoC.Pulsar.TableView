namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos.Functions;

internal readonly record struct TryApplyRawCategoryMessageCommand(SpanByte SerializedValue, int IncomingVersion);
