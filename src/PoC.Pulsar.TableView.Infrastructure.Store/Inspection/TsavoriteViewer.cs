using System.Text;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Metadatas;
using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Inspection;

public sealed class TsavoriteViewer
{
    public const int DefaultLimit = 100;
    public const int MaxLimit = 500;

    private static readonly IReadOnlyDictionary<string, TsavoriteViewerType> Types =
        new Dictionary<string, TsavoriteViewerType>(StringComparer.OrdinalIgnoreCase)
        {
            ["sports"] = new("sports",
                             StorageKey.SportMessagePrefix,
                             key => StorageKey.SportMessage(key).Value,
                             typeof(SportMessage)),
            ["categories"] = new("categories",
                                 StorageKey.CategoryMessagePrefix.Value,
                                 key => StorageKey.CategoryMessage(key).Value,
                                 typeof(RawCategoryMessage)),
            ["category-by-sport"] = new("category-by-sport",
                                        StorageKey.CategoryBySportIndexPrefix,
                                        key => key,
                                        typeof(string)),
            ["category-by-parent"] = new("category-by-parent",
                                         StorageKey.CategoryByParentIndexPrefix,
                                         key => key,
                                         typeof(string)),
            ["missing-category-by-sport"] = new("missing-category-by-sport",
                                                StorageKey.OrphanCategoryBySportIndexPrefix,
                                                key => key,
                                                typeof(string)),
            ["missing-sport-by-category"] = new("missing-sport-by-category",
                                                StorageKey.OrphanSportByCategoryIndexPrefix,
                                                key => key,
                                                typeof(string)),
            ["rejected"] = new("rejected",
                               StorageKey.RejectedRecordPrefix,
                               key => StorageKey.RejectedRecord(key).Value,
                               typeof(RejectedProjection)),
            ["store-metadata"] = new("store-metadata",
                                     StorageKey.StoreMetadata,
                                     _ => StorageKey.StoreMetadata,
                                     typeof(StoreMetadata))
        };

    private readonly ITsavoriteEngine _engine;
    private readonly IStateSerializer _serializer;

    public TsavoriteViewer(ITsavoriteEngine engine, IStateSerializer serializer)
    {
        _engine = engine ?? throw new ArgumentNullException(nameof(engine));
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    }

    public IReadOnlyList<TsavoriteViewerEntry> List(string type, int limit)
    {
        var viewerType = ResolveType(type);
        var result = new List<TsavoriteViewerEntry>();
        var prefixBytes = Encoding.UTF8.GetBytes(viewerType.Prefix);
        var boundedLimit = BoundLimit(limit);

        _engine.ScanByPrefix(prefixBytes, (keySpan, valueSpan) =>
        {
            if (result.Count >= boundedLimit)
            {
                return;
            }

            var storageKey = Encoding.UTF8.GetString(keySpan);
            var value = Deserialize(viewerType, valueSpan);
            if (value is not null)
            {
                result.Add(new TsavoriteViewerEntry(storageKey,
                                                    ExtractLogicalKey(viewerType, storageKey),
                                                    viewerType.Name,
                                                    value));
            }
        });

        return result;
    }

    public TsavoriteViewerEntry? Get(string type, string logicalKey)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(logicalKey);

        var viewerType = ResolveType(type);
        var expectedStorageKey = viewerType.BuildStorageKey(logicalKey);
        var expectedStorageKeyBytes = Encoding.UTF8.GetBytes(expectedStorageKey);
        TsavoriteViewerEntry? result = null;

        _engine.ScanByPrefix(expectedStorageKeyBytes, (keySpan, valueSpan) =>
        {
            if (result is not null)
            {
                return;
            }

            var storageKey = Encoding.UTF8.GetString(keySpan);
            if (!StringComparer.Ordinal.Equals(storageKey, expectedStorageKey))
            {
                return;
            }

            var value = Deserialize(viewerType, valueSpan);
            if (value is not null)
            {
                result = new TsavoriteViewerEntry(storageKey,
                                                  ExtractLogicalKey(viewerType, storageKey),
                                                  viewerType.Name,
                                                  value);
            }
        });

        return result;
    }

    public static IReadOnlyList<string> SupportedTypes() => Types.Keys.OrderBy(type => type, StringComparer.OrdinalIgnoreCase).ToArray();

    private static TsavoriteViewerType ResolveType(string type)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(type);
        return Types.TryGetValue(type, out var viewerType)
            ? viewerType
            : throw new NotSupportedException($"Unsupported Tsavorite viewer type '{type}'. Supported types: {string.Join(", ", SupportedTypes())}.");
    }

    private static int BoundLimit(int limit) => Math.Clamp(limit <= 0 ? DefaultLimit : limit, 1, MaxLimit);

    private object? Deserialize(TsavoriteViewerType type, ReadOnlySpan<byte> valueSpan)
        => type.ValueType == typeof(SportMessage) ? _serializer.Deserialize<SportMessage>(valueSpan) :
           type.ValueType == typeof(RawCategoryMessage) ? _serializer.Deserialize<RawCategoryMessage>(valueSpan) :
           type.ValueType == typeof(string) ? _serializer.Deserialize<string>(valueSpan) :
           type.ValueType == typeof(RejectedProjection) ? _serializer.Deserialize<RejectedProjection>(valueSpan) :
           type.ValueType == typeof(StoreMetadata) ? _serializer.Deserialize<StoreMetadata>(valueSpan) :
           throw new NotSupportedException($"Unsupported Tsavorite viewer CLR type '{type.ValueType.FullName}'.");

    private static string ExtractLogicalKey(TsavoriteViewerType type, string storageKey)
        => storageKey.StartsWith(type.Prefix, StringComparison.Ordinal)
            ? storageKey[type.Prefix.Length..]
            : storageKey;

    private sealed record TsavoriteViewerType(string Name, string Prefix, Func<string, string> BuildStorageKey, Type ValueType);
}
