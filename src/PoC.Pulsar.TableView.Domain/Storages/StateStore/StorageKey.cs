using System.Text;
using PoC.Pulsar.TableView.Domain.Entities.Categories;
using PoC.Pulsar.TableView.Domain.Entities.Sports;

namespace PoC.Pulsar.TableView.Domain.Storages.StateStore;

public readonly record struct StorageKey(string Value)
{
    public static StorageKey Create(string value)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(value);
        return new StorageKey(value);
    }

    public static implicit operator StorageKey(string value) => StorageKey.Create(value);

    public int GetUtf8ByteCount() => Encoding.UTF8.GetByteCount(Value);
    public int WriteUtf8Bytes(Span<byte> destination) => Encoding.UTF8.GetBytes(Value, destination);

    public override string ToString() => Value;

    #region keyFactory

    public const string StoreMetadata = "__geo-projector:store-metadata";
    public const string StoreDiagnostics = "__geo-projector:store-diagnostics";

    public static StorageKey TopicCheckpoint(string topicName, int partitionId)
        => $"__geo-projector:topic-checkpoint:{topicName}:{partitionId}";

    // public static StorageKey DurableReceipt(string topicName, int partitionId)
    //     => $"__geo-projector:durable-receipt:{topicName}:{partitionId}";

    public static StorageKey SportEntity(SportId sportId)
        => $"__geo-projector:entity:sport:{sportId.Value}";
    public static StorageKey CategoryEntity(CategoryId categoryId)
       => $"__geo-projector:entity:category:{categoryId.Value}";
    public static StorageKey SportMessage(string sportId)
        => $"__geo-projector:raw:sport:{sportId}";

    public static StorageKey CategoryMessage(string categoryId)
        => $"__geo-projector:raw:category:{categoryId}";

    public static StorageKey CountryTaxonomyMaterializedView(SportId sportId)
        => $"__geo-projector:mv:country-taxonomy:{sportId.Value}";
    public static StorageKey CategoryIdsBySport(SportId sportId)
        => $"__geo-projector:idx:category-ids:by-sport:{sportId.Value}";
    public static StorageKey OrphanCategoryIdsBySport(SportId sportId)
        => $"__geo-projector:missing:category-ids:by-sport:{sportId.Value}";

    public static StorageKey PendingCountByPropertyType(SportId sportId)
        => $"__geo-projector:idx:missing-count-category:sport:{sportId.Value}";

    public static StorageKey RejectedRecord(string recordId)
        => $"__geo-projector:rejected:{recordId}";

    #endregion

}
