using System.Text;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.Sports;

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

    private static string EncodeSegment(string value) => Uri.EscapeDataString(value);

    #region keyFactory

    public const string StoreMetadata = "__geo-projector:store-metadata";
    public const string StoreDiagnostics = "__geo-projector:store-diagnostics";
    public const string CategoryBySportIndexPrefix = "__geo-projector:idx:category:by-sport:";
    public const string CategoryByParentIndexPrefix = "__geo-projector:idx:category:by-parent:";
    public const string OrphanCategoryBySportIndexPrefix = "__geo-projector:missing:category:by-sport:";
    public const string OrphanSportByCategoryIndexPrefix = "__geo-projector:missing:sport:by-category:";

    public const string SportMessagePrefix = "__geo-projector:raw:sport:";
    public static StorageKey TopicCheckpoint(string physicalTopic)
    {
        string encodedTopicName = EncodeSegment(physicalTopic);
        return $"__geo-projector:topic-checkpoint:{encodedTopicName}";
    }

    public static StorageKey ViewCheckpoint(string viewName)
    {
        string encodedViewName = EncodeSegment(viewName);
        return $"__geo-projector:view-checkpoint:{encodedViewName}";
    }

    
    public static StorageKey CategoryBySportPrefix(SportId sportId)
    {
        string encodedSportId = EncodeSegment(sportId.Value);
        return $"{CategoryBySportIndexPrefix}{encodedSportId}:category:";
    }

    public static StorageKey CategoryBySport(SportId sportId, CategoryId categoryId)
    {
        string encodedCategoryId = EncodeSegment(categoryId.Value);
        return $"{CategoryBySportPrefix(sportId).Value}{encodedCategoryId}";
    }

    public static StorageKey CategoryByParentPrefix(CategoryId parentCategoryId)
    {
        string encodedParentCategoryId = EncodeSegment(parentCategoryId.Value);
        return $"{CategoryByParentIndexPrefix}{encodedParentCategoryId}:category:";
    }

    public static StorageKey CategoryByParent(CategoryId parentCategoryId, CategoryId categoryId)
    {
        string encodedCategoryId = EncodeSegment(categoryId.Value);
        return $"{CategoryByParentPrefix(parentCategoryId).Value}{encodedCategoryId}";
    }
    public static StorageKey OrphanCategoryBySportPrefix(SportId sportId)
    {
        string encodedSportId = EncodeSegment(sportId.Value);
        return $"{OrphanCategoryBySportIndexPrefix}{encodedSportId}:category:";
    }
    public static StorageKey OrphanCategoryBySport(SportId sportId, CategoryId categoryId)
    {
        string encodedCategoryId = EncodeSegment(categoryId.Value);
        return $"{OrphanCategoryBySportPrefix(sportId).Value}{encodedCategoryId}";
    }
    public static StorageKey SportMessage(string sportId)
    {
        string encodedSportId = EncodeSegment(sportId);
        return $"{SportMessagePrefix}{encodedSportId}";
    }

    public static StorageKey OrphanSportByCategoryPrefix(CategoryId categoryId)
    {
        string encodedCategoryId = EncodeSegment(categoryId.Value);
        return $"{OrphanSportByCategoryIndexPrefix}{encodedCategoryId}:sport:";
    }

    public static StorageKey OrphanSportByCategory(CategoryId categoryId, SportId sportId)
    {
        string encodedSportId = EncodeSegment(sportId.Value);
        return $"{OrphanSportByCategoryPrefix(categoryId).Value}{encodedSportId}";
    }


    public static StorageKey CategoryMessage(string categoryId)
    {
        string encodedCategoryId = EncodeSegment(categoryId);
        return $"__geo-projector:raw:category:{encodedCategoryId}";
    }

    public static StorageKey CategoryMessagePrefix => "__geo-projector:raw:category:";
    public const string RejectedRecordPrefix = "__geo-projector:rejected:";

    public static StorageKey CountryTaxonomyMaterializedView(SportId sportId)
    {
        string encodedSportId = EncodeSegment(sportId.Value);
        return $"__geo-projector:mv:country-taxonomy:{encodedSportId}";
    }

    public static StorageKey GeoTaxonomyViewMetadata(SportId sportId)
    {
        string encodedSportId = EncodeSegment(sportId.Value);
        return $"__geo-projector:metadata:geo-taxonomy:{encodedSportId}";
    }

    public static StorageKey RejectedRecord(string recordId)
    {
        string encodedRecordId = EncodeSegment(recordId);
        return $"{RejectedRecordPrefix}{encodedRecordId}";
    }

    #endregion

}
