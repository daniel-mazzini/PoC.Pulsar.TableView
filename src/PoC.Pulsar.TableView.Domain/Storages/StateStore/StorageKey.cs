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

    /// <summary>
    /// Replace ':' characters with '-' efficiently with span.
    /// </summary>
    private static string SanitizeId(string id)
    {
        if (string.IsNullOrEmpty(id)) return id;

        ReadOnlySpan<char> idSpan = id.AsSpan();

        int colonIndex = idSpan.IndexOf(':');

        if (colonIndex == -1)
        {
            return id;
        }

        return string.Create(id.Length, id, (destinationSpan, originalId) =>
        {
            ReadOnlySpan<char> sourceSpan = originalId.AsSpan();

            for (int i = 0; i < sourceSpan.Length; i++)
            {
                char c = sourceSpan[i];
                // Reemplazamos en caliente mientras escribimos el string definitivo
                destinationSpan[i] = (c == ':') ? '-' : c;
            }
        });
    }

    #region keyFactory

    public const string StoreMetadata = "__geo-projector:store-metadata";
    public const string StoreDiagnostics = "__geo-projector:store-diagnostics";
    internal const string PrefixStoreCategoryBySport = "__geo-projector:idx:category:by-sport:";
    internal const string PrefixStoreCategoryByParent = "__geo-projector:idx:category:by-parent:";

    public const string SportMessagePrefix = "__geo-projector:raw:sport:";
    public static StorageKey TopicCheckpoint(string topicName, int partitionId)
    {
        string safeTopicName = SanitizeId(topicName);
        return $"__geo-projector:topic-checkpoint:{safeTopicName}:{partitionId}";
    }

    public static StorageKey ViewCheckpoint(string viewName)
    {
        string safeViewName = SanitizeId(viewName);
        return $"__geo-projector:view-checkpoint:{safeViewName}";
    }

    
    public static StorageKey CategoryBySportPrefix(SportId sportId)
    {
        string safeSportId = SanitizeId(sportId.Value);
        return $"{PrefixStoreCategoryBySport}{safeSportId}:category:";
    }

    public static StorageKey CategoryBySport(SportId sportId, CategoryId categoryId)
    {
        string safeCategoryId = SanitizeId(categoryId.Value);
        return $"{CategoryBySportPrefix(sportId).Value}{safeCategoryId}";
    }

    public static StorageKey CategoryByParentPrefix(CategoryId parentCategoryId)
    {
        string safeParentCategoryId = SanitizeId(parentCategoryId.Value);
        return $"{PrefixStoreCategoryByParent}{safeParentCategoryId}:category:";
    }

    public static StorageKey CategoryByParent(CategoryId parentCategoryId, CategoryId categoryId)
    {
        string safeCategoryId = SanitizeId(categoryId.Value);
        return $"{CategoryByParentPrefix(parentCategoryId).Value}{safeCategoryId}";
    }
    public static StorageKey OrphanCategoryBySportPrefix(SportId sportId)
    {
        string safeSportId = SanitizeId(sportId.Value);
        return $"__geo-projector:missing:category:by-sport:{safeSportId}:category:";
    }
    public static StorageKey OrphanCategoryBySport(SportId sportId, CategoryId categoryId)
    {
        string safeCategoryId = SanitizeId(categoryId.Value);
        return $"{OrphanCategoryBySportPrefix(sportId).Value}{safeCategoryId}";
    }
    public static StorageKey SportMessage(string sportId)
    {
        string safeSportId = SanitizeId(sportId);
        return $"{SportMessagePrefix}{safeSportId}";
    }

    public static StorageKey OrphanSportByCategoryPrefix(CategoryId categoryId)
    {
        string safeCategoryId = SanitizeId(categoryId.Value);
        return $"__geo-projector:missing:sport:by-category:{safeCategoryId}:sport:";
    }

    public static StorageKey OrphanSportByCategory(CategoryId categoryId, SportId sportId)
    {
        string safeSportId = SanitizeId(sportId.Value);
        return $"{OrphanSportByCategoryPrefix(categoryId).Value}{safeSportId}";
    }


    public static StorageKey CategoryMessage(string categoryId)
    {
        string safeCategoryId = SanitizeId(categoryId);
        return $"__geo-projector:raw:category:{safeCategoryId}";
    }

    public static StorageKey CategoryMessagePrefix => "__geo-projector:raw:category:";

    public static StorageKey CountryTaxonomyMaterializedView(SportId sportId)
    {
        string safeSportId = SanitizeId(sportId.Value);
        return $"__geo-projector:mv:country-taxonomy:{safeSportId}";
    }

    public static StorageKey GeoTaxonomyViewMetadata(SportId sportId)
    {
        string safeSportId = SanitizeId(sportId.Value);
        return $"__geo-projector:metadata:geo-taxonomy:{safeSportId}";
    }

    public static StorageKey CategoryIdsBySport(SportId sportId)
    {
        string safeSportId = SanitizeId(sportId.Value);
        return $"__geo-projector:idx:category-ids:by-sport:{safeSportId}";
    }

    public static StorageKey OrphanCategoryIdsBySport(SportId sportId)
    {
        string safeSportId = SanitizeId(sportId.Value);
        return $"__geo-projector:missing:category-ids:by-sport:{safeSportId}";
    }

    public static StorageKey PendingCountByPropertyType(SportId sportId)
    {
        string safeSportId = SanitizeId(sportId.Value);
        return $"__geo-projector:idx:missing-count-category:sport:{safeSportId}";
    }

    public static StorageKey RejectedRecord(string recordId)
    {
        string safeRecordId = SanitizeId(recordId);
        return $"__geo-projector:rejected:{safeRecordId}";
    }

    #endregion

}
