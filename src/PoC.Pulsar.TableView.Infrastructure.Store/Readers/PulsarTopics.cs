namespace PoC.Pulsar.TableView.Infrastructure.Store.Readers;

public static class PulsarTopics
{
    public const string Sports = "sports";
    public const string Categories = "categories";
    public const string CountryTaxonomyViews = "sport-country-taxonomy-views";
    public const string SportsRejected = "sports-rejected";
    public const string CategoriesRejected = "categories-rejected";
    public const string MissingViewSla = "missing-propertyview-sla";

    public static string Partition(string topicName, int partitionId) => $"{topicName}-partition-{partitionId}";

    public static string Qualify(string topicNamespace, string topicName) => $"persistent://{topicNamespace}/{topicName}";

    public static string QualifyIfNeeded(string topicNamespace, string topicName)
        => topicName.StartsWith("persistent://", StringComparison.Ordinal) || topicName.StartsWith("non-persistent://", StringComparison.Ordinal)
            ? topicName
            : Qualify(topicNamespace, topicName);
}
