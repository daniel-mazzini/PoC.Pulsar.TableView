using PoC.Pulsar.TableView.Cli.Commands;
using PoC.Pulsar.TableView.Cli.Configuration;
using PoC.Pulsar.TableView.Cli.Pulsar;

namespace PoC.Pulsar.TableView.Cli.CompactTopic;

internal sealed class CompactTopicCommandRunner : ICompactTopicCommandRunner
{
    private readonly IPulsarAdminClient _adminClient;
    private readonly PulsarPublishOptions _options;

    public CompactTopicCommandRunner(IPulsarAdminClient adminClient, Microsoft.Extensions.Options.IOptions<PulsarPublishOptions> options)
    {
        _adminClient = adminClient;
        _options = options.Value;
    }

    public async Task<int> RunAsync(CompactTopicVerb verb, CancellationToken cancellationToken)
    {
        var topic = ResolveTopic(verb.Topic);

        await _adminClient.TriggerCompactionAsync(topic, cancellationToken);
        Console.WriteLine($"Triggered compaction for {topic}");
        return 0;
    }

    private string ResolveTopic(string topic)
    {
        return topic.ToLowerInvariant() switch
        {
            "sports" => BuildTopic(_options.InputNamespace, "sports"),
            "categories" => BuildTopic(_options.InputNamespace, "categories"),
            "taxonomy-view" => BuildTopic(_options.OutputNamespace, "taxonomy-view"),
            _ => throw new InvalidOperationException("The compact-topic command only supports sports, categories, and taxonomy-view.")
        };
    }

    private static string BuildTopic(string topicNamespace, string topicName)
        => $"persistent://{topicNamespace}/{topicName}";
}
