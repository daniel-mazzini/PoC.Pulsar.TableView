using CommandLine;

namespace PoC.Pulsar.TableView.Cli.Commands;

[Verb("compact-topic", HelpText = "Trigger Pulsar compaction for one of the supported topics.")]
public sealed class CompactTopicVerb
{
    [Value(0, Required = true, HelpText = "Topic to compact: sports, categories, or taxonomy-view.")]
    public string Topic { get; init; } = "";
}
