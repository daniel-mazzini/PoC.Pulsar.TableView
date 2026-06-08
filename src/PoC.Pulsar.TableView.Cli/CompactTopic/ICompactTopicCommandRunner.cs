using PoC.Pulsar.TableView.Cli.Commands;

namespace PoC.Pulsar.TableView.Cli.CompactTopic;

internal interface ICompactTopicCommandRunner
{
    Task<int> RunAsync(CompactTopicVerb verb, CancellationToken cancellationToken);
}
