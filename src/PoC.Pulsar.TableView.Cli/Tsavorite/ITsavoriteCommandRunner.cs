using PoC.Pulsar.TableView.Cli.Commands;

namespace PoC.Pulsar.TableView.Cli.Tsavorite;

internal interface ITsavoriteCommandRunner
{
    Task<int> RunAsync(TsavoriteVerb verb, CancellationToken cancellationToken);
}
