namespace PoC.Pulsar.TableView.Cli.Tsavorite;

internal interface ITsavoriteViewerClient
{
    Task<string> ListAsync(string type, int limit, CancellationToken cancellationToken);

    Task<string> GetAsync(string type, string key, CancellationToken cancellationToken);
}
