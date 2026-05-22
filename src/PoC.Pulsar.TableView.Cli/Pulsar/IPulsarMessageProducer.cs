namespace PoC.Pulsar.TableView.Cli.Pulsar;

internal interface IPulsarMessageProducer : IAsyncDisposable
{
    Task SendAsync(string key, IReadOnlyDictionary<string, string> properties, byte[] payload);
}
