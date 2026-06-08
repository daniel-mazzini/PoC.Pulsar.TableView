namespace PoC.Pulsar.TableView.Cli.Pulsar;

internal interface IPulsarAdminClient
{
    Task TriggerCompactionAsync(string topic, CancellationToken cancellationToken);
}
