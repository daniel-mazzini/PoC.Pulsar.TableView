namespace PoC.Pulsar.TableView.Cli.Pulsar;

internal interface IPulsarMessageProducerFactory
{
    Task<IPulsarMessageProducer> CreateAsync(string topic);
}
