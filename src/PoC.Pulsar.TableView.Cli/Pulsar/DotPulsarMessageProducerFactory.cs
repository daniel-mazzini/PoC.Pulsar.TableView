using System.Buffers;
using DotPulsar;
using DotPulsar.Abstractions;
using DotPulsar.Extensions;
using Microsoft.Extensions.Options;
using PoC.Pulsar.TableView.Cli.Configuration;

namespace PoC.Pulsar.TableView.Cli.Pulsar;

internal sealed class DotPulsarMessageProducerFactory : IPulsarMessageProducerFactory, IDisposable, IAsyncDisposable
{
    private readonly PulsarPublishOptions _options;
    private IPulsarClient? _client;

    public DotPulsarMessageProducerFactory(IOptions<PulsarPublishOptions> options)
    {
        _options = options.Value;
    }

    public Task<IPulsarMessageProducer> CreateAsync(string topic)
    {
        _client ??= PulsarClient.Builder()
            .ServiceUrl(new Uri(_options.ServiceUrl))
            .Build();

        var producer = _client.NewProducer(DotPulsar.Schema.ByteSequence)
            .Topic(topic)
            .Create();

        return Task.FromResult<IPulsarMessageProducer>(new DotPulsarMessageProducer(producer));
    }

    public async ValueTask DisposeAsync()
    {
        if (_client is not null)
        {
            await _client.DisposeAsync();
        }
    }

    public void Dispose()
    {
        if (_client is not null)
        {
            _client.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    private sealed class DotPulsarMessageProducer : IPulsarMessageProducer
    {
        private readonly IProducer<ReadOnlySequence<byte>> _producer;

        public DotPulsarMessageProducer(IProducer<ReadOnlySequence<byte>> producer)
        {
            _producer = producer;
        }

        public async Task SendAsync(string key, IReadOnlyDictionary<string, string> properties, byte[] payload)
        {
            var builder = _producer.NewMessage().Key(key);

            foreach (var property in properties)
            {
                builder = builder.Property(property.Key, property.Value);
            }

            await builder.Send(payload);
        }

        public ValueTask DisposeAsync()
        {
            return _producer.DisposeAsync();
        }
    }
}
