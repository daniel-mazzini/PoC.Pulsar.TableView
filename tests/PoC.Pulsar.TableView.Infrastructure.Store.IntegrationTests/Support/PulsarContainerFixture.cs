using DotPulsar;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

public sealed class PulsarContainerFixture : IAsyncLifetime
{
    private readonly IContainer _container;

    public PulsarContainerFixture()
    {
        _container = new ContainerBuilder()
            .WithImage("apachepulsar/pulsar:3.2.1")
            .WithPortBinding(6650, assignRandomHostPort: true)
            .WithPortBinding(8080, assignRandomHostPort: true)
            .WithEntrypoint("/bin/bash")
            .WithCommand("-c", "bin/pulsar standalone")
            .Build();
    }

    public string BrokerUrl => $"pulsar://127.0.0.1:{_container.GetMappedPublicPort(6650)}";

    public string AdminUrl => $"http://127.0.0.1:{_container.GetMappedPublicPort(8080)}";

    public async ValueTask<IPulsarClient> CreateClientAsync()
    {
        await EnsureAdminReadyAsync();
        return PulsarClient.Builder()
            .ServiceUrl(new Uri(BrokerUrl, UriKind.Absolute))
            .Build();
    }

    public async Task<string> CreateNamespaceAsync(string prefix)
    {
        var namespaceName = $"{prefix}-{Guid.CreateVersion7():N}".ToLowerInvariant();
        await ExecAdminCommandAsync($"bin/pulsar-admin --admin-url http://127.0.0.1:8080 namespaces create public/{namespaceName} || true");
        return $"public/{namespaceName}";
    }

    public Task CreateTopicAsync(string topicNamespace, string topicName)
        => ExecAdminCommandAsync($"bin/pulsar-admin --admin-url http://127.0.0.1:8080 topics create persistent://{topicNamespace}/{topicName} || true");

    public Task CreatePartitionedTopicAsync(string topicNamespace, string topicName, int partitions)
        => ExecAdminCommandAsync($"bin/pulsar-admin --admin-url http://127.0.0.1:8080 topics create-partitioned-topic persistent://{topicNamespace}/{topicName} -p {partitions} || true");

    public async ValueTask InitializeAsync()
    {
        await _container.StartAsync();
        await EnsureAdminReadyAsync();
    }

    public async ValueTask DisposeAsync()
    {
        await _container.DisposeAsync();
    }

    private async Task EnsureAdminReadyAsync()
    {
        var deadline = DateTimeOffset.UtcNow.AddMinutes(2);

        while (DateTimeOffset.UtcNow < deadline)
        {
            try
            {
                var result = await _container.ExecAsync(["/bin/bash", "-lc", "bin/pulsar-admin --admin-url http://127.0.0.1:8080 brokers healthcheck"]);
                if (result.ExitCode == 0)
                {
                    return;
                }
            }
            catch (InvalidOperationException)
            {
            }

            await Task.Delay(TimeSpan.FromSeconds(2));
        }

        throw new TimeoutException("Pulsar admin endpoint did not become ready in time.");
    }

    private async Task ExecAdminCommandAsync(string command)
    {
        await EnsureAdminReadyAsync();

        var result = await _container.ExecAsync(["/bin/bash", "-lc", command]);
        if (result.ExitCode == 0)
        {
            return;
        }

        throw new InvalidOperationException($"Pulsar admin command failed: {command}{Environment.NewLine}{result.Stderr}");
    }
}
