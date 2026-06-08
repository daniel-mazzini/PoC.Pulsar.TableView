using Microsoft.Extensions.Options;
using PoC.Pulsar.TableView.Cli.Commands;
using PoC.Pulsar.TableView.Cli.CompactTopic;
using PoC.Pulsar.TableView.Cli.Configuration;
using PoC.Pulsar.TableView.Cli.Pulsar;
using Xunit;

namespace PoC.Pulsar.TableView.Cli.UnitTests.CompactTopic;

public sealed class CompactTopicCommandRunnerTests
{
    [Theory]
    [InlineData("sports", "persistent://public/input/sports")]
    [InlineData("categories", "persistent://public/input/categories")]
    [InlineData("sport-country-taxonomy-views", "persistent://public/output/sport-country-taxonomy-views")]
    public async Task run_async_should_trigger_supported_topic_compaction(string topic, string expectedTopic)
    {
        var adminClient = new FakePulsarAdminClient();
        var runner = new CompactTopicCommandRunner(adminClient, Options.Create(new PulsarPublishOptions
        {
            InputNamespace = "public/input",
            OutputNamespace = "public/output"
        }));

        var result = await runner.RunAsync(new CompactTopicVerb { Topic = topic }, CancellationToken.None);

        Assert.Equal(0, result);
        Assert.Equal(expectedTopic, adminClient.LastTopic);
    }

    [Fact]
    public async Task run_async_should_throw_for_unsupported_topic()
    {
        var adminClient = new FakePulsarAdminClient();
        var runner = new CompactTopicCommandRunner(adminClient, Options.Create(new PulsarPublishOptions
        {
            InputNamespace = "public/input",
            OutputNamespace = "public/output"
        }));

        await Assert.ThrowsAsync<InvalidOperationException>(() => runner.RunAsync(new CompactTopicVerb
        {
            Topic = "unknown"
        }, CancellationToken.None));

        Assert.Null(adminClient.LastTopic);
    }

    private sealed class FakePulsarAdminClient : IPulsarAdminClient
    {
        public string? LastTopic { get; private set; }

        public Task TriggerCompactionAsync(string topic, CancellationToken cancellationToken)
        {
            LastTopic = topic;
            return Task.CompletedTask;
        }
    }
}
