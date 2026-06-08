using PoC.Pulsar.TableView.Cli.Commands;
using PoC.Pulsar.TableView.Cli.CompactTopic;
using PoC.Pulsar.TableView.Cli.Publishing;
using PoC.Pulsar.TableView.Cli.Tsavorite;
using Xunit;

namespace PoC.Pulsar.TableView.Cli.UnitTests.Commands;

public sealed class PublishSampleApplicationTests
{
    [Fact]
    public void run_should_return_zero_for_help()
    {
        var publisher = new FakeSamplePublisher();
        var application = new PublishSampleApplication(publisher);

        var result = application.Run(["--help"]);

        Assert.Equal(0, result);
        Assert.False(publisher.WasCalled);
    }

    [Fact]
    public void run_should_return_zero_for_version()
    {
        var publisher = new FakeSamplePublisher();
        var application = new PublishSampleApplication(publisher);

        var result = application.Run(["--version"]);

        Assert.Equal(0, result);
        Assert.False(publisher.WasCalled);
    }

    [Fact]
    public void run_should_return_one_for_unknown_command()
    {
        var publisher = new FakeSamplePublisher();
        var application = new PublishSampleApplication(publisher);

        var result = application.Run(["unknown"]);

        Assert.Equal(1, result);
        Assert.False(publisher.WasCalled);
    }

    [Fact]
    public void run_should_call_sample_publisher_for_publish_sample_command()
    {
        var publisher = new FakeSamplePublisher();
        var application = new PublishSampleApplication(publisher);

        var result = application.Run(["publish-sample"]);

        Assert.Equal(0, result);
        Assert.True(publisher.WasCalled);
    }

    [Fact]
    public void run_should_call_tsavorite_runner_for_list_command()
    {
        var publisher = new FakeSamplePublisher();
        var runner = new FakeTsavoriteCommandRunner();
        var application = new PublishSampleApplication(publisher, runner, null);

        var result = application.Run(["tsavorite", "list", "sports"]);

        Assert.Equal(0, result);
        Assert.False(publisher.WasCalled);
        Assert.NotNull(runner.LastVerb);
        Assert.Equal("list", runner.LastVerb!.Operation);
        Assert.Equal("sports", runner.LastVerb.Type);
    }

    [Fact]
    public void run_should_call_tsavorite_runner_for_get_command()
    {
        var publisher = new FakeSamplePublisher();
        var runner = new FakeTsavoriteCommandRunner();
        var application = new PublishSampleApplication(publisher, runner, null);

        var result = application.Run(["tsavorite", "get", "sports", "--key", "sport-1"]);

        Assert.Equal(0, result);
        Assert.Equal("get", runner.LastVerb!.Operation);
        Assert.Equal("sports", runner.LastVerb.Type);
        Assert.Equal("sport-1", runner.LastVerb.Key);
    }

    [Fact]
    public void run_should_call_tsavorite_runner_for_watch_command()
    {
        var publisher = new FakeSamplePublisher();
        var runner = new FakeTsavoriteCommandRunner();
        var application = new PublishSampleApplication(publisher, runner, null);

        var result = application.Run(["tsavorite", "list", "sports", "--watch", "20s"]);

        Assert.Equal(0, result);
        Assert.Equal("20s", runner.LastVerb!.Watch);
    }

    [Fact]
    public void run_should_call_compact_topic_runner_for_compact_topic_command()
    {
        var publisher = new FakeSamplePublisher();
        var runner = new FakeCompactTopicCommandRunner();
        var application = new PublishSampleApplication(publisher, tsavoriteCommandRunner: null, compactTopicCommandRunner: runner);

        var result = application.Run(["compact-topic", "sports"]);

        Assert.Equal(0, result);
        Assert.False(publisher.WasCalled);
        Assert.NotNull(runner.LastVerb);
        Assert.Equal("sports", runner.LastVerb!.Topic);
    }

    private sealed class FakeSamplePublisher : ISamplePublisher
    {
        public bool WasCalled { get; private set; }

        public Task PublishAsync()
        {
            WasCalled = true;
            return Task.CompletedTask;
        }
    }

    private sealed class FakeTsavoriteCommandRunner : ITsavoriteCommandRunner
    {
        public TsavoriteVerb? LastVerb { get; private set; }

        public Task<int> RunAsync(TsavoriteVerb verb, CancellationToken cancellationToken)
        {
            LastVerb = verb;
            return Task.FromResult(0);
        }
    }

    private sealed class FakeCompactTopicCommandRunner : ICompactTopicCommandRunner
    {
        public CompactTopicVerb? LastVerb { get; private set; }

        public Task<int> RunAsync(CompactTopicVerb verb, CancellationToken cancellationToken)
        {
            LastVerb = verb;
            return Task.FromResult(0);
        }
    }
}
