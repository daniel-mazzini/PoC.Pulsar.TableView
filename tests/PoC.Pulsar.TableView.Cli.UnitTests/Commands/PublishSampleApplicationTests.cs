using PoC.Pulsar.TableView.Cli.Commands;
using PoC.Pulsar.TableView.Cli.Publishing;
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

    private sealed class FakeSamplePublisher : ISamplePublisher
    {
        public bool WasCalled { get; private set; }

        public Task PublishAsync()
        {
            WasCalled = true;
            return Task.CompletedTask;
        }
    }
}
