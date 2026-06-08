using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using PoC.Pulsar.TableView.Cli.Commands;
using PoC.Pulsar.TableView.Cli.CompactTopic;
using PoC.Pulsar.TableView.Cli.Configuration;
using PoC.Pulsar.TableView.Cli.Hosting;
using PoC.Pulsar.TableView.Cli.Tsavorite;
using Xunit;

namespace PoC.Pulsar.TableView.Cli.UnitTests.Hosting;

public sealed class CliHostTests
{
    [Fact]
    public void create_should_register_expected_services()
    {
        using var host = CliHost.Create([
            "--Pulsar:ServiceUrl=http://localhost:6650",
            "--Pulsar:AdminUrl=http://localhost:8080",
            "--Pulsar:InputNamespace=public/default",
            "--Pulsar:OutputNamespace=public/tableview-outputs",
            "--TsavoriteViewer:BaseUrl=http://127.0.0.1:18080"]);

        Assert.NotNull(host.Services.GetRequiredService<PublishSampleApplication>());
        Assert.NotNull(host.Services.GetRequiredService<ITsavoriteCommandRunner>());
        Assert.NotNull(host.Services.GetRequiredService<ICompactTopicCommandRunner>());
        Assert.Equal("http://localhost:6650", host.Services.GetRequiredService<IOptions<PulsarPublishOptions>>().Value.ServiceUrl);
        Assert.Equal("http://127.0.0.1:18080", host.Services.GetRequiredService<IOptions<TsavoriteViewerOptions>>().Value.BaseUrl);
    }

    [Fact]
    public void create_should_validate_pulsar_service_url()
    {
        using var host = CliHost.Create([
            "--Pulsar:ServiceUrl=not-a-uri",
            "--Pulsar:AdminUrl=http://localhost:8080",
            "--Pulsar:InputNamespace=public/default",
            "--Pulsar:OutputNamespace=public/tableview-outputs"]);

        var exception = Assert.Throws<OptionsValidationException>(() => host.Services.GetRequiredService<IOptions<PulsarPublishOptions>>().Value);

        Assert.Contains("Pulsar:ServiceUrl must be a valid URI.", exception.Failures);
    }
}
