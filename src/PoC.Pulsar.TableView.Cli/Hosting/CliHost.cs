using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using PoC.Pulsar.TableView.Cli.Commands;
using PoC.Pulsar.TableView.Cli.Configuration;
using PoC.Pulsar.TableView.Cli.Publishing;
using PoC.Pulsar.TableView.Cli.Pulsar;
using PoC.Pulsar.TableView.Cli.Samples;
using PoC.Pulsar.TableView.Cli.Tsavorite;

namespace PoC.Pulsar.TableView.Cli.Hosting;

internal static class CliHost
{
    public static IHost Create(string[] args)
    {
        var builder = Host.CreateApplicationBuilder(new HostApplicationBuilderSettings
        {
            Args = args,
            ContentRootPath = AppContext.BaseDirectory
        });

        builder.Logging.ClearProviders();
        builder.Logging.AddSimpleConsole(options =>
        {
            options.SingleLine = true;
            options.TimestampFormat = "HH:mm:ss ";
        });

        builder.Services
            .AddOptions<PulsarPublishOptions>()
            .Bind(builder.Configuration.GetSection(PulsarPublishOptions.SectionName))
            .Validate(options => Uri.TryCreate(options.ServiceUrl, UriKind.Absolute, out _), "Pulsar:ServiceUrl must be a valid URI.")
            .Validate(options => !string.IsNullOrWhiteSpace(options.InputNamespace), "Pulsar:InputNamespace is required.");

        builder.Services
            .AddOptions<TsavoriteViewerOptions>()
            .Bind(builder.Configuration.GetSection(TsavoriteViewerOptions.SectionName))
            .Validate(options => Uri.TryCreate(options.BaseUrl, UriKind.Absolute, out _), "TsavoriteViewer:BaseUrl must be a valid URI.");

        builder.Services.AddSingleton<PublishSampleApplication>();
        builder.Services.AddSingleton<ISamplePublisher, SamplePublisher>();
        builder.Services.AddSingleton<ISampleDataLoader, SampleDataLoader>();
        builder.Services.AddSingleton<ISampleSchemaSerializer, AvroSampleSchemaSerializer>();
        builder.Services.AddSingleton<IPulsarMessageProducerFactory, DotPulsarMessageProducerFactory>();
        builder.Services.AddSingleton<ITsavoriteViewerClient, TsavoriteViewerClient>();
        builder.Services.AddSingleton<ITsavoriteCommandRunner, TsavoriteCommandRunner>();

        return builder.Build();
    }
}
