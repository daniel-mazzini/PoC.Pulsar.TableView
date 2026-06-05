using System.Diagnostics;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;
using PoC.Pulsar.TableView.Domain.Serializers;
using PoC.Pulsar.TableView.Infrastructure.Store.Inspection;
using PoC.Pulsar.TableView.Infrastructure.Store.Observability;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages;
using PoC.Pulsar.TableView.Processor;
using PoC.Pulsar.TableView.Processor.Configuration;

var options = ProjectorOptions.FromEnvironment();
var tsavoriteViewerOptions = TsavoriteViewerOptions.FromEnvironment();
using var tracerProvider = CreateTracerProvider();
using var meterProvider = CreateMeterProvider();
var services = new ServiceCollection()
    .AddProcessorLogging()
    .AddProcessorServices(options);
    
await using var serviceProvider = services.BuildServiceProvider(new ServiceProviderOptions
{
    ValidateOnBuild = true,
    ValidateScopes = true
});

using var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, args) =>
{
    args.Cancel = true;
    cts.Cancel();
};

var programLogger = serviceProvider.GetRequiredService<ILogger<Program>>();
var tsavoriteEngine = serviceProvider.GetRequiredService<ITsavoriteEngine>();
var serializer = serviceProvider.GetRequiredService<IStateSerializer>();
var processor = serviceProvider.GetRequiredService<GeoTaxonomyProcessor>();
programLogger.LogInformation("store initialized at path {StorePath}", options.StorePath);

WebApplication? tsavoriteViewerApp = null;
if (tsavoriteViewerOptions.Enabled)
{
    tsavoriteViewerApp = CreateTsavoriteViewerApp(tsavoriteEngine, serializer, tsavoriteViewerOptions.Url);
    await tsavoriteViewerApp.StartAsync(cts.Token);
    programLogger.LogInformation("tsavorite viewer listening at {TsavoriteViewerUrl}", tsavoriteViewerOptions.Url);
}
else
{
    programLogger.LogInformation("tsavorite viewer disabled. Set TSAVORITE_VIEWER_ENABLED=true to enable it.");
}

using var processorActivity = ProjectorStoreTelemetry.StartActivity("processor.run",
                                                                    operation: "GeoTaxonomyProcessor.RunAsync");
processorActivity?.SetTag("pulsar.service_url", options.ServiceUrl);
processorActivity?.SetTag("pulsar.input_namespace", options.InputNamespace);
processorActivity?.SetTag("pulsar.output_namespace", options.OutputNamespace);
processorActivity?.SetTag("store.path", options.StorePath);
processorActivity?.SetTag("tsavorite.viewer.enabled", tsavoriteViewerOptions.Enabled);
programLogger.LogInformation("processor starting with Pulsar service {ServiceUrl}, input namespace {InputNamespace}, output namespace {OutputNamespace}",
                             options.ServiceUrl,
                             options.InputNamespace,
                             options.OutputNamespace);

try
{
    await processor.RunAsync(cts.Token);
    processorActivity?.SetTag("result", cts.IsCancellationRequested ? "cancelled" : "completed");
    programLogger.LogInformation("processor stopped.");
}
catch (OperationCanceledException) when (cts.IsCancellationRequested)
{
    processorActivity?.SetTag("result", "cancelled");
    programLogger.LogInformation("processor cancellation requested.");
}
catch (Exception exception)
{
    processorActivity?.SetTag("result", "error");
    processorActivity?.SetStatus(ActivityStatusCode.Error, exception.GetType().Name);
    programLogger.LogError(exception, "processor failed.");
    throw;
}
finally
{
    if (tsavoriteViewerApp is not null)
    {
        await tsavoriteViewerApp.StopAsync(CancellationToken.None);
        await tsavoriteViewerApp.DisposeAsync();
    }
}

static TracerProvider CreateTracerProvider()
{
    var builder = Sdk.CreateTracerProviderBuilder()
                     .SetResourceBuilder(CreateResourceBuilder())
                     .AddSource(ProjectorStoreTelemetry.Name);

    if (HasOtlpEndpoint())
    {
        builder.AddOtlpExporter();
    }

    return builder.Build();
}

static MeterProvider CreateMeterProvider()
{
    var builder = Sdk.CreateMeterProviderBuilder()
                     .SetResourceBuilder(CreateResourceBuilder())
                     .AddMeter(ProjectorStoreTelemetry.Name);

    if (HasOtlpEndpoint())
    {
        builder.AddOtlpExporter();
    }

    return builder.Build();
}

static ResourceBuilder CreateResourceBuilder()
    => ResourceBuilder.CreateDefault()
                      .AddService("poc-pulsar-tableview-processor");

static bool HasOtlpEndpoint()
    => !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT"))
       || !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"))
       || !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"));

static WebApplication CreateTsavoriteViewerApp(ITsavoriteEngine engine, IStateSerializer serializer, string url)
{
    var builder = WebApplication.CreateSlimBuilder();
    builder.WebHost.UseUrls(url);

    var app = builder.Build();
    var viewer = new TsavoriteViewer(engine, serializer);

    app.MapGet("/tsavorite/types", () => Results.Ok(TsavoriteViewer.SupportedTypes()));
    app.MapGet("/tsavorite/{type}", (string type, int? limit) =>
    {
        try
        {
            return Results.Ok(viewer.List(type, limit ?? TsavoriteViewer.DefaultLimit));
        }
        catch (NotSupportedException exception)
        {
            return Results.BadRequest(new { error = exception.Message });
        }
    });

    app.MapGet("/tsavorite/{type}/{key}", (string type, string key) =>
    {
        try
        {
            var entry = viewer.Get(type, key);
            return entry is null
                ? Results.NotFound(new { error = $"Tsavorite entry '{type}/{key}' was not found." })
                : Results.Ok(entry);
        }
        catch (NotSupportedException exception)
        {
            return Results.BadRequest(new { error = exception.Message });
        }
    });

    return app;
}
