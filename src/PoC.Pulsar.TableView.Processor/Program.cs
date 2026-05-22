using DotPulsar;
using Microsoft.Extensions.Logging;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Infrastructure.Store;
using PoC.Pulsar.TableView.Processor;

var serviceUrl = Environment.GetEnvironmentVariable("PULSAR_SERVICE_URL") ?? "pulsar://127.0.0.1:6650";
var inputNamespace = Environment.GetEnvironmentVariable("PULSAR_INPUT_NAMESPACE") ?? "public/tableview-inputs";

using var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .AddSimpleConsole(options =>
        {
            options.SingleLine = true;
            options.TimestampFormat = "HH:mm:ss ";
        })
        .SetMinimumLevel(LogLevel.Information);
});

using var cts = new CancellationTokenSource();

Console.CancelKeyPress += (_, args) =>
{
    args.Cancel = true;
    cts.Cancel();
};

await using var client = PulsarClient.Builder()
    .ServiceUrl(new Uri(serviceUrl))
    .Build();

var sportStore = new InMemoryStateStore<string, SportMessage>();
var categoryStore = new InMemoryStateStore<string, RawCategoryMessage>();

var sportDeserializer = new DefaultAvroSerializer<SportMessage>("SportMessage.avsc");
var categoryDeserializer = new DefaultAvroSerializer<RawCategoryMessage>("RawCategoryMessage.avsc");

var sportsView = new PulsarTableView<SportMessage>(
    client,
    BuildTopic(inputNamespace, "sports"),
    sportDeserializer.Deserialize,
    sportStore,
    loggerFactory.CreateLogger<PulsarTableView<SportMessage>>());

var categoriesView = new PulsarTableView<RawCategoryMessage>(
    client,
    BuildTopic(inputNamespace, "categories"),
    categoryDeserializer.Deserialize,
    categoryStore,
    loggerFactory.CreateLogger<PulsarTableView<RawCategoryMessage>>());

var processor = new GeoTaxonomyProcessor(
    sportsView,
    categoriesView,
    new TaxonomyViewPublisher(client, BuildTopic("public/tableview-outputs", "taxonomy-view"), new DefaultAvroSerializer<GeoTaxonomyMessage>("GeoTaxonomyMessage.avsc")),
    loggerFactory.CreateLogger<GeoTaxonomyProcessor>());

await processor.RunAsync(cts.Token);

static string BuildTopic(string @namespace, string topicName)
{
    return $"persistent://{@namespace}/{topicName}";
}
