using DotPulsar;
using Microsoft.Extensions.Logging;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Storages;
using PoC.Pulsar.TableView.Domain.Storages.Entities;
using PoC.Pulsar.TableView.Domain.Storages.Indexes;
using PoC.Pulsar.TableView.Infrastructure.Store;
using PoC.Pulsar.TableView.Infrastructure.Store.Publisher;
using PoC.Pulsar.TableView.Infrastructure.Store.Readers;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages;
using PoC.Pulsar.TableView.Processor;
using PoC.Pulsar.TableView.Processor.Configuration;

var serviceUrl = Environment.GetEnvironmentVariable("PULSAR_SERVICE_URL") ?? "pulsar://127.0.0.1:6650";
var inputNamespace = Environment.GetEnvironmentVariable("PULSAR_INPUT_NAMESPACE") ?? "public/tableview-inputs";

using var loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddSimpleConsole(options =>
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

var options = new ProjectorOptions();

await using var client = PulsarClient.Builder()
    .ServiceUrl(new Uri(options.ServiceUrl, UriKind.Absolute))
    .Build();

IStateSerializer serializer = new MemoryPackWrapper();
ITsavoriteEngine tsavoriteEngine = new TsavoriteEngine(options.StorePath);
loggerFactory.CreateLogger<Program>()
            .LogInformation("store initialized at path {StorePath}", options.StorePath);

IProjectorTopicReaderFactory readerFactory = new DotPulsarProjectorTopicReaderFactory(client, options.InputNamespace);
IAvroSerializer<GeoTaxonomyViewMessage> _projectionSerializer = new DefaultAvroSerializer<GeoTaxonomyViewMessage>("GeoTaxonomyViewMessage.avsc");
await using var publisher = new DotPulsarPropertyTaxonomyViewPublisher(client,options.OutputNamespace,_projectionSerializer);


ISportMessageStorage sportStore = new SportMessageStorage(tsavoriteEngine, serializer);
var sportDeserializer = new DefaultAvroSerializer<SportMessage>("SportMessage.avsc");

var sportsView = new PulsarTableView<SportMessage>(
    client,
    BuildTopic(inputNamespace, PulsarTopics.Sports),
    sportDeserializer.Deserialize,
    sportStore,
    loggerFactory.CreateLogger<PulsarTableView<SportMessage>>());

ICategoryMessageStorage categoryStore = new CategoryMessageStorage(tsavoriteEngine, serializer);
var categoryDeserializer = new DefaultAvroSerializer<RawCategoryMessage>("RawCategoryMessage.avsc");
var categoriesView = new PulsarTableView<RawCategoryMessage>(
    client,
    BuildTopic(inputNamespace, PulsarTopics.Categories),
    categoryDeserializer.Deserialize,
    categoryStore,
    loggerFactory.CreateLogger<PulsarTableView<RawCategoryMessage>>());

ICategoryBySportIndex categoryBySportIndex;
IOrphanCategoryBySportIndex orphanCategoryBySportIndex;
var processor = new GeoTaxonomyProcessor(
    sportsView,
    categoriesView,
    publisher,
    //new TaxonomyViewPublisher(client, BuildTopic("public/tableview-outputs", "taxonomy-view"), new DefaultAvroSerializer<GeoTaxonomyViewMessage>("GeoTaxonomyViewMessage.avsc")),
    loggerFactory.CreateLogger<GeoTaxonomyProcessor>());

await processor.RunAsync(cts.Token);

static string BuildTopic(string @namespace, string topicName)
{
    return $"persistent://{@namespace}/{topicName}";
}
