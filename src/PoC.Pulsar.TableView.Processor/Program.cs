using DotPulsar;
using Microsoft.Extensions.Logging;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Entities;
using PoC.Pulsar.TableView.Domain.Storages;
using PoC.Pulsar.TableView.Domain.Storages.Controls;
using PoC.Pulsar.TableView.Domain.Storages.Entities;
using PoC.Pulsar.TableView.Domain.Storages.Indexes;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Infrastructure.Store;
using PoC.Pulsar.TableView.Infrastructure.Store.Publisher;
using PoC.Pulsar.TableView.Infrastructure.Store.Readers;
using PoC.Pulsar.TableView.Infrastructure.Store.Serialization;
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

// State serializar (used when save in store)
IStateSerializer serializer = new MemoryPackWrapper();
ITsavoriteEngine tsavoriteEngine = new TsavoriteEngine(options.StorePath);
loggerFactory.CreateLogger<Program>()
            .LogInformation("store initialized at path {StorePath}", options.StorePath);

// Message serialization
AvroSchemaRegistry avroSchemaRegistry = new AvroSchemaRegistry();
avroSchemaRegistry.Register<SportMessage>("./Schemas/SportMessage");
var avroSerializer = avroSchemaRegistry.Build();

// Metadata storage
IMetadataStorage metadataStorage = new MetadataStorage(tsavoriteEngine, serializer);
StoreMetadata metadata = await metadataStorage.EnsureMetadataAsync(CancellationToken.None);

// Unit of Work
IUnitOfWorkFactory unitOfWorkFactory = new UnitOfWorkFactory(tsavoriteEngine, metadataStorage, serializer);

IProjectorTopicReaderFactory readerFactory = new DotPulsarProjectorTopicReaderFactory(client, options.InputNamespace);

await using var projectorPublisher = new DotPulsarPropertyTaxonomyViewPublisher(client,options.OutputNamespace,avroSerializer);
await using var rejectedPublisher = new DotPulsarRejectedMessagePublisher(client, options.OutputNamespace, avroSerializer);

var sportsView = new PulsarTableView<SportMessage>(
    BuildTopic(inputNamespace, PulsarTopics.Sports),
    readerFactory,
    rejectedPublisher,
    unitOfWorkFactory,
    avroSerializer,
    metadata,
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
