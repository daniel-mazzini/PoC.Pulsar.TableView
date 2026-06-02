using DotPulsar;
using Microsoft.Extensions.Logging;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Categories;
using PoC.Pulsar.TableView.Domain.MaterializeViews;
using PoC.Pulsar.TableView.Domain.Metadatas;
using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Domain.Serializers;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Infrastructure.Store;
using PoC.Pulsar.TableView.Infrastructure.Store.Publisher;
using PoC.Pulsar.TableView.Infrastructure.Store.Readers;
using PoC.Pulsar.TableView.Infrastructure.Store.Serialization;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.UnitOfWorks;
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
avroSchemaRegistry.Register<RawCategoryMessage>("./Schemas/RawCategoryMessage");
var avroSerializer = avroSchemaRegistry.Build();

// Metadata storage
IMetadataStorage metadataStorage = new MetadataStorage(tsavoriteEngine, serializer);
StoreMetadata metadata = await metadataStorage.EnsureMetadataAsync(CancellationToken.None);

// Unit of Work
IUnitOfWorkFactory unitOfWorkFactory = new UnitOfWorkFactory(tsavoriteEngine, metadataStorage, serializer);

IProjectorTopicReaderFactory readerFactory = new DotPulsarProjectorTopicReaderFactory(client, options.InputNamespace);

await using var projectorPublisher = new DotPulsarPropertyTaxonomyViewPublisher(client, options.OutputNamespace, avroSerializer);
await using var rejectedPublisher = new DotPulsarRejectedMessagePublisher(client, options.OutputNamespace, avroSerializer);


ITableViewMessageApplier<SportMessage> projectorMessageApplier = new SportMessageApplier(rejectedPublisher);
ITableViewMessageApplier<RawCategoryMessage> categoryMessageApplier = new RawCategoryMessageApplier(rejectedPublisher);

var sportsView = new PulsarTableView<SportMessage>(
    BuildTopic(inputNamespace, PulsarTopics.Sports),
    readerFactory,
    unitOfWorkFactory,
    avroSerializer,
    projectorMessageApplier,
    metadata,
    loggerFactory.CreateLogger<PulsarTableView<SportMessage>>());

var categoriesView = new PulsarTableView<RawCategoryMessage>(
    BuildTopic(inputNamespace, PulsarTopics.Categories),
    readerFactory,
    unitOfWorkFactory,
    avroSerializer,
    categoryMessageApplier,
    metadata,
    loggerFactory.CreateLogger<PulsarTableView<RawCategoryMessage>>());

ICategoryBySportIndex categoryBySportIndex = new InMemoryCategoryBySportIndex();
IOrphanCategoryBySportIndex orphanCategoryBySportIndex = new InMemoryOrphanCategoryBySportIndex();
IGeoTaxonomyViewStorage taxonomyViewStorage = new InMemoryGeoTaxonomyViewStorage();
var processor = new GeoTaxonomyProcessor(
    sportsView,
    categoriesView,
    projectorPublisher,
    categoryBySportIndex,
    orphanCategoryBySportIndex,
    taxonomyViewStorage,
    unitOfWorkFactory,
    metadata,
    loggerFactory.CreateLogger<GeoTaxonomyProcessor>());

await processor.RunAsync(cts.Token);

static string BuildTopic(string @namespace, string topicName)
{
    return $"persistent://{@namespace}/{topicName}";
}
