using DotPulsar;
using DotPulsar.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.MaterializeViews;
using PoC.Pulsar.TableView.Domain.Metadatas;
using PoC.Pulsar.TableView.Domain.Projector;
using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Domain.Serializers;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Domain.TableView;
using PoC.Pulsar.TableView.Infrastructure.Store;
using PoC.Pulsar.TableView.Infrastructure.Store.Publisher;
using PoC.Pulsar.TableView.Infrastructure.Store.Readers;
using PoC.Pulsar.TableView.Infrastructure.Store.Serialization;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.UnitOfWorks;
using PoC.Pulsar.TableView.Processor.Configuration;

namespace PoC.Pulsar.TableView.Processor;

internal static class ProcessorServiceCollectionExtensions
{
    public static IServiceCollection AddProcessorServices(this IServiceCollection services, ProjectorOptions options)
    {
        services.AddSingleton(options);

        services.AddSingleton<IPulsarClient>(_ => PulsarClient.Builder()
            .ServiceUrl(new Uri(options.ServiceUrl, UriKind.Absolute))
            .Build());

        services.AddSingleton<IStateSerializer, MemoryPackWrapper>();
        services.AddSingleton<ITsavoriteEngine>(sp => new TsavoriteEngine(sp.GetRequiredService<ProjectorOptions>().StorePath));
        services.AddSingleton<IStateSession>(sp => new TsavoriteSessionWrapper(sp.GetRequiredService<ITsavoriteEngine>()));
        services.AddSingleton<IMetadataStorage, MetadataStorage>();
        services.AddSingleton(sp => sp.GetRequiredService<IMetadataStorage>()
                                      .EnsureMetadataAsync(CancellationToken.None)
                                      .GetAwaiter()
                                      .GetResult());
        services.AddSingleton<IUnitOfWorkFactory, UnitOfWorkFactory>();

        services.AddSingleton<IAvroSerializer>(_ => BuildAvroSerializer());
        services.AddSingleton<ITopicShardReaderStrategy>(sp =>
            new DotPulsarProjectorTopicReaderFactory(sp.GetRequiredService<IPulsarClient>(),
                                                     sp.GetRequiredService<ProjectorOptions>().InputNamespace));

        services.AddSingleton<ITaxonomyViewPublisher>(sp =>
            new DotPulsarPropertyTaxonomyViewPublisher(sp.GetRequiredService<IPulsarClient>(),
                                                       sp.GetRequiredService<ProjectorOptions>().OutputNamespace,
                                                       sp.GetRequiredService<IAvroSerializer>()));
        services.AddSingleton<IRejectedMessagePublisher>(sp =>
            new DotPulsarRejectedMessagePublisher(sp.GetRequiredService<IPulsarClient>(),
                                                  sp.GetRequiredService<ProjectorOptions>().OutputNamespace,
                                                  sp.GetRequiredService<IAvroSerializer>()));

        services.AddSingleton<ITableViewMessageApplier<SportMessage>, SportMessageApplier>();
        services.AddSingleton<ITableViewMessageApplier<RawCategoryMessage>, RawCategoryMessageApplier>();
        services.AddSingleton<IPulsarTableView<SportMessage>>(sp =>
            new PulsarTableView<SportMessage>(
                BuildTopic(sp.GetRequiredService<ProjectorOptions>().InputNamespace, PulsarTopics.Sports),
                sp.GetRequiredService<ITopicShardReaderStrategy>(),
                sp.GetRequiredService<IUnitOfWorkFactory>(),
                sp.GetRequiredService<IAvroSerializer>(),
                sp.GetRequiredService<ITableViewMessageApplier<SportMessage>>(),
                sp.GetRequiredService<StoreMetadata>(),
                sp.GetRequiredService<ILogger<PulsarTableView<SportMessage>>>()));
        services.AddSingleton<IPulsarTableView<RawCategoryMessage>>(sp =>
            new PulsarTableView<RawCategoryMessage>(
                BuildTopic(sp.GetRequiredService<ProjectorOptions>().InputNamespace, PulsarTopics.Categories),
                sp.GetRequiredService<ITopicShardReaderStrategy>(),
                sp.GetRequiredService<IUnitOfWorkFactory>(),
                sp.GetRequiredService<IAvroSerializer>(),
                sp.GetRequiredService<ITableViewMessageApplier<RawCategoryMessage>>(),
                sp.GetRequiredService<StoreMetadata>(),
                sp.GetRequiredService<ILogger<PulsarTableView<RawCategoryMessage>>>()));

        services.AddSingleton<IGeoTaxonomyViewStorage, InMemoryGeoTaxonomyViewStorage>();
        services.AddSingleton<GeoTaxonomyProcessor>();

        return services;
    }

    public static IServiceCollection AddProcessorLogging(this IServiceCollection services)
    {
        services.AddLogging(builder =>
        {
            builder.AddSimpleConsole(options =>
                {
                    options.SingleLine = true;
                    options.TimestampFormat = "HH:mm:ss ";
                })
                .SetMinimumLevel(LogLevel.Information);
        });

        return services;
    }

    private static AvroSerializer BuildAvroSerializer()
    {
        AvroSchemaRegistry avroSchemaRegistry = new();
        avroSchemaRegistry.Register<SportMessage>(BuildSchemaPath("SportMessage.avsc"));
        avroSchemaRegistry.Register<RawCategoryMessage>(BuildSchemaPath("RawCategoryMessage.avsc"));
        avroSchemaRegistry.Register<GeoTaxonomyViewMessage>(BuildSchemaPath("GeoTaxonomyViewMessage.avsc"));
        avroSchemaRegistry.Register<SportRejectedMessage>(BuildSchemaPath("SportRejectedMessage.avsc"));
        avroSchemaRegistry.Register<RawCategoryRejectedMessage>(BuildSchemaPath("RawCategoryRejectedMessage.avsc"));
        return avroSchemaRegistry.Build();
    }

    private static string BuildTopic(string @namespace, string topicName)
        => $"persistent://{@namespace}/{topicName}";

    private static string BuildSchemaPath(string fileName)
        => Path.Combine(AppContext.BaseDirectory, "AvroSchemas", fileName);
}
