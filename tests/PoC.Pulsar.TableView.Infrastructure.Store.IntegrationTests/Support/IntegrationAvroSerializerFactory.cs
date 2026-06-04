using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Domain.Serializers;
using PoC.Pulsar.TableView.Infrastructure.Store.Serialization;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;

internal static class IntegrationAvroSerializerFactory
{
    public static IAvroSerializer Create()
    {
        var registry = new AvroSchemaRegistry();
        registry.Register<SportMessage>(Path.Combine(IntegrationTestPaths.AvroSchemasRootPath, "SportMessage.avsc"));
        registry.Register<RawCategoryMessage>(Path.Combine(IntegrationTestPaths.AvroSchemasRootPath, "RawCategoryMessage.avsc"));
        registry.Register<GeoTaxonomyViewMessage>(Path.Combine(IntegrationTestPaths.AvroSchemasRootPath, "GeoTaxonomyViewMessage.avsc"));
        registry.Register<SportRejectedMessage>(Path.Combine(IntegrationTestPaths.AvroSchemasRootPath, "SportRejectedMessage.avsc"));
        registry.Register<RawCategoryRejectedMessage>(Path.Combine(IntegrationTestPaths.AvroSchemasRootPath, "RawCategoryRejectedMessage.avsc"));
        return registry.Build();
    }
}
