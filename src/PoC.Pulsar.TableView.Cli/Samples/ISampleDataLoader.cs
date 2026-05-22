using PoC.Pulsar.TableView.Contracts;

namespace PoC.Pulsar.TableView.Cli.Samples;

internal interface ISampleDataLoader
{
    IReadOnlyList<SportMessage> LoadSports();

    IReadOnlyList<RawCategoryMessage> LoadCategories();
}
