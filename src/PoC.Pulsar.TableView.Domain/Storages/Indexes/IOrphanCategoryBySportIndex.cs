using PoC.Pulsar.TableView.Domain.Entities.Categories;
using PoC.Pulsar.TableView.Domain.Entities.Sports;

namespace PoC.Pulsar.TableView.Domain.Storages.Indexes;

public interface IOrphanCategoryBySportIndex
{
    ValueTask AddOrphanCategorybySportAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken);
    ValueTask ClearOrphanCategoryWithSportIdAsync(SportId sportId, CancellationToken cancellationToken);
    ValueTask<IReadOnlySet<CategoryId>> GetOrphanCategoriesBySport(SportId sportId, CancellationToken cancellationToken);
    ValueTask RemoveOrphanCategorybySportAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken);

    ValueTask AddOrphanCategoryByParentAsync(CategoryId parentCategoryId, CategoryId categoryId, CancellationToken cancellationToken);
    ValueTask<IReadOnlySet<CategoryId>> GetOrphanCategoriesByParent(CategoryId parentCategoryId, CancellationToken cancellationToken);
    ValueTask RemoveOrphanCategorybyParentAsync(CategoryId parentCategoryId, CategoryId categoryId, CancellationToken cancellationToken);
    ValueTask ClearOrphanCategoryWithParentAsync(CategoryId parentCategoryId, CancellationToken cancellationToken);
}
