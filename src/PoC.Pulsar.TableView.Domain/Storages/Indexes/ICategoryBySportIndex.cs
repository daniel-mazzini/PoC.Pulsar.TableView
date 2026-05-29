using PoC.Pulsar.TableView.Domain.Entities.Categories;
using PoC.Pulsar.TableView.Domain.Entities.Sports;

namespace PoC.Pulsar.TableView.Domain.Storages.Indexes;

public interface ICategoryBySportIndex
{
    ValueTask AddCategorybySportAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken);
    ValueTask<IReadOnlySet<CategoryId>> GetCategoriesBySport(SportId sportId, CancellationToken cancellationToken);
    ValueTask RemoveCategorybySportAsync(SportId sportId, CategoryId categoryId, CancellationToken cancellationToken);
    ValueTask ClearCategoryWithSportIdAsync(SportId sportId, CancellationToken cancellationToken);

    ValueTask AddCategoryByParentAsync(CategoryId parentCategoryId, CategoryId categoryId, CancellationToken cancellationToken);
    ValueTask<IReadOnlySet<CategoryId>> GetCategoriesByParent(CategoryId parentCategoryId, CancellationToken cancellationToken);
    ValueTask RemoveCategorybyParentAsync(CategoryId parentCategoryId, CategoryId categoryId, CancellationToken cancellationToken);
    ValueTask ClearCategoryWithParentAsync(CategoryId parentCategoryId, CancellationToken cancellationToken);
}

