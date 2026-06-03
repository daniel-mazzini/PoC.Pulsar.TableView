using PoC.Pulsar.TableView.Domain.Sports;

namespace PoC.Pulsar.TableView.Domain.Categories;

public interface ICategoryPendingIndex
{
    ValueTask<bool> TryMarkCategoryWaitingForSportAsync(SportId sportId,
                                                        CategoryId categoryId,
                                                        Func<SportId, CancellationToken, ValueTask<bool>> sportExistsCheck,
                                                        CancellationToken cancellationToken);

    ValueTask ResolveCategoryWaitingForSportAsync(SportId sportId,
                                                  CategoryId categoryId,
                                                  CancellationToken cancellationToken);

    ValueTask<IReadOnlySet<CategoryId>> GetCategoriesWaitingForSportAsync(SportId sportId, CancellationToken cancellationToken);

    ValueTask<IReadOnlySet<SportId>> GetMissingSportsForCategoryAsync(CategoryId categoryId, CancellationToken cancellationToken);

    ValueTask RemoveCategoryFromPendingAsync(CategoryId categoryId, CancellationToken cancellationToken);

    ValueTask ClearAsync(CancellationToken cancellationToken);
}

