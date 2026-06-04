using PoC.Pulsar.TableView.Contracts;

namespace PoC.Pulsar.TableView.Domain.MaterializeViews;

public readonly record struct GeoTaxonomyViewMutationResult(bool ViewExists, bool Changed, GeoTaxonomyViewMessage? View)
{
    public static GeoTaxonomyViewMutationResult Missing()
        => new(false, false, null);

    public static GeoTaxonomyViewMutationResult Unchanged(GeoTaxonomyViewMessage view)
        => new(true, false, view);

    public static GeoTaxonomyViewMutationResult ChangedView(GeoTaxonomyViewMessage view)
        => new(true, true, view);
}
