using PoC.Pulsar.TableView.Contracts;

namespace PoC.Pulsar.TableView.Domain.Filter;

public class GeoCategoryMessageFilter : IValuePredicate<RawCategoryMessage>
{
    public GeoCategoryMessageFilter()
    {
    }

    public bool Match(RawCategoryMessage value)
    {
        return !string.IsNullOrWhiteSpace(value.CountryCode);
    }
}
