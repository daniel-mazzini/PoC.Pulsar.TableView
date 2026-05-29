namespace PoC.Pulsar.TableView.Domain.Filter;

public interface IValuePredicate<T>
{
    bool Match(T value);
}
