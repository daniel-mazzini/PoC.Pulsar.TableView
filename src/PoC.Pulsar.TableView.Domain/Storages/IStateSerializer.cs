namespace PoC.Pulsar.TableView.Domain.Storages;

/// <summary>
/// Use to serialize and deserialize values to and from Tsavorite
/// </summary>
public interface IStateSerializer
{
    byte[] Serialize<T>(T value);
    T? Deserialize<T>(ReadOnlySpan<byte> bytes);
}


