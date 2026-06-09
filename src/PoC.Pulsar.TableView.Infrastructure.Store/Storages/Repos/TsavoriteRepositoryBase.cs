using PoC.Pulsar.TableView.Infrastructure.Store.Observability;
using System.Buffers;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;

using StateAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
public abstract class TsavoriteRepositoryBase
{
    private readonly IStateSerializer _serializer;

    protected IStateSerializer Serializer => _serializer;

    protected TsavoriteRepositoryBase(IStateSerializer serializer)
    {
        _serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
    }

    protected ValueTask<T?> ReadFromSessionAsync<T, TInput, TOutput, TFunctions>(ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunctions, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, StateAllocator> session,
                                                                                         StorageKey key,
                                                                                         CancellationToken cancellationToken)
        where TFunctions : ISessionFunctions<SpanByte, SpanByte, TInput, TOutput, Empty>
    {
        using var activity = ProjectorStoreTelemetry.StartActivity("Tsavorite Get", operation: "BasicContext.Read");
        int byteCount = key.GetUtf8ByteCount();
        byte[] rentedArray = ArrayPool<byte>.Shared.Rent(byteCount);

        try
        {
            // Write the bytes of the conversion directly into the rented array. Use Sapn for performance and to avoid unnecessary allocations.
            key.WriteUtf8Bytes(rentedArray.AsSpan());
            // The ArrayPool almost never gives you an array of the exact size you requested. Use AsMemory to pin the memory in the next step.
            var keyMemory = rentedArray.AsMemory(0, byteCount);
            // We pin the memory to prevent the garbage collector from moving it while Tsavorite accesses it via a physical pointer.
            // The 'using' clause ensures that the memory is always unpinned, preventing leaks or fragmentation.
            using var pinnedKey = keyMemory.Pin();
            var storeKey = SpanByte.FromPinnedMemory(keyMemory);

            var input = default(TInput)!;
            var output = default(TOutput)!;

            var status = session.BasicContext.Read(ref storeKey, ref input, ref output, Empty.Default);
            if (status.IsPending)
            {
                // if the data is not in memory, we need to wait for it to be loaded from disk. 
                session.BasicContext.CompletePending(wait: true, spinWaitForCommit: false);
                status = session.BasicContext.Read(ref storeKey, ref input, ref output, Empty.Default);
            }

            if (status.NotFound)
            {
                activity?.SetTag("result", "missing");
                return ValueTask.FromResult<T?>(default);
            }

            if (!status.Found)
            {
                throw new InvalidOperationException($"Tsavorite read for '{key}' failed with status '{status}'.");
            }

            var value = Serializer.Deserialize<T>(ExtractReadOnlySpan(output));
            activity?.SetTag("result", "success");
            return ValueTask.FromResult<T?>(value);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rentedArray);
        }
    }
    
    protected ValueTask UpsertIntoSessionAsync<T, TInput, TOutput, TFunctions>(
        ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunctions, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, StateAllocator> session,
        StorageKey key,
        TInput input,
        T value,
        CancellationToken cancellationToken)
        where TFunctions : ISessionFunctions<SpanByte, SpanByte, TInput, TOutput, Empty>
    {
        using var activity = ProjectorStoreTelemetry.StartActivity("Tsavorite Upsert", operation: "Upsert");
        UpsertSerializedIntoSession(session, key, input, Serializer.Serialize(value));
        activity?.SetTag("result", "success");
        return ValueTask.CompletedTask;
    }

    protected virtual ValueTask DeleteFromSessionAsync<TInput, TOutput, TFunctions>(ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunctions, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, StateAllocator> session,
                                                                                    StorageKey storageKey,
                                                                                    CancellationToken cancellationToken)
    where TFunctions : ISessionFunctions<SpanByte, SpanByte, TInput, TOutput, Empty>
    {
        using var activity = ProjectorStoreTelemetry.StartActivity("Tsavorite Delete", operation: "BasicContext.Delete");
        int keyByteCount = storageKey.GetUtf8ByteCount();
        byte[] rentedKeyArray = ArrayPool<byte>.Shared.Rent(keyByteCount);

        try
        {
            storageKey.WriteUtf8Bytes(rentedKeyArray.AsSpan());
            var keyMemory = rentedKeyArray.AsMemory(0, keyByteCount);
            using var pinnedKey = keyMemory.Pin();
            var storeKey = SpanByte.FromPinnedMemory(keyMemory);

            var status = session.BasicContext.Delete(ref storeKey, Empty.Default);

            if (!status.IsCompletedSuccessfully)
            {
                throw new InvalidOperationException($"Tsavorite delete for '{storageKey.Value}' failed with status '{status}'.");
            }

            activity?.SetTag("result", "success");
            return ValueTask.CompletedTask;
        }
        finally
        {
            // 6. Devolvemos el arreglo al pool pase lo que pase
            ArrayPool<byte>.Shared.Return(rentedKeyArray);
        }
    }

    /// <summary>
    /// This method is designed for high performance and low latency in scenarios with frequent upsert operations, such as updating the state of entities in a real-time projection. 
    /// This is achieved by:
    ///  - Renting byte arrays from the ArrayPool to use as buffers for the UTF-8 encoded key and the serialized value, instead of allocating new arrays on the heap.
    ///  - Writing the UTF-8 bytes of the key directly into the rented key array using Span APIs, which is more efficient than encoding to a string and then to bytes.
    ///  - Copying the serialized value directly into the rented value array, again using Span for efficiency.
    ///  - Pinning the rented arrays in memory to get stable pointers that can be passed to Tsavorite without risking that the GC moves them, which allows Tsavorite to read/write directly from/to these buffers without extra copying.
    ///  - Using Tsavorite's pointer-based APIs to perform the upsert operation directly on the pinned memory, which minimizes overhead and latency.
    /// 
    /// </summary>
    /// <typeparam name="TInput"></typeparam>
    /// <typeparam name="TOutput"></typeparam>
    /// <typeparam name="TFunctions"></typeparam>
    /// <param name="session"></param>
    /// <param name="storageKey"></param>
    /// <param name="input"></param>
    /// <param name="serializedValueSpan"></param>
    /// <exception cref="InvalidOperationException"></exception>
    private static void UpsertSerializedIntoSession<TInput, TOutput, TFunctions>(ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunctions, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, StateAllocator> session,
                                                                                   StorageKey storageKey,
                                                                                   TInput input,
                                                                                   ReadOnlySpan<byte> serializedValueSpan)
    where TFunctions : ISessionFunctions<SpanByte, SpanByte, TInput, TOutput, Empty>
    {
        int keyByteCount = storageKey.GetUtf8ByteCount();
        int valueByteCount = serializedValueSpan.Length;

        byte[] rentedKeyArray = ArrayPool<byte>.Shared.Rent(keyByteCount);
        byte[] rentedValueArray = ArrayPool<byte>.Shared.Rent(valueByteCount);

        try
        {
            storageKey.WriteUtf8Bytes(rentedKeyArray.AsSpan());
            serializedValueSpan.CopyTo(rentedValueArray.AsSpan());

            var keyMemory = rentedKeyArray.AsMemory(0, keyByteCount);
            var valueMemory = rentedValueArray.AsMemory(0, valueByteCount);

            using var pinnedKey = keyMemory.Pin();
            using var pinnedValue = valueMemory.Pin();

            var storeKey = SpanByte.FromPinnedMemory(keyMemory);
            var storeValue = SpanByte.FromPinnedMemory(valueMemory);
            var output = default(TOutput)!;

            // Use pointer-based APIs for maximum performance. Tsavorite will read the key and value directly from the pinned memory without any additional copying.
            var status = session.BasicContext.Upsert(ref storeKey, ref input, ref storeValue, ref output, Empty.Default);

            if (!status.IsCompletedSuccessfully)
            {
                throw new InvalidOperationException($"Tsavorite upsert for '{storageKey.Value}' failed with status '{status}'.");
            }
        }
        finally
        {
            // return the rented arrays to the pool to avoid memory leaks
            ArrayPool<byte>.Shared.Return(rentedKeyArray);
            ArrayPool<byte>.Shared.Return(rentedValueArray);
        }
    }

    private static ReadOnlySpan<byte> ExtractReadOnlySpan<TOutput>(TOutput output)
        => output switch
        {
            SpanByteAndMemory spanByteAndMemory => spanByteAndMemory.AsReadOnlySpan(),
            _ => throw new InvalidOperationException($"Unsupported Tsavorite output type '{typeof(TOutput).Name}'.")
        };
}