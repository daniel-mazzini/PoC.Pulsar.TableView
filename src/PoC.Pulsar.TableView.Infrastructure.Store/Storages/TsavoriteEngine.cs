using PoC.Pulsar.TableView.Domain.Storages;
using System.Buffers;
using System.IO;
using System.Linq;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages;

using StateAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;

using StateStore = TsavoriteKV<SpanByte, SpanByte, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>;

public sealed class TsavoriteEngine : ITsavoriteEngine
{
    private readonly StateStore _store;
    private readonly string _storePath;
    private readonly SemaphoreSlim _checkpointLock = new(1, 1);
    private int _deferredCheckpointDepth;
    private bool _checkpointPending;
    private bool _disposed;

    public TsavoriteEngine(string storePath)
        : this(storePath, new MemoryPackWrapper())
    {
    }

    internal TsavoriteEngine(string storePath, IStateSerializer serializer)
    {
        ArgumentNullException.ThrowIfNull(serializer);

        _storePath = Path.GetFullPath(storePath);
        Directory.CreateDirectory(_storePath);

        var settings = new KVSettings<SpanByte, SpanByte>(_storePath, deleteDirOnDispose: false)
        {
            TryRecoverLatest = true
        };

        var functions = StoreFunctions<SpanByte, SpanByte>.Create();
        _store = new StateStore(
            settings,
            functions,
            static (allocatorSettings, storeFunctions) => new StateAllocator(allocatorSettings, storeFunctions));

        try
        {
            _store.Recover(0, false, -1);
        }
        catch (TsavoriteNoHybridLogException)
        {
            // First startup has no checkpoint yet.
        }
    }

    public ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, StateAllocator> CreateLightSession()
    {
        ThrowIfDisposed();
        return _store.NewSession<SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>>(
            new SpanByteFunctions<Empty>(MemoryPool<byte>.Shared),
            ReadCopyOptions.None);
    }

    public ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunctions, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, StateAllocator> CreateRmwSession<TInput, TOutput, TFunctions>(TFunctions customFunctions)
        where TFunctions : SessionFunctionsBase<SpanByte, SpanByte, TInput, TOutput, Empty>
    {
        ThrowIfDisposed();
        ArgumentNullException.ThrowIfNull(customFunctions);
        return _store.NewSession<TInput, TOutput, Empty, TFunctions>(customFunctions, ReadCopyOptions.None);
    }

    public IDisposable DeferDurableCheckpoints()
    {
        ThrowIfDisposed();
        Interlocked.Increment(ref _deferredCheckpointDepth);
        return new DeferredCheckpointScope(this);
    }

    public async Task CompleteWriteAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (Volatile.Read(ref _deferredCheckpointDepth) > 0)
        {
            _checkpointPending = true;
            return;
        }

        await TakeDurableCheckpointAsync(cancellationToken);
    }
    public async Task<Guid> CheckpointAsync(CancellationToken ct)
    {
        // En Tsavorite, un Full Checkpoint guarda el Índice y el Log
        var (status, token) = await _store.TakeFullCheckpointAsync(CheckpointType.Snapshot, cancellationToken: ct);

        if (!status)
        {
            throw new Exception($"Fallo al realizar el checkpoint de Tsavorite. Status: {status}");
        }

        return token;
    }
    private async Task TakeDurableCheckpointAsync(CancellationToken cancellationToken)
    {
        await _checkpointLock.WaitAsync(cancellationToken);
        try
        {
            const int MaxAttempts = 3;

            for (var attempt = 1; attempt <= MaxAttempts; attempt++)
            {
                var (success, _) = await _store.TakeFullCheckpointAsync(CheckpointType.Snapshot, cancellationToken);
                if (success)
                {
                    return;
                }

                await _store.CompleteCheckpointAsync(cancellationToken);
                await Task.Yield();
            }

            throw new InvalidOperationException("Tsavorite projector state store checkpoint was not completed.");
        }
        finally
        {
            _checkpointLock.Release();
        }
    }

    public async Task FlushAsync(CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        if (!_checkpointPending)
        {
            return;
        }

        await TakeDurableCheckpointAsync(cancellationToken);
        _checkpointPending = false;
    }

    public long GetStoreSizeBytes()
    {
        ThrowIfDisposed();
        if (!Directory.Exists(_storePath))
        {
            return 0;
        }

        return Directory
            .EnumerateFiles(_storePath, "*", SearchOption.AllDirectories)
            .Sum(file => new FileInfo(file).Length);
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        _checkpointLock.Dispose();
        _store.Dispose();
        _disposed = true;
    }

    

    private void EndDeferredCheckpoints()
    {
        var depth = Interlocked.Decrement(ref _deferredCheckpointDepth);
        if (depth < 0)
        {
            Interlocked.Exchange(ref _deferredCheckpointDepth, 0);
            throw new InvalidOperationException("Deferred checkpoint scope was disposed too many times.");
        }
    }

    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }

    public ClientSession<SpanByte, SpanByte, SpanByte, SpanByteAndMemory, Empty, SpanByteFunctions<Empty>, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, StateAllocator> CreateBasicSession()
    {
        throw new NotImplementedException();
    }

    private sealed class DeferredCheckpointScope : IDisposable
    {
        private TsavoriteEngine? _owner;

        public DeferredCheckpointScope(TsavoriteEngine owner)
        {
            _owner = owner;
        }

        public void Dispose()
        {
            var owner = Interlocked.Exchange(ref _owner, null);
            owner?.EndDeferredCheckpoints();
        }
    }
}