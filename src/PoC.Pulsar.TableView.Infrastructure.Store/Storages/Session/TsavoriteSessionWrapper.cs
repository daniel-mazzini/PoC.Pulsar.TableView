namespace PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;

using StateAllocator = SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>;
using StateSession = ClientSession<
    SpanByte,
    SpanByte,
    SpanByte,
    SpanByteAndMemory,
    Empty,
    SpanByteFunctions<Empty>,
    StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>,
    SpanByteAllocator<StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>>>;

public class TsavoriteSessionWrapper : ITsavoriteSessionProvider
{
    private readonly ITsavoriteEngine _engine;
    private StateSession? _lightSession;
    private readonly ConcurrentDictionary<Type, IDisposable> _rmwSessions = new();
    private bool _disposed;

    public Guid SessionId { get; }

    public ITsavoriteEngine Engine => _engine;

    public TsavoriteSessionWrapper(ITsavoriteEngine engine)
    {
        _engine = engine;
        SessionId = Guid.NewGuid();
    }
    public StateSession GetLightSession()
    {
        ThrowIfDisposed();
        return _lightSession ??= _engine.CreateLightSession();
    }
    public ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunctions, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, StateAllocator> GetSession<TInput, TOutput, TFunctions>(TFunctions customFunctions = null)
        where TFunctions : SessionFunctionsBase<SpanByte, SpanByte, TInput, TOutput, Empty>
    {
        ThrowIfDisposed();

        var functionType = typeof(TFunctions);
        var session = _rmwSessions.GetOrAdd(functionType, (_) => _engine.CreateRmwSession<TInput, TOutput, TFunctions>(customFunctions));
        return (ClientSession<SpanByte, SpanByte, TInput, TOutput, Empty, TFunctions, StoreFunctions<SpanByte, SpanByte, SpanByteComparer, SpanByteRecordDisposer>, StateAllocator>)session;
    }

    public void Dispose()
    {
        _lightSession?.Dispose();
        foreach (var session in _rmwSessions.Values) session.Dispose();
        _rmwSessions.Clear();
        _disposed = true;

        GC.SuppressFinalize(this);
        
    }
    private void ThrowIfDisposed()
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
    }
}