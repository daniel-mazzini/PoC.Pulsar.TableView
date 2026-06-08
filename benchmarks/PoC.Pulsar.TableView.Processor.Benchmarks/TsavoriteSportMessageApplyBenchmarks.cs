using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Order;
using PoC.Pulsar.TableView.Contracts;
using PoC.Pulsar.TableView.Infrastructure.Store.Serialization;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Repos;
using PoC.Pulsar.TableView.Infrastructure.Store.Storages.Session;
using System.IO;

namespace PoC.Pulsar.TableView.Processor.Benchmarks;

[MemoryDiagnoser]
[Orderer(SummaryOrderPolicy.FastestToSlowest)]
public class TsavoriteSportMessageApplyBenchmarks
{
    private const int DefaultMessageCount = 10_000;

    private MemoryPackWrapper _serializer = null!;
    private SportMessage[] _seedMessages = null!;
    private SportMessage[] _incomingMessages = null!;
    private TsavoriteEngine _legacyEngine = null!;
    private TsavoriteEngine _rmwEngine = null!;
    private SportMessageStorage _legacyStorage = null!;
    private SportMessageStorage _rmwStorage = null!;
    private TsavoriteSessionWrapper _legacySession = null!;
    private TsavoriteSessionWrapper _rmwSession = null!;
    private string _iterationRootPath = null!;

    [Params(DefaultMessageCount)]
    public int MessageCount { get; set; }

    [GlobalSetup]
    public void global_setup()
    {
        _serializer = new MemoryPackWrapper();
        _seedMessages = create_messages(MessageCount, version: 1);
        _incomingMessages = create_messages(MessageCount, version: 2);
    }

    [IterationSetup]
    public void iteration_setup()
    {
        _iterationRootPath = Path.Combine(Path.GetTempPath(), "PoC.Pulsar.TableView.Benchmarks", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_iterationRootPath);

        var legacyPath = Path.Combine(_iterationRootPath, "legacy");
        var rmwPath = Path.Combine(_iterationRootPath, "rmw");
        Directory.CreateDirectory(legacyPath);
        Directory.CreateDirectory(rmwPath);

        _legacyEngine = new TsavoriteEngine(legacyPath);
        _rmwEngine = new TsavoriteEngine(rmwPath);
        _legacySession = new TsavoriteSessionWrapper(_legacyEngine);
        _rmwSession = new TsavoriteSessionWrapper(_rmwEngine);
        _legacyStorage = new SportMessageStorage(_legacySession, _serializer);
        _rmwStorage = new SportMessageStorage(_rmwSession, _serializer);

        foreach (var message in _seedMessages)
        {
            _legacyStorage.UpsertAsync(message, CancellationToken.None).GetAwaiter().GetResult();
            _rmwStorage.UpsertAsync(message, CancellationToken.None).GetAwaiter().GetResult();
        }
    }

    [IterationCleanup]
    public void iteration_cleanup()
    {
        _legacyStorage?.Dispose();
        _rmwStorage?.Dispose();
        _legacySession?.Dispose();
        _rmwSession?.Dispose();
        _legacyEngine?.Dispose();
        _rmwEngine?.Dispose();

        if (!string.IsNullOrWhiteSpace(_iterationRootPath) && Directory.Exists(_iterationRootPath))
        {
            Directory.Delete(_iterationRootPath, recursive: true);
        }
    }

    [Benchmark(Baseline = true)]
    public void legacy_try_load_plus_upsert_updates()
    {
        foreach (var message in _incomingMessages)
        {
            var current = _legacyStorage.TryLoadAsync(message.Id, CancellationToken.None).GetAwaiter().GetResult();
            if (current is null || message.Version > current.Version)
            {
                _legacyStorage.UpsertAsync(message, CancellationToken.None).GetAwaiter().GetResult();
            }
        }
    }

    [Benchmark]
    public void rmw_compact_try_apply_updates()
    {
        foreach (var message in _incomingMessages)
        {
            _rmwStorage.TryApplyAsync(message, CancellationToken.None).GetAwaiter().GetResult();
        }
    }

    private static SportMessage[] create_messages(int count, int version)
    {
        var messages = new SportMessage[count];

        for (var index = 0; index < count; index++)
        {
            messages[index] = new SportMessage
            {
                Id = $"sport-{index}",
                Name = $"sport-{index}",
                Provider = "provider",
                EntityCoverage = "coverage",
                SportType = "sport",
                Version = version
            };
        }

        return messages;
    }
}
