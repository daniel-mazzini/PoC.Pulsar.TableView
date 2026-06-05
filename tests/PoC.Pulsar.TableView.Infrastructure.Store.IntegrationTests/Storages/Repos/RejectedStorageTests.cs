using MemoryPack;
using PoC.Pulsar.TableView.Domain.Rejected;
using PoC.Pulsar.TableView.Domain.Storages.StateStore;
using PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Support;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;

namespace PoC.Pulsar.TableView.Infrastructure.Store.IntegrationTests.Storages.Repos;

public sealed class RejectedStorageTests
{
    private readonly ITestOutputHelper _output;

    public RejectedStorageTests(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public async Task rejected_storage_should_persist_rejected_projection()
    {
        using var context = new TsavoriteIntegrationContext(nameof(rejected_storage_should_persist_rejected_projection));
        var storage = context.CreateRejectedStorage();
        var projection = IntegrationTestData.RejectedProjection("message-1");

        await storage.SaveRejectedRecordAsync(projection, CancellationToken.None);
        var stored = context.ReadSingleByPrefix<PoC.Pulsar.TableView.Domain.Rejected.RejectedProjection>(StorageKey.RejectedRecord(projection.MessageKey).Value);

        Assert.NotNull(stored);
        Assert.Equal(projection.MessageKey, stored.MessageKey);
        Assert.Equal(projection.Reason.Code, stored.Reason.Code);
    }

    [Fact]
    public async Task rejected_storage_should_scan_10_000_records_within_time_threshold()
    {
        using var context = new TsavoriteIntegrationContext(nameof(rejected_storage_should_scan_10_000_records_within_time_threshold));
        var storage = context.CreateRejectedStorage();
        var scanTimeThreshold = TimeSpan.FromSeconds(1);

        for (var index = 0; index < 10_000; index++)
        {
            await storage.SaveRejectedRecordAsync(IntegrationTestData.RejectedProjection($"message-{index}"), CancellationToken.None);
        }

        var prefix = "__geo-projector:rejected:";
        var stopwatch = Stopwatch.StartNew();

        ConcurrentDictionary<string,RejectedProjection> scanned = [];

        context.Engine.ScanByPrefix(Encoding.UTF8.GetBytes(prefix),
                                                (key, value) => 
                                                {
                                                    var keyString = Encoding.UTF8.GetString(key);
                                                    if (value.Length <= 4) return;
                                                    ReadOnlySpan<byte> payload = value.Slice(4);
                                                    RejectedProjection projection = MemoryPackSerializer.Deserialize<RejectedProjection>(payload)!;
                                                    scanned.TryAdd(keyString, projection);
                                                });

        stopwatch.Stop();
        _output.WriteLine($"scan 10_000 rejected records elapsed: {stopwatch.Elapsed}");
        _output.WriteLine($"scan prefix: {prefix}");

        Assert.Equal(10_000, scanned.Count);
        Assert.True(stopwatch.Elapsed <= scanTimeThreshold, $"Expected rejected scan to finish within {scanTimeThreshold}, but took {stopwatch.Elapsed}.");
    }
}
