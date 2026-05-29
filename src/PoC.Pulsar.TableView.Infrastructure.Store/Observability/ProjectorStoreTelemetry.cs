using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace PoC.Pulsar.TableView.Infrastructure.Store.Observability;

public static class ProjectorStoreTelemetry
{
    public const string Name = "PoC.Pulsar.TableView.Store";
    private const string TagTopicKey = "topic";
    private const string TagPartitionIdKey = "partition_id";
    private const string TagOperationKey = "operation";
    private const string PhaseKey = "phase";
    public static readonly ActivitySource ActivitySource = new(Name);

    private static long _activeTopicReaders;
    public static readonly Meter Meter = new(Name);
    public static KeyValuePair<string, object?> StoreTag => new("store", "country-taxonomy-projector");

    static ProjectorStoreTelemetry()
    {
        Meter.CreateObservableGauge("projector.topic.reader.active.count",
                                    () => new Measurement<long>(Interlocked.Read(ref _activeTopicReaders), StoreTag));
    }

    // Gauge variables
    public static void IncrementActiveTopicReaders() => Interlocked.Increment(ref _activeTopicReaders);
    public static void DecrementActiveTopicReaders() => Interlocked.Decrement(ref _activeTopicReaders);

    // Counter
    public static readonly Counter<long> TopicReaderCancelled = Meter.CreateCounter<long>("projector.topic.reader.cancelled.total");
    public static readonly Counter<long> TopicReaderErrors = Meter.CreateCounter<long>("projector.topic.reader.errors.total");

    public static Activity? StartActivity(string name,
                                          string? topic = null,
                                          int? partitionId = null,
                                          string? operation = null,
                                          string? phase = null)
    {
        var activity = ActivitySource.StartActivity(name);
        if (activity == null)
        {
            return activity;
        }

        if (topic is not null)
        {
            activity.SetTag(TagTopicKey, topic);
        }

        if (partitionId.HasValue)
        {
            activity.SetTag(TagPartitionIdKey, partitionId.Value);
        }

        if (operation is not null)
        {
            activity.SetTag(TagOperationKey, operation);
        }

        if (phase is not null)
        {
            activity.SetTag(PhaseKey, phase);
        }

        return activity;
    }

}
