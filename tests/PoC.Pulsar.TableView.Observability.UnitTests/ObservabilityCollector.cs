using System.Diagnostics;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using PoC.Pulsar.TableView.Infrastructure.Store.Observability;

namespace PoC.Pulsar.TableView.Observability.UnitTests;

internal sealed class ObservabilityCollector : IDisposable
{
    private readonly List<Activity> _activities = [];
    private readonly List<Metric> _metrics = [];
    private readonly MeterProvider _meterProvider;
    private readonly TracerProvider _tracerProvider;

    public ObservabilityCollector()
    {
        _tracerProvider = Sdk.CreateTracerProviderBuilder()
                             .AddSource(ProjectorStoreTelemetry.Name)
                             .AddInMemoryExporter(_activities)
                             .Build();

        _meterProvider = Sdk.CreateMeterProviderBuilder()
                            .AddMeter(ProjectorStoreTelemetry.Name)
                            .AddInMemoryExporter(_metrics)
                            .Build();
    }

    public bool HasLongSum(string name, long value, params KeyValuePair<string, object?>[] expectedTags)
    {
        Flush();

        foreach (var metric in _metrics.Where(metric => metric.Name == name))
        {
            foreach (ref readonly var point in metric.GetMetricPoints())
            {
                if (point.GetSumLong() == value && HasTags(point, expectedTags))
                {
                    return true;
                }
            }
        }

        return false;
    }

    public bool HasHistogramPoint(string name, params KeyValuePair<string, object?>[] expectedTags)
    {
        Flush();

        foreach (var metric in _metrics.Where(metric => metric.Name == name))
        {
            foreach (ref readonly var point in metric.GetMetricPoints())
            {
                if (HasTags(point, expectedTags))
                {
                    return true;
                }
            }
        }

        return false;
    }

    public bool HasActivity(string name, params KeyValuePair<string, object?>[] expectedTags)
    {
        Flush();

        return _activities.Any(activity => activity.DisplayName == name && expectedTags.All(tag => HasActivityTag(activity, tag)));
    }

    public void Dispose()
    {
        _meterProvider.Dispose();
        _tracerProvider.Dispose();
    }

    private void Flush()
    {
        _tracerProvider.ForceFlush();
        _meterProvider.ForceFlush();
    }

    private static bool HasTags(MetricPoint point, params KeyValuePair<string, object?>[] expectedTags)
    {
        Dictionary<string, object?> tags = new(StringComparer.Ordinal);
        foreach (var tag in point.Tags)
        {
            tags[tag.Key] = tag.Value;
        }

        return expectedTags.All(expected => tags.TryGetValue(expected.Key, out var actual) && Equals(actual?.ToString(), expected.Value?.ToString()));
    }

    private static bool HasActivityTag(Activity activity, KeyValuePair<string, object?> expected)
        => activity.Tags.Any(tag => tag.Key == expected.Key && tag.Value == expected.Value?.ToString());
}
