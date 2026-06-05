using CommandLine;

namespace PoC.Pulsar.TableView.Cli.Commands;

[Verb("tsavorite", HelpText = "Inspect the live Tsavorite store through the processor.")]
public sealed class TsavoriteVerb
{
    [Value(0, Required = true, HelpText = "Operation to execute: list or get.")]
    public string Operation { get; init; } = "";

    [Value(1, Required = true, HelpText = "Logical type to inspect, such as sports or categories.")]
    public string Type { get; init; } = "";

    [Option("key", Required = false, HelpText = "Logical entity id for get operations.")]
    public string? Key { get; init; }

    [Option("watch", Required = false, HelpText = "Repeat list operations at this interval, such as 20s or 1m.")]
    public string? Watch { get; init; }

    [Option("limit", Required = false, Default = 100, HelpText = "Maximum number of entries to return.")]
    public int Limit { get; init; }
}
