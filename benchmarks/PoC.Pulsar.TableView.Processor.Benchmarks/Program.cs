using BenchmarkDotNet.Running;

namespace PoC.Pulsar.TableView.Processor.Benchmarks;

public static class Program
{
    public static void Main(string[] args)
    {
        BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args);
    }
}
