using CommandLine;
using PoC.Pulsar.TableView.Cli.Publishing;

namespace PoC.Pulsar.TableView.Cli.Commands;

internal sealed class PublishSampleApplication
{
    private readonly ISamplePublisher _samplePublisher;

    public PublishSampleApplication(ISamplePublisher samplePublisher)
    {
        _samplePublisher = samplePublisher;
    }

    public int Run(string[] args)
    {
        if (args.Length > 0 && !args[0].StartsWith("-", StringComparison.Ordinal) && args[0] != "publish-sample")
        {
            return 1;
        }

        return Parser.Default.ParseArguments<PublishSampleVerb>(args)
            .MapResult(
                _ => RunPublishSample(),
                errors => errors.All(error => error.Tag is ErrorType.HelpRequestedError or ErrorType.VersionRequestedError) ? 0 : 1);
    }

    private int RunPublishSample()
    {
        _samplePublisher.PublishAsync().GetAwaiter().GetResult();
        return 0;
    }
}
