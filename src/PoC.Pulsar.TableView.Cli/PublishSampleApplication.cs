using CommandLine;

namespace PoC.Pulsar.TableView.Cli;

public static class PublishSampleApplication
{
    public static int Run(string[] args)
    {
        return Parser.Default.ParseArguments<PublishSampleVerb>(args)
            .MapResult(
                _ => RunPublishSample(),
                errors => errors.Any(error => error.Tag is ErrorType.HelpRequestedError or ErrorType.VersionRequestedError) ? 0 : 1);
    }

    private static int RunPublishSample()
    {
        new SamplePublisher().PublishAsync().GetAwaiter().GetResult();
        return 0;
    }
}
