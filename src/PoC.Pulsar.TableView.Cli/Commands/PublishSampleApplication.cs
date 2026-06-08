using CommandLine;
using PoC.Pulsar.TableView.Cli.CompactTopic;
using PoC.Pulsar.TableView.Cli.Publishing;
using PoC.Pulsar.TableView.Cli.Tsavorite;

namespace PoC.Pulsar.TableView.Cli.Commands;

internal sealed class PublishSampleApplication
{
    private readonly ISamplePublisher _samplePublisher;
    private readonly ITsavoriteCommandRunner? _tsavoriteCommandRunner;
    private readonly ICompactTopicCommandRunner? _compactTopicCommandRunner;

    public PublishSampleApplication(ISamplePublisher samplePublisher)
        : this(samplePublisher, tsavoriteCommandRunner: null, compactTopicCommandRunner: null)
    {
    }

    public PublishSampleApplication(ISamplePublisher samplePublisher, ITsavoriteCommandRunner? tsavoriteCommandRunner, ICompactTopicCommandRunner? compactTopicCommandRunner)
    {
        _samplePublisher = samplePublisher;
        _tsavoriteCommandRunner = tsavoriteCommandRunner;
        _compactTopicCommandRunner = compactTopicCommandRunner;
    }

    public int Run(string[] args)
    {
        if (args.Length > 0 && !args[0].StartsWith("-", StringComparison.Ordinal) && args[0] is not ("publish-sample" or "tsavorite" or "compact-topic"))
        {
            return 1;
        }

        try
        {
            return Parser.Default.ParseArguments(args, typeof(PublishSampleVerb), typeof(TsavoriteVerb), typeof(CompactTopicVerb))
                .MapResult(
                    (PublishSampleVerb _) => RunPublishSample(),
                    (TsavoriteVerb verb) => RunTsavorite(verb),
                    (CompactTopicVerb verb) => RunCompactTopic(verb),
                    errors => errors.All(error => error.Tag is ErrorType.HelpRequestedError or ErrorType.HelpVerbRequestedError or ErrorType.VersionRequestedError) ? 0 : 1);
        }
        catch (OperationCanceledException)
        {
            return 0;
        }
        catch (Exception exception)
        {
            Console.Error.WriteLine(exception.Message);
            return 1;
        }
    }

    private int RunPublishSample()
    {
        _samplePublisher.PublishAsync().GetAwaiter().GetResult();
        return 0;
    }

    private int RunTsavorite(TsavoriteVerb verb)
    {
        if (_tsavoriteCommandRunner is null)
        {
            return 1;
        }

        using var cancellationTokenSource = new CancellationTokenSource();
        Console.CancelKeyPress += (_, args) =>
        {
            args.Cancel = true;
            cancellationTokenSource.Cancel();
        };

        return _tsavoriteCommandRunner.RunAsync(verb, cancellationTokenSource.Token).GetAwaiter().GetResult();
    }

    private int RunCompactTopic(CompactTopicVerb verb)
    {
        if (_compactTopicCommandRunner is null)
        {
            return 1;
        }

        using var cancellationTokenSource = new CancellationTokenSource();
        Console.CancelKeyPress += (_, args) =>
        {
            args.Cancel = true;
            cancellationTokenSource.Cancel();
        };

        return _compactTopicCommandRunner.RunAsync(verb, cancellationTokenSource.Token).GetAwaiter().GetResult();
    }
}
