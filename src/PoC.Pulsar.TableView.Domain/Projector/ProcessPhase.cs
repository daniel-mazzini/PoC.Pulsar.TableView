namespace PoC.Pulsar.TableView.Domain.Projector;

public abstract record ProcessPhase(string Name)
{
    public static ProcessPhase Bootstrap = new BootstrapPhase();
    public static ProcessPhase Live = new LivePhase();

    public override string ToString()
    {
        return Name;
    }
}

public sealed record BootstrapPhase() : ProcessPhase("bootstrap");
public sealed record LivePhase() : ProcessPhase("live");
