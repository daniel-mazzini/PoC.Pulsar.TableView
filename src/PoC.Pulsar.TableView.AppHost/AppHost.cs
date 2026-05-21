using Aspire.Hosting;

var builder = DistributedApplication.CreateBuilder(args);

var appHostDirectory = new DirectoryInfo(AppContext.BaseDirectory);
var configuration = appHostDirectory.Parent?.Parent?.Name ?? "Debug";
var repositoryRoot = Path.GetFullPath(Path.Combine(AppContext.BaseDirectory, "..", "..", "..", "..", ".."));
var cliDllPath = Path.GetFullPath(Path.Combine(repositoryRoot, "src", "PoC.Pulsar.TableView.Cli", "bin", configuration, "net10.0", "PoC.Pulsar.TableView.Cli.dll"));

var pulsar = builder.AddContainer("pulsar", "apachepulsar/pulsar", "3.2.1")
    .WithEndpoint(port: 6650, targetPort: 6650, name: "broker")
    .WithHttpEndpoint(port: 8080, targetPort: 8080, name: "admin")
    .WithEntrypoint("/bin/bash")
    .WithArgs("-c", "bin/pulsar standalone");

var pulsarInitScript = """
set -euo pipefail

until bin/pulsar-admin --admin-url http://pulsar:8080 brokers healthcheck >/dev/null 2>&1; do
  sleep 2
done

bin/pulsar-admin --admin-url http://pulsar:8080 namespaces create -c standalone public/tableview-inputs || true
bin/pulsar-admin --admin-url http://pulsar:8080 namespaces create -c standalone public/tableview-outputs || true
bin/pulsar-admin --admin-url http://pulsar:8080 namespaces set-compaction-threshold --threshold 1M public/tableview-inputs
bin/pulsar-admin --admin-url http://pulsar:8080 namespaces set-compaction-threshold --threshold 1M public/tableview-outputs
bin/pulsar-admin --admin-url http://pulsar:8080 topics create persistent://public/tableview-inputs/sports || true
bin/pulsar-admin --admin-url http://pulsar:8080 topics create persistent://public/tableview-inputs/categories || true
bin/pulsar-admin --admin-url http://pulsar:8080 topics create persistent://public/tableview-outputs/taxonomy-view || true
""";

var pulsarInit = builder.AddContainer("pulsar-init", "apachepulsar/pulsar", "3.2.1")
    .WithEntrypoint("/bin/bash")
    .WithArgs("-lc", pulsarInitScript)
    .WaitFor(pulsar);

builder.AddContainer("dekaf", "visortelle/dekaf", "1.1.0")
    .WithHttpEndpoint(port: 8090, targetPort: 8090, name: "http")
    .WithEnvironment("DEKAF_PULSAR_WEB_URL", "http://pulsar:8080")
    .WithEnvironment("DEKAF_PULSAR_BROKER_URL", "pulsar://pulsar:6650")
    .WaitFor(pulsarInit);

builder.AddExecutable(
        "cli-publish-sample",
        "dotnet",
        repositoryRoot,
        "run",
        "--project",
        "src/PoC.Pulsar.TableView.Cli/PoC.Pulsar.TableView.Cli.csproj",
        "--",
        "publish-sample")
    .WithEnvironment("PULSAR_SERVICE_URL", "pulsar://127.0.0.1:6650")
    .WithEnvironment("PULSAR_INPUT_NAMESPACE", "public/tableview-inputs")
    .WaitForCompletion(pulsarInit)
    .WithExplicitStart();

builder.Build().Run();
