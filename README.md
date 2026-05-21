# Pulsar TableView PoC

## Run the Aspire environment

Start the local infrastructure with:

```powershell
dotnet run --project .\src\PoC.Pulsar.TableView.AppHost\PoC.Pulsar.TableView.AppHost.csproj
```

Aspire will start:

- Apache Pulsar standalone on `6650` and `8080`
- the initialization job that creates `public/tableview-inputs` and `public/tableview-outputs`
- Dekaf on `8090`
- the `cli` seeding executable after the Pulsar init job completes

## Verify Pulsar and the topics

After Aspire is up, run:

```powershell
curl.exe http://localhost:8080/admin/v2/persistent/public/tableview-outputs
```

That should return the three topics:

- `persistent://public/tableview-inputs/sports`
- `persistent://public/tableview-inputs/categories`
- `persistent://public/tableview-outputs/taxonomy-view`

You can also open Dekaf at:

```powershell
http://localhost:8090
```

## Seed Pulsar with sample data

Run the CLI directly:

```powershell
$env:PULSAR_SERVICE_URL = "pulsar://localhost:6650"
$env:PULSAR_INPUT_NAMESPACE = "public/tableview-inputs"
dotnet run --project .\src\PoC.Pulsar.TableView.Cli\PoC.Pulsar.TableView.Cli.csproj -- publish-sample
```

Use `--help` if you want to see the available command shape.

Or start Aspire and let the `cli` executable run after Pulsar is ready:

```powershell
dotnet run --project .\src\PoC.Pulsar.TableView.AppHost\PoC.Pulsar.TableView.AppHost.csproj
```

The CLI reads the JSON files from `samples\publish`, writes Avro payloads to the input topics, and republishes each sample three times with the same key and increasing `Version` values.
