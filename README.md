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
