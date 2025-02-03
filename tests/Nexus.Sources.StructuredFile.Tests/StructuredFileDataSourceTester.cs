using Nexus.DataModel;
using Nexus.Extensibility;

namespace Nexus.Sources.Tests;

public class StructuredFileDataSourceTester : StructuredFileDataSource<object?, object?>
{
    private readonly Action<ReadInfo<object?>>? _onNewReadInfo;
    
    public StructuredFileDataSourceTester(
        Action<ReadInfo<object?>>? onNewReadInfo = default
    )
    {
        _onNewReadInfo = onNewReadInfo;
    }

    public new Task<(DateTime RegularUtcFileBegin, (string FilePath, TimeSpan FileBeginOffset)[])> 
        FindFileBeginAndPathsAsync(DateTime begin, FileSource<object?> fileSource)
    {
        return base.FindFileBeginAndPathsAsync(begin, fileSource);
    }

    protected override Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(string path, CancellationToken cancellationToken)
    {
        return Task.FromResult(new CatalogRegistration[] { new("/A/B/C", string.Empty) });
    }

    protected override Task<ResourceCatalog> EnrichCatalogAsync(ResourceCatalog catalog, CancellationToken cancellationToken)
    {
        var representation = new Representation(
            dataType: NexusDataType.INT64,
            samplePeriod: TimeSpan.FromSeconds(1)
        );

        var fileSourceId = Context.SourceConfiguration.FileSourceGroupsMap.First().Value.First().Key;

        var resource = new ResourceBuilder(id: "Resource1")
            .WithFileSourceId(fileSourceId)
            .AddRepresentation(representation)
            .Build();

        catalog = catalog.Merge(new ResourceCatalog(id: "/A/B/C", resources: new List<Resource>() { resource }));

        return Task.FromResult(catalog);
    }

    protected override async Task ReadAsync(ReadInfo<object?> readInfo, ReadRequest[] readRequests, CancellationToken cancellationToken)
    {
        _onNewReadInfo?.Invoke(readInfo);

        var bytes = await File
            .ReadAllBytesAsync(readInfo.FilePath, cancellationToken);

        foreach (var readRequest in readRequests)
        {
            bytes
                .CopyTo(readRequest.Data.Span);

            readRequest
                .Status
                .Span
                .Fill(1);
        }
    }
}
