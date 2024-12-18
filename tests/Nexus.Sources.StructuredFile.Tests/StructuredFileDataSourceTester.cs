﻿using Nexus.DataModel;
using Nexus.Extensibility;
using System.Text.Json;

namespace Nexus.Sources.Tests;

public class StructuredFileDataSourceTester : StructuredFileDataSource
{
    private readonly Action<ReadInfo>? _onNewReadInfo;
    
    public StructuredFileDataSourceTester(
        Action<ReadInfo>? onNewReadInfo = default
    )
    {
        _onNewReadInfo = onNewReadInfo;
    }

    public Dictionary<string, Dictionary<string, IReadOnlyList<FileSource>>> Config { get; private set; } = default!;

    public new Task<(DateTime RegularUtcFileBegin, (string FilePath, TimeSpan FileBeginOffset)[])> 
        FindFileBeginAndPathsAsync(DateTime begin, FileSource fileSource)
    {
        return base.FindFileBeginAndPathsAsync(begin, fileSource);
    }

    protected override async Task InitializeAsync(CancellationToken cancellationToken)
    {
        var configFilePath = Path.Combine(Root, "config.json");

        if (!File.Exists(configFilePath))
            throw new Exception($"The configuration file does not exist on path {configFilePath}.");

        var jsonString = await File.ReadAllTextAsync(configFilePath, cancellationToken);
        Config = JsonSerializer.Deserialize<Dictionary<string, Dictionary<string, IReadOnlyList<FileSource>>>>(jsonString)!;
    }

    protected override Task<Func<string, Dictionary<string, IReadOnlyList<FileSource>>>> GetFileSourceProviderAsync(CancellationToken cancellationToken)
    {
        return Task.FromResult<Func<string, Dictionary<string, IReadOnlyList<FileSource>>>>(catalogId => Config[catalogId]);
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

        var fileSourceId = Config.First().Value.First().Key;

        var resource = new ResourceBuilder(id: "Resource1")
            .WithFileSourceId(fileSourceId)
            .AddRepresentation(representation)
            .Build();

        catalog = catalog.Merge(new ResourceCatalog(id: "/A/B/C", resources: new List<Resource>() { resource }));

        return Task.FromResult(catalog);
    }

    protected override async Task ReadAsync(ReadInfo readInfo, ReadRequest[] readRequests, CancellationToken cancellationToken)
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
