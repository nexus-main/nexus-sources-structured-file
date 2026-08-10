using System.Globalization;
using System.Runtime.InteropServices;
using Nexus.DataModel;
using Nexus.Extensibility;

namespace Nexus.Sources.Tests;

public class TimestampedStructuredFileDataSourceTester : StructuredFileDataSource<object?, object?>
{
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

    protected override async Task ReadAsync(ReadInfo<object?> info, ReadRequest[] readRequests, CancellationToken cancellationToken)
    {
        var samplePeriod = readRequests.First().CatalogItem.Representation.SamplePeriod;
        var lines = await File.ReadAllLinesAsync(info.FilePath, cancellationToken);

        foreach (var line in lines)
        {
            var parts = line.Split(';');

            var dateTime = DateTime.ParseExact(
                parts[0],
                "yyyy-MM-ddTHH:mm:ssZ",
                CultureInfo.InvariantCulture,
                DateTimeStyles.AdjustToUniversal
            );

            var value = long.Parse(parts[1], CultureInfo.InvariantCulture);
            var index = (int)((dateTime - info.BufferBegin).Ticks / samplePeriod.Ticks);

            if (index < 0 || index >= info.FileBlock)
                continue;

            foreach (var readRequest in readRequests)
            {
                MemoryMarshal.Cast<byte, long>(readRequest.Data.Span)[index] = value;
                readRequest.Status.Span[index] = 1;
            }
        }
    }
}
