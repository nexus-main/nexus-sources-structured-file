using Microsoft.Extensions.Logging.Abstractions;
using Nexus.DataModel;
using Nexus.Extensibility;
using System.Globalization;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.Json;
using Xunit;

namespace Nexus.Sources.Tests;

public class StructuredFileDataSourceTests
{
    [Fact]
    public async Task CanProvideFirstFile()
    {
        var tester = new StructuredFileDataSourceTester();
        var dataSource = tester as IDataSource;

        var context = new DataSourceContext(
            ResourceLocator: new Uri(Path.Combine(Directory.GetCurrentDirectory(), "DATABASES/F")),
            SystemConfiguration: default!,
            SourceConfiguration: default!,
            RequestConfiguration: default!);

        await dataSource!.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        var methodInfo = dataSource
            .GetType()
            .GetMethod(
                "TryGetFirstFile",
                BindingFlags.NonPublic | BindingFlags.Instance) ?? throw new Exception("method info is null");
        var config = tester.Config.Values.First().Values.First()[0];

        var args = new object[] { config, default! };
        methodInfo.Invoke(dataSource, args);

        Assert.EndsWith("DATA/2019-12/20191231_12_x_0000.dat", ((string)args[1]).Replace('\\', '/'));
    }

    [Theory]
    [InlineData("DATABASES/A", "2019-12-31T12-00-00Z", "2020-01-02T00-20-00Z")]
    [InlineData("DATABASES/B", "2019-12-31T12-00-00Z", "2020-01-02T00-20-00Z")]
    [InlineData("DATABASES/C", "2019-12-31T12-00-00Z", "2020-01-02T00-20-00Z")]
    [InlineData("DATABASES/D", "2019-12-31T10-00-00Z", "2020-01-02T01-00-00Z")]
    [InlineData("DATABASES/E", "2019-12-31T12-00-00Z", "2020-01-03T00-00-00Z")]
    [InlineData("DATABASES/F", "2019-12-31T12-00-00Z", "2020-01-02T02-00-00Z")]
    [InlineData("DATABASES/G", "2019-12-31T00-40-22Z", "2020-01-01T01-39-23Z")]
    [InlineData("DATABASES/H", "2019-12-31T12-00-00Z", "2020-01-02T00-20-00Z")]
    [InlineData("DATABASES/I", "2019-12-31T23-55-00Z", "2020-01-01T00-15-00Z")]
    [InlineData("DATABASES/J", "2020-01-01T00-00-00Z", "2020-01-05T00-00-00Z")]
    [InlineData("DATABASES/K", "2020-01-01T00-00-00Z", "2020-01-02T00-00-00Z")]
    public async Task CanProvideTimeRange(string root, string expectedBeginString, string expectedEndString)
    {
        var expectedBegin = DateTime.ParseExact(expectedBeginString, "yyyy-MM-ddTHH-mm-ssZ", null, DateTimeStyles.AdjustToUniversal);
        var expectedEnd = DateTime.ParseExact(expectedEndString, "yyyy-MM-ddTHH-mm-ssZ", null, DateTimeStyles.AdjustToUniversal);

        var dataSource = new StructuredFileDataSourceTester() as IDataSource;

        var context = new DataSourceContext(
            ResourceLocator: new Uri(Path.Combine(Directory.GetCurrentDirectory(), root)),
            SystemConfiguration: default!,
            SourceConfiguration: default!,
            RequestConfiguration: default!);

        await dataSource!.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        var (begin, end) = await dataSource!.GetTimeRangeAsync("/A/B/C", CancellationToken.None);

        Assert.Equal(expectedBegin, begin);
        Assert.Equal(expectedEnd, end);
    }

    [Theory]
    [InlineData("DATABASES/A", "2020-01-02T00-00-00Z", "2020-01-03T00-00-00Z", 2 / 144.0, 4)]
    [InlineData("DATABASES/A", "2019-12-30T00-00-00Z", "2020-01-03T00-00-00Z", 3 / (4 * 144.0), 4)]
    [InlineData("DATABASES/B", "2020-01-02T00-00-00Z", "2020-01-03T00-00-00Z", 2 / 144.0, 4)]
    [InlineData("DATABASES/C", "2020-01-02T00-00-00Z", "2020-01-03T00-00-00Z", 2 / 144.0, 4)]
    [InlineData("DATABASES/D", "2020-01-01T22-10-00Z", "2020-01-02T22-10-00Z", (1 / 144.0 + 2 / 24.0) / 2, 4)]
    [InlineData("DATABASES/E", "2020-01-02T00-00-00Z", "2020-01-03T00-00-00Z", (1 + 2 / 48.0) / 2, 4)]
    [InlineData("DATABASES/F", "2020-01-02T00-00-00Z", "2020-01-03T00-00-00Z", 2 / 24.0, 4)]
    [InlineData("DATABASES/G", "2020-01-01T00-00-00Z", "2020-01-02T00-00-00Z", 2 / 86400.0, 6)]
    [InlineData("DATABASES/H", "2020-01-02T00-00-00Z", "2020-01-03T00-00-00Z", 2 / 144.0, 4)]
    [InlineData("DATABASES/I", "2019-12-31T00-00-00Z", "2020-01-02T00-00-00Z", 2 / (2 * 288.0), 4)]
    [InlineData("DATABASES/J", "2020-01-01T00-00-00Z", "2020-01-06T00-00-00Z", 3 / 5.0, 1)]
    public async Task CanProvideAvailability(string root, string beginString, string endString, double expected, int precision)
    {
        var begin = DateTime.ParseExact(beginString, "yyyy-MM-ddTHH-mm-ssZ", default, DateTimeStyles.AdjustToUniversal);
        var end = DateTime.ParseExact(endString, "yyyy-MM-ddTHH-mm-ssZ", default, DateTimeStyles.AdjustToUniversal);

        var dataSource = new StructuredFileDataSourceTester() as IDataSource;

        var context = new DataSourceContext(
            ResourceLocator: new Uri(Path.Combine(Directory.GetCurrentDirectory(), root)),
            SystemConfiguration: default!,
            SourceConfiguration: default!,
            RequestConfiguration: default!);

        await dataSource!.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        var actual = await dataSource!.GetAvailabilityAsync("/A/B/C", begin, end, CancellationToken.None);

        Assert.Equal(expected, actual, precision);
    }

    [Theory]
    [InlineData("A", "calibrated", "DATA/calibrated/2019-12/2019-12-31/2019-12-31_12-00-00.dat", "2019-12-31T12-00-00Z")]
    [InlineData("B", "calibrated", "DATA/2019-12/calibrated/2019-12-31/2019-12-31_12-00-00.dat", "2019-12-31T12-00-00Z")]
    [InlineData("C", "default", "DATA/2019-12-31/__0_2019-12-31_12-00-00_000000.dat", "2019-12-31T12-00-00Z")]
    [InlineData("D", "position_A", "DATA/position_A/__0_2019-12-31_12-00-00_000000.dat", "2019-12-31T10-00-00Z")]
    [InlineData("E", "real_time", "DATA/2019-12/prefix_real_time_data_2019-12-31_12-00-00.dat", "2019-12-31T12-00-00Z")]
    [InlineData("F", "default", "DATA/2019-12/20191231_12_x_0000.dat", "2019-12-31T12-00-00Z")]
    [InlineData("G", "default", "DATA/prefix_01-01-2020/00-40-22.dat", "2020-01-01T00-40-22Z")]
    [InlineData("H", "default", "2019-12-31_12-00-00.dat", "2019-12-31T12-00-00Z")]
    [InlineData("I", "default", "DATA/2019-12-31_23-55-00/data.dat", "2019-12-31T23-55-00Z")]
    [InlineData("J", "default", "DATA1/2020_01_01.dat", "2020-01-01T00-00-00Z")]
    public void CanGetFileBeginByPath(string database, string key, string filePath, string expectedFileBeginString)
    {
        // Arrange
        var expectedFileBegin = DateTime.ParseExact(expectedFileBeginString, "yyyy-MM-ddTHH-mm-ssZ", default, DateTimeStyles.AdjustToUniversal);
        var configFilePath = $"DATABASES/{database}/config.json";
        var configJson = File.ReadAllText(configFilePath);
        var config = JsonSerializer.Deserialize<Dictionary<string, Dictionary<string, IReadOnlyList<FileSource>>>>(configJson)!;
        var fileSource = config["/A/B/C"][key][0];
        var fullFilePath = Path.Combine($"DATABASES/{database}", filePath);

        // Act
        var success = StructuredFileDataSource.TryGetFileBeginByPath(fullFilePath, fileSource, out var fileBegin, folderBegin: default);

        // Assert
        Assert.True(success);
        Assert.Equal(expectedFileBegin, fileBegin);
    }

    [Theory]
    [InlineData("2020-01-01T00-00-00Z", "2020-01-01T00-00-00Z")]
    [InlineData("2020-01-02T00-00-00Z", "2020-01-01T00-00-00Z")]
    public async Task GetAvailabilityThrowsForInvalidTimePeriod(string beginString, string endString)
    {
        var begin = DateTime.ParseExact(beginString, "yyyy-MM-ddTHH-mm-ssZ", default, DateTimeStyles.AdjustToUniversal);
        var end = DateTime.ParseExact(endString, "yyyy-MM-ddTHH-mm-ssZ", default, DateTimeStyles.AdjustToUniversal);

        var dataSource = new StructuredFileDataSourceTester() as IDataSource;

        await Assert.ThrowsAsync<ArgumentException>(() =>
            dataSource!.GetAvailabilityAsync("/A/B/C", begin, end, CancellationToken.None));
    }

    [Fact]
    public async Task CanReadSingle()
    {
        var dataSource = new StructuredFileDataSourceTester() as IDataSource;

        var context = new DataSourceContext(
            ResourceLocator: new Uri(Path.Combine(Directory.GetCurrentDirectory(), "DATABASES/TESTDATA")),
            SystemConfiguration: default!,
            SourceConfiguration: default!,
            RequestConfiguration: default!);

        await dataSource!.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        var catalog = await dataSource!.GetCatalogAsync("/A/B/C", CancellationToken.None);
        var resource = catalog.Resources![0];
        var representation = resource.Representations![0];
        var catalogItem = new CatalogItem(catalog, resource, representation, default);

        var begin = new DateTime(2019, 12, 31, 0, 0, 0, DateTimeKind.Utc);
        var end = new DateTime(2020, 01, 03, 0, 0, 0, DateTimeKind.Utc);
        var (data, status) = ExtensibilityUtilities.CreateBuffers(representation, begin, end);

        var expectedLength = 3 * 86400;
        var expectedData = new long[expectedLength];
        var expectedStatus = new byte[expectedLength];

        void GenerateData(DateTimeOffset dateTime)
        {
            var data = Enumerable.Range(0, 600)
                .Select(value => dateTime.Add(TimeSpan.FromSeconds(value)).ToUnixTimeSeconds())
                .ToArray();

            var offset = (int)(dateTime - begin).TotalSeconds;
            data.CopyTo(expectedData.AsSpan()[offset..]);
            expectedStatus.AsSpan().Slice(offset, 600).Fill(1);
        }

        GenerateData(new DateTimeOffset(2019, 12, 31, 12, 00, 0, 0, TimeSpan.Zero));
        GenerateData(new DateTimeOffset(2019, 12, 31, 12, 20, 0, 0, TimeSpan.Zero));
        GenerateData(new DateTimeOffset(2020, 01, 01, 00, 00, 0, 0, TimeSpan.Zero));
        GenerateData(new DateTimeOffset(2020, 01, 02, 09, 40, 0, 0, TimeSpan.Zero));
        GenerateData(new DateTimeOffset(2020, 01, 02, 09, 50, 0, 0, TimeSpan.Zero));

        var request = new ReadRequest(catalogItem, data, status);

        await dataSource!.ReadAsync(
            begin,
            end,
            [request, request],
            default!,
            new Progress<double>(), CancellationToken.None);

        Assert.True(expectedData.SequenceEqual(MemoryMarshal.Cast<byte, long>(data.Span).ToArray()));
        Assert.True(expectedStatus.SequenceEqual(status.ToArray()));
    }

    [Theory]
    [InlineData("2020-01-01T00-00-00Z", "2020-01-01T00-00-00Z")]
    [InlineData("2020-01-02T00-00-00Z", "2020-01-01T00-00-00Z")]
    public async Task ReadSingleThrowsForInvalidTimePeriod(string beginString, string endString)
    {
        var begin = DateTime.ParseExact(beginString, "yyyy-MM-ddTHH-mm-ssZ", default, DateTimeStyles.AdjustToUniversal);
        var end = DateTime.ParseExact(endString, "yyyy-MM-ddTHH-mm-ssZ", default, DateTimeStyles.AdjustToUniversal);

        var dataSource = new StructuredFileDataSourceTester() as IDataSource;

        var context = new DataSourceContext(
            ResourceLocator: new Uri(Path.Combine(Directory.GetCurrentDirectory(), "DATABASES/TESTDATA")),
            SystemConfiguration: default!,
            SourceConfiguration: default!,
            RequestConfiguration: default!);

        await dataSource!.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        var catalog = await dataSource!.GetCatalogAsync("/A/B/C", CancellationToken.None);
        var resource = catalog.Resources![0];
        var representation = resource.Representations![0];
        var catalogItem = new CatalogItem(catalog, resource, representation, default);
        var request = new ReadRequest(catalogItem, default, default);

        await Assert.ThrowsAsync<ArgumentException>(() =>
            dataSource.ReadAsync(
                begin,
                end,
                [request],
                default!,
                default!,
                CancellationToken.None));
    }
}