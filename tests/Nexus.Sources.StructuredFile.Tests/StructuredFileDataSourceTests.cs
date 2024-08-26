using Microsoft.Extensions.Logging.Abstractions;
using Nexus.DataModel;
using Nexus.Extensibility;
using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;
using Xunit;

namespace Nexus.Sources.Tests;

public class StructuredFileDataSourceTests
{
    [Theory]

    // date + time
    [InlineData("2020-01-01T00-00-00", "yyyy-MM-ddTHH-mm-ss", "06:00", "2020-01-01T00-00-00+06:00")]
    [InlineData("2020-01-01T00-00-00Z", "yyyy-MM-ddTHH-mm-ssK", "06:00", "2020-01-01T00-00-00+00:00")]
    [InlineData("2020-01-01T00-00-00+00", "yyyy-MM-ddTHH-mm-sszz", "06:00", "2020-01-01T00-00-00+00:00")]
    [InlineData("2020-01-01T00-00-00+00:00", "yyyy-MM-ddTHH-mm-ssK", "06:00", "2020-01-01T00-00-00+00:00")]
    [InlineData("2020-01-01T00-00-00+03", "yyyy-MM-ddTHH-mm-sszz", "06:00", "2020-01-01T00-00-00+03:00")]
    [InlineData("2020-01-01T00-00-00+03:00", "yyyy-MM-ddTHH-mm-ssK", "06:00", "2020-01-01T00-00-00+03:00")]

    // date
    [InlineData("2020-01-01", "yyyy-MM-dd", "06:00", "2020-01-01T00-00-00+06:00")]
    [InlineData("2020-01-01Z", "yyyy-MM-ddK", "06:00", "2020-01-01T00-00-00+00:00")]
    [InlineData("2020-01-01+00", "yyyy-MM-ddzz", "06:00", "2020-01-01T00-00-00+00:00")]
    [InlineData("2020-01-01+00:00", "yyyy-MM-ddK", "06:00", "2020-01-01T00-00-00+00:00")]
    [InlineData("2020-01-01+03", "yyyy-MM-ddzz", "06:00", "2020-01-01T00-00-00+03:00")]
    [InlineData("2020-01-01+03:00", "yyyy-MM-ddK", "06:00", "2020-01-01T00-00-00+03:00")]

    // time
    [InlineData("00-00-00", "HH-mm-ss", "06:00", "0001-01-01T00-00-00+06:00")]
    [InlineData("00-00-00Z", "HH-mm-ssK", "06:00", "0001-01-01T00-00-00+00:00")]
    [InlineData("00-00-00+00", "HH-mm-sszz", "06:00", "0001-01-01T00-00-00+00:00")]
    [InlineData("00-00-00+00:00", "HH-mm-ssK", "06:00", "0001-01-01T00-00-00+00:00")]
    [InlineData("00-00-00+03", "HH-mm-sszz", "06:00", "0001-01-01T00-00-00+03:00")]
    [InlineData("00-00-00+03:00", "HH-mm-ssK", "06:00", "0001-01-01T00-00-00+03:00")]
    public void CanTryParseToUtc(
        string input,
        string format,
        string utcOffsetString,
        string expectedDateTimeString)
    {
        var utcOffset = TimeSpan.ParseExact(utcOffsetString, "hh\\:mm", default);

        var success = CustomDateTimeOffset.TryParseExact(
            input,
            format,
            utcOffset,
            out var actual);

        var expectedDateTimeStringParts = expectedDateTimeString.Split('+');

        var expected = new CustomDateTimeOffset
        (
            dateTime: DateTime.ParseExact(
                expectedDateTimeStringParts[0],
                "yyyy-MM-ddTHH-mm-ss",
                default),

            offset: TimeSpan.ParseExact(
                expectedDateTimeStringParts[1],
                "hh\\:mm",
                default)
        );

        Assert.True(success);
        Assert.Equal(expected, actual);
    }

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

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        var methodInfo = dataSource
            .GetType()
            .GetMethod(
                "TryGetFirstFile",
                BindingFlags.NonPublic | BindingFlags.Instance
            ) ?? throw new Exception("method info is null");
            
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
    [InlineData("DATABASES/L", "2020-01-01T00-00-00Z", "2020-01-04T00-00-00Z")]
    [InlineData("DATABASES/M", "2020-01-01T01-35-23Z", "2020-01-01T05-00-00Z")]
    [InlineData("DATABASES/N", "2019-12-31T12-00-00Z", "2020-01-03T12-00-00Z")]
    [InlineData("DATABASES/O", "2020-01-01T00-35-23Z", "2020-01-01T01-00-10Z")]
    [InlineData("DATABASES/P", "2020-01-01T00-00-00Z", "2020-01-01T00-20-00Z")]
    [InlineData("DATABASES/Q", "2019-12-31T23-00-00Z", "2020-01-02T23-00-00Z")]
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

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        var (begin, end) = await dataSource.GetTimeRangeAsync("/A/B/C", CancellationToken.None);

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
    [InlineData("DATABASES/L", "2020-01-01T00-00-00Z", "2020-01-04T00-00-00Z", 1, 0)]
    [InlineData("DATABASES/M", "2020-01-01T00-00-00Z", "2020-01-02T00-00-00Z", 4 / 24.0, 3)]
    [InlineData("DATABASES/N", "2020-01-01T00-00-00Z", "2020-01-03T00-00-00Z", 7 / 8.0, 2)]
    [InlineData("DATABASES/O", "2020-01-01T00-00-00Z", "2020-01-02T00-00-00Z", 3 / 288.0, 4)]
    [InlineData("DATABASES/P", "2020-01-01T00-00-00Z", "2020-01-02T00-00-00Z", 2 / 144.0, 4)]
    [InlineData("DATABASES/Q", "2020-01-01T00-00-00Z", "2020-01-03T00-00-00Z", 0.5, 1)]
    public async Task CanProvideAvailability(string root, string beginString, string endString, double expected, int precision)
    {
        // Arrange
        var begin = DateTime.ParseExact(beginString, "yyyy-MM-ddTHH-mm-ssZ", default, DateTimeStyles.AdjustToUniversal);
        var end = DateTime.ParseExact(endString, "yyyy-MM-ddTHH-mm-ssZ", default, DateTimeStyles.AdjustToUniversal);

        var dataSource = new StructuredFileDataSourceTester() as IDataSource;

        var context = new DataSourceContext(
            ResourceLocator: new Uri(Path.Combine(Directory.GetCurrentDirectory(), root)),
            SystemConfiguration: default!,
            SourceConfiguration: default!,
            RequestConfiguration: default!);

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        // Act
        var actual = await dataSource.GetAvailabilityAsync("/A/B/C", begin, end, CancellationToken.None);

        // Assert
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
    [InlineData("K", "default", "DATA/2020-01-01T00-00-00Z.dat", "2020-01-01T00-00-00Z")]
    [InlineData("L", "default", "DATA/2020-01-02T00-00-00Z_V1.dat", "2020-01-02T00-00-00Z")]
    [InlineData("M", "default", "DATA/2020-01-01T01-35-23Z.dat", "2020-01-01T01-35-23Z")]
    [InlineData("N", "default", "DATA/2020-01-01/06-00-00.dat", "2019-12-31T18-00-00Z")]
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
        Assert.Equal(expectedFileBegin, fileBegin.UtcDateTime);
    }

    [Fact]
    public async Task CanFindFileBeginAndPaths_Database_M()
    {
        // Arrange
        var databaseFolderPath = "DATABASES/M";
        var configFilePath = $"{databaseFolderPath}/config.json";
        var configJson = File.ReadAllText(configFilePath);
        var config = JsonSerializer.Deserialize<Dictionary<string, Dictionary<string, IReadOnlyList<FileSource>>>>(configJson)!;
        var fileSource = config["/A/B/C"]["default"][0];
        var dataSource = new StructuredFileDataSourceTester();

        var context = new DataSourceContext(
            ResourceLocator: new Uri(Path.Combine(Directory.GetCurrentDirectory(), databaseFolderPath)),
            SystemConfiguration: default!,
            SourceConfiguration: default!,
            RequestConfiguration: default!);

        await ((IDataSource)dataSource).SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        // Act
        var actual = await dataSource.FindFileBeginAndPathsAsync(
            begin: new DateTime(2020, 01, 01, 01, 00, 00, DateTimeKind.Utc),
            fileSource: fileSource
        );

        // Assert
        Assert.Equal(
            expected: new DateTime(2020, 01, 01, 01, 00, 00, DateTimeKind.Utc),
            actual: actual.RegularUtcFileBegin
        );

        Assert.Collection(actual.Item2.Order(),
            actual1 =>
            {
                Assert.Equal(
                    expected: "2020-01-01T01-35-23Z.dat",
                    actual: Path.GetFileName(actual1.FilePath)
                );

                Assert.Equal(
                    expected: new TimeSpan(00, 35, 23),
                    actual: actual1.FileBeginOffset
                );
            },
            actual2 =>
            {
                Assert.Equal(
                    expected: "2020-01-01T01-47-01Z.dat",
                    actual: Path.GetFileName(actual2.FilePath)
                );

                Assert.Equal(
                    expected: new TimeSpan(00, 47, 01),
                    actual: actual2.FileBeginOffset
                );
            }
        );
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
            dataSource.GetAvailabilityAsync("/A/B/C", begin, end, CancellationToken.None));
    }

    [Theory]
    [InlineData("A", "2019-12-31T00:00:00Z", "2020-01-03T00:00:00Z")]
    [InlineData("B", "2019-12-31T00:00:00Z", "2020-01-03T00:00:00Z")]
    [InlineData("C", "2019-12-31T00:00:00Z", "2020-01-03T00:00:00Z")]
    [InlineData("D", "2019-12-31T00:00:00Z", "2020-01-03T00:00:00Z")]
    [InlineData("E", "2019-12-31T00:00:00Z", "2020-01-03T00:00:00Z")]
    [InlineData("F", "2019-12-31T00:00:00Z", "2020-01-03T00:00:00Z")]
    [InlineData("G", "2019-12-31T00:00:00Z", "2020-01-02T00:00:00Z")]
    [InlineData("H", "2019-12-31T00:00:00Z", "2020-01-03T00:00:00Z")]
    [InlineData("I", "2019-12-31T00:00:00Z", "2020-01-02T00:00:00Z")]
    [InlineData("J", "2020-01-01T00:00:00Z", "2020-01-05T00:00:00Z")]
    [InlineData("K", "2020-01-01T00:00:00Z", "2020-01-02T00:00:00Z")]
    [InlineData("L", "2020-01-01T00:00:00Z", "2020-01-04T00:00:00Z")]
    [InlineData("M", "2020-01-01T00:00:00Z", "2020-01-02T00:00:00Z")]
    [InlineData("N", "2020-01-01T00:00:00Z", "2020-01-04T00:00:00Z")]
    [InlineData("O", "2020-01-01T00:00:00Z", "2020-01-02T00:00:00Z")]
    [InlineData("O2", "2020-01-01T00:35:23Z", "2020-01-01T00:35:24Z")]
    [InlineData("P", "2020-01-01T00:00:00Z", "2020-01-02T00:00:00Z")]
    [InlineData("Q", "2020-01-01T00:00:00Z", "2020-01-03T00:00:00Z")]
    public async Task CanRead_ReadInfos(string database, string beginString, string endString)
    {
        // Arrange
        var readInfos = new List<ReadInfo>();
        var dataSource = new StructuredFileDataSourceTester(readInfos.Add) as IDataSource;

        var context = new DataSourceContext(
            ResourceLocator: new Uri(Path.Combine(Directory.GetCurrentDirectory(), $"DATABASES/{database[0]}")),
            SystemConfiguration: default!,
            SourceConfiguration: default!,
            RequestConfiguration: default!);

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        var catalog = await dataSource.GetCatalogAsync("/A/B/C", CancellationToken.None);
        var resource = catalog.Resources![0];
        var representation = resource.Representations![0];
        var catalogItem = new CatalogItem(catalog, resource, representation, default);

        var begin = DateTime.ParseExact(beginString, "yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);
        var end = DateTime.ParseExact(endString, "yyyy-MM-ddTHH:mm:ssZ", CultureInfo.InvariantCulture, DateTimeStyles.AdjustToUniversal);
        var (data, status) = ExtensibilityUtilities.CreateBuffers(representation, begin, end);

        var request = new ReadRequest(catalogItem, data, status);

        // Act
        await dataSource.ReadAsync(
            begin,
            end,
            [request, request],
            default!,
            new Progress<double>(), CancellationToken.None
        );

        // Assert
        var preparedReadInfos = readInfos
            .Select(x => x with 
                { 
                    FileSource = default!,
                    FilePath = x.FilePath.Split("/DATABASES/")[1]
                }
            )
            .OrderBy(x => x.FilePath);

        var actualJsonString = JsonSerializer.Serialize(preparedReadInfos, new JsonSerializerOptions { WriteIndented = true });
        File.WriteAllText($"{database}.json", actualJsonString);

        var expectedJsonString = File.ReadAllText($"expected/{database}.json");

        Assert.Equal(expectedJsonString, actualJsonString);
    }

    [Fact]
    public async Task CanRead()
    {
        var dataSource = new StructuredFileDataSourceTester() as IDataSource;

        var context = new DataSourceContext(
            ResourceLocator: new Uri(Path.Combine(Directory.GetCurrentDirectory(), "DATABASES/TESTDATA")),
            SystemConfiguration: default!,
            SourceConfiguration: default!,
            RequestConfiguration: default!);

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        var catalog = await dataSource.GetCatalogAsync("/A/B/C", CancellationToken.None);
        var resource = catalog.Resources![0];
        var representation = resource.Representations![0];
        var catalogItem = new CatalogItem(catalog, resource, representation, default);

        var begin = new DateTime(2019, 12, 31, 0, 0, 0, DateTimeKind.Utc);
        var end = new DateTime(2020, 01, 03, 0, 0, 0, DateTimeKind.Utc);
        var (data, status) = ExtensibilityUtilities.CreateBuffers(representation, begin, end);

        var expectedLength = 3 * 86400;
        var expectedData = new long[expectedLength];
        var expectedStatus = new byte[expectedLength];

        void GenerateData(DateTimeOffset dateTime, int length)
        {
            var data = Enumerable.Range(0, length)
                .Select(value => dateTime.Add(TimeSpan.FromSeconds(value)).ToUnixTimeSeconds())
                .ToArray();

            var offset = (int)(dateTime - begin).TotalSeconds;
            data.CopyTo(expectedData.AsSpan()[offset..]);
            expectedStatus.AsSpan().Slice(offset, length).Fill(1);
        }

        GenerateData(new DateTimeOffset(2019, 12, 31, 12, 00, 10, TimeSpan.Zero), length: 590);
        GenerateData(new DateTimeOffset(2019, 12, 31, 12, 20, 00, TimeSpan.Zero), length: 600);
        GenerateData(new DateTimeOffset(2020, 01, 01, 00, 00, 00, TimeSpan.Zero), length: 600);
        GenerateData(new DateTimeOffset(2020, 01, 02, 09, 40, 00, TimeSpan.Zero), length: 600);
        GenerateData(new DateTimeOffset(2020, 01, 02, 09, 50, 00, TimeSpan.Zero), length: 600);

        var request = new ReadRequest(catalogItem, data, status);

        await dataSource.ReadAsync(
            begin,
            end,
            [request, request],
            default!,
            new Progress<double>(), CancellationToken.None
        );

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

        await dataSource.SetContextAsync(context, NullLogger.Instance, CancellationToken.None);

        var catalog = await dataSource.GetCatalogAsync("/A/B/C", CancellationToken.None);
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