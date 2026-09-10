using Nexus.DataModel;

namespace Nexus.Sources;

/// <summary>
/// Settings for the structured file data source.
/// </summary>
/// <typeparam name="TAdditionalSettings">The type of the additional settings (top-level).</typeparam>
/// <typeparam name="TAdditionalFileSourceSettings">The type of the additional file source settings.</typeparam>
/// <param name="FileSourceGroupsMap">The file source groups map.</param>
/// <param name="AdditionalSettings">The addtional settings.</param>
public record StructuredFileDataSourceSettings<TAdditionalSettings, TAdditionalFileSourceSettings>(
    Dictionary<string, Dictionary<string, IReadOnlyList<FileSource<TAdditionalFileSourceSettings>>>> FileSourceGroupsMap,
    TAdditionalSettings AdditionalSettings
);

/// <summary>
/// A structure to hold information about a file-based database.
/// </summary>
/// <param name="Begin">The date/time from when this file source applies.</param>
/// <param name="PathSegments">A format string that describes the folder structure of the data. An example of a file that is located under the path "group-A/2020-01" would translate into the array ["'group-A'", "yyyy-MM"].</param>
/// <param name="FileTemplate">A format string that describes the file naming scheme. The template of a file named 20200101_13_my-id_1234.dat would look like "yyyyMMdd_HH'_my-id_????.dat'".</param>
/// <param name="FileDateTimePreselector">An optional regular expression to select only relevant parts of a file name (e.g. to select the date/time and a unqiue identifier in case there is more than one kind of file in the same folder). In case of a file named 20200101_13_my-id_1234.dat the preselector could be like "(.{11})_my-id". It is also required for file names containing an opaque string that changes for every file.</param>
/// <param name="FileDateTimeSelector">An optional date/time selector which is mandatory when the preselector is provided. In case of a file named like "20200101_13_my-id_1234.dat", and a preselector of "(.{11})_my-id", the selector should be like "yyyyMMdd_HH".</param>
/// <param name="FilePeriod">The regular time-grid period used to locate and process candidate files, including wildcard file searches. It answers which nominal time bucket is inspected for a request timestamp and should match the cadence implied by the folder/file naming scheme. It is not the exact end of an irregular file. A smaller value creates finer processing buckets and more lookup iterations; a larger value creates broader buckets but can miss or group files incorrectly when it no longer matches the naming cadence.</param>
/// <param name="MaxFileDuration">The optional maximum elapsed time covered by a single file, measured from the timestamp parsed from the file name. If omitted, <paramref name="FilePeriod" /> is used. For irregular files, this controls how far back the search includes earlier files that may still overlap the request; it does not define the regular search bucket and does not replace <paramref name="FilePeriod" />. A value smaller than the real file coverage can drop data or miss overlapping irregular files; a larger value can read more candidate data than necessary.</param>
/// <param name="FileNameOffset">The file name offset of the file data. This is useful for files that are named according to the end date of the data they contain.</param>
/// <param name="UtcOffset">The UTC offset of the file data.</param>
/// <param name="IrregularTimeInterval">The file time interval is irregular. I.e. the file end is not aligned to multiples of the file period.</param>
/// <param name="AdditionalSettings">Additional settings to be used by the data source implementation.</param>
public record FileSource<TAdditionalSettings>(
    DateTime Begin,
    string[] PathSegments,
    string FileTemplate,
    string? FileDateTimePreselector,
    string? FileDateTimeSelector,
    TimeSpan FilePeriod,
    TimeSpan? MaxFileDuration,
    TimeSpan FileNameOffset,
    TimeSpan UtcOffset,
    bool IrregularTimeInterval,
    TAdditionalSettings AdditionalSettings
);

/// <summary>
/// A read request.
/// </summary>
/// <param name="OriginalResourceName">The original resource name.</param>
/// <param name="CatalogItem">The catalog item to be read.</param>
/// <param name="Data">The data buffer.</param>
/// <param name="Status">The status buffer. A value of 0x01 ('1') indicates that the corresponding value in the data buffer is valid, otherwise it is treated as <see cref="float.NaN"/> or <see cref="double.NaN"/>, respectively.</param>
public record StructuredFileReadRequest(
    string OriginalResourceName,
    CatalogItem CatalogItem,
    Memory<byte> Data,
    Memory<byte> Status
);

/// <summary>
/// A structure to hold read information.
/// </summary>
/// <param name="FilePath">The path of the file to read.</param>
/// <param name="FileSource">The associated file source.</param>
/// <param name="RegularFileBegin">The regular begin date/time of the file.</param>
/// <param name="BufferBegin">The date/time represented by the first element of the provided read buffers.</param>
/// <param name="FileOffset">The element offset within the file.</param>
/// <param name="FileBlock">The element count to read from the file.</param>
/// <param name="FileLength">The expected total number of elements within the file.</param>
public record ReadInfo<TAdditionalSettings>(

#if !IS_PUBLISH_BUILD
#pragma warning disable CS1573
    int BufferOffset,
#pragma warning restore CS1573
#endif

    string FilePath,
    FileSource<TAdditionalSettings> FileSource,
    DateTime RegularFileBegin,
    DateTime BufferBegin,
    long FileOffset,
    long FileBlock,
    long FileLength
);
