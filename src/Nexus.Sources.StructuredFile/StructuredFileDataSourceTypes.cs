﻿using System.Text.Json;
using Nexus.DataModel;
using Nexus.Extensibility;

namespace Nexus.Sources;

/// <summary>
/// A structure to hold information about a file-based database.
/// </summary>
/// <param name="Begin">The date/time from when this file source applies.</param>
/// <param name="PathSegments">A format string that describes the folder structure of the data. An example of a file that is located under the path "group-A/2020-01" would translate into the array ["'group-A'", "yyyy-MM"].</param>
/// <param name="FileTemplate">A format string that describes the file naming scheme. The template of a file named 20200101_13_my-id_1234.dat would look like "yyyyMMdd_HH'_my-id_????.dat'".</param>
/// <param name="FileDateTimePreselector">An optional regular expression to select only relevant parts of a file name (e.g. to select the date/time and a unqiue identifier in case there is more than one kind of file in the same folder). In case of a file named 20200101_13_my-id_1234.dat the preselector could be like "(.{11})_my-id". It is also required for file names containing an opaque string that changes for every file.</param>
/// <param name="FileDateTimeSelector">An optional date/time selector which is mandatory when the preselector is provided. In case of a file named like "20200101_13_my-id_1234.dat", and a preselector of "(.{11})_my-id", the selector should be like "yyyyMMdd_HH".</param>
/// <param name="FilePeriod">The period per file.</param>
/// <param name="UtcOffset">The UTC offset of the file data.</param>
/// <param name="IrregularTimeInterval">The file time interval is irregular. I.e. the file end is not aligned to multiples of the file period.</param>
/// <param name="AdditionalProperties">Additional properties to be used by the data source implementation.</param>
public record FileSource(
    DateTime Begin,
    string[] PathSegments,
    string FileTemplate,
    string? FileDateTimePreselector,
    string? FileDateTimeSelector,
    TimeSpan FilePeriod,
    TimeSpan UtcOffset,
    bool IrregularTimeInterval,
    JsonElement? AdditionalProperties
);

/// <summary>
/// A structure to hold read information.
/// </summary>
/// <param name="FilePath">The path of the file to read.</param>
/// <param name="FileSource">The associated file source.</param>
/// <param name="RegularFileBegin">The regular begin date/time of the file.</param>
/// <param name="FileOffset">The element offset within the file.</param>
/// <param name="FileBlock">The element count to read from the file.</param>
/// <param name="FileLength">The expected total number of elements within the file.</param>
public record ReadInfo(

#if DEBUG
#pragma warning disable CS1573
    int BufferOffset,
#pragma warning restore CS1573
#endif

    string FilePath,
    FileSource FileSource,
    DateTime RegularFileBegin,
    long FileOffset,
    long FileBlock,
    long FileLength
);

/// <summary>
/// An read request with additional data.
/// </summary>
/// <param name="CatalogItem">The CatalogItem to be read.</param>
/// <param name="Data">The data buffer.</param>
/// <param name="Status">The status buffer. A value of 0x01 ('1') indicates that the corresponding value in the data buffer is valid, otherwise it is treated as System.Double.NaN.</param>
/// <param name="OriginalName">The original name of the resource to read.</param>
public record StructuredFileReadRequest(
    CatalogItem CatalogItem,
    Memory<byte> Data,
    Memory<byte> Status,
    string OriginalName
) : ReadRequest(CatalogItem, Data, Status);