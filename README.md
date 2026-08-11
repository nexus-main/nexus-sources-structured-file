# Nexus.Sources.StructuredFile

This data source extension provides a base class for reading regularly organized structured files into Nexus. Derived data sources, such as CSV readers, implement the actual file parsing while this package handles file discovery, time-range slicing, file-source selection, offsets, and read-buffer metadata.

## Configuration Shape

A structured-file data source is configured with top-level settings and one or more file source groups.

```json
{
  "fileSourceGroupsMap": {
    "default": {
      "voltage": [
        {
          "begin": "2026-08-10T00:00:00Z",
          "pathSegments": ["'DATA'", "yyyy-MM-dd", "HH"],
          "fileTemplate": "'DAQ_5ms-'yyyy-MM-dd-HH-mm-ss'.csv'",
          "fileDateTimePreselector": null,
          "fileDateTimeSelector": null,
          "filePeriod": "00:10:00",
          "maxFileDuration": "00:10:01",
          "fileNameOffset": "00:00:00",
          "utcOffset": "00:00:00",
          "irregularTimeInterval": true,
          "additionalSettings": {}
        }
      ]
    }
  },
  "additionalSettings": {}
}
```

The exact `additionalSettings` shape is defined by the derived data source implementation.

## File Discovery And Duration

The most important configuration concept is that file discovery and file coverage are separate concerns:

| Setting | Question it answers | Used for | Think of it as |
| --- | --- | --- | --- |
| `filePeriod` | At what regular cadence are files or folders named, grouped, or expected? | Building and searching candidate file paths, including wildcard/glob paths | The file discovery grid |
| `maxFileDuration` | How long can one file contain data after the timestamp parsed from its file name? | Looking backward far enough so a file with an imprecise or shifted name is not missed | The maximum real file duration |

Short version:

```text
filePeriod = where to look
maxFileDuration = how long one file may cover
```

The discovery grid should match the cadence implied by the folder and file naming scheme. For example, if folders or file names are organized every 10 minutes, use a 10-minute grid even when the row timestamps inside the files are more precise.

The duration setting is the maximum elapsed time covered by one physical file, measured from the timestamp parsed from its name. If it is omitted, the grid period is used as the duration as well.

Keeping these concepts separate is important for files whose names are less precise than their contents. A file may be discovered on a clean 10-minute grid while its rows use millisecond timestamps. The search must still follow the 10-minute layout, but the lookback window must cover the real maximum file duration.

### Use Cases

| Case | Example file names | Example file contents | `filePeriod` | `maxFileDuration` | Why |
| --- | --- | --- | --- | --- | --- |
| Exact regular files | `2026-08-10/12/2026-08-10-12-00-00.csv`<br>`2026-08-10/12/2026-08-10-12-10-00.csv` | First file contains `12:00:00.000` through before `12:10:00.000`.<br>Second file contains `12:10:00.000` through before `12:20:00.000`. | `00:10:00` | Omit, or `00:10:00` | File names, folder layout, and file contents all use the same 10-minute cadence. The default `maxFileDuration = filePeriod` is enough. |
| File name less precise than file contents | `2026-08-10/12/DAQ_5ms-2026-08-10-12-20-00.csv`<br>`2026-08-10/12/DAQ_5ms-2026-08-10-12-30-00.csv` | First file contains rows from about `12:20:00.133` through before `12:30:00.133`.<br>Second file contains rows from about `12:30:00.133` through before `12:40:00.133`. | `00:10:00` | `00:10:01` or the measured maximum | This is the common irregular case. The file name has only second precision, but row timestamps have millisecond precision. A request at `12:30:00.050` may still need the file named `12-20-00.csv`, because the next file's first row is only at about `12:30:00.133`. |
| Irregular start names on a regular storage cadence | `2026-08-10/12/DAQ_5ms-2026-08-10-12-23-26.csv`<br>`2026-08-10/12/DAQ_5ms-2026-08-10-12-33-26.csv` | First file contains about `12:23:26.000` through before `12:33:26.xxx`.<br>Second file contains about `12:33:26.000` through before `12:43:26.xxx`. | `00:10:00` | `00:10:01` or the measured maximum | The storage is still processed on a 10-minute grid, but the parsed file begin is not aligned to the grid. `maxFileDuration` tells the search how far a file can extend from its parsed begin timestamp. |
| Files shorter than the naming cadence | `2026-08-10/12/2026-08-10-12-00-00.csv`<br>`2026-08-10/12/2026-08-10-12-10-00.csv` | First file contains `12:00:00.000` through before `12:08:00.000`.<br>No data exists from `12:08:00.000` to `12:10:00.000`. | `00:10:00` | Omit, or `00:08:00` | The naming cadence is still 10 minutes. `maxFileDuration` does not fill gaps; a shorter value can reduce unnecessary candidate reads but is not required for correctness. |
| File named by end time | `2026-08-10/12/DAQ_5ms-2026-08-10-12-10-00.csv`<br>`2026-08-10/12/DAQ_5ms-2026-08-10-12-20-00.csv` | First file contains `12:00:00.000` through before `12:10:00.000`.<br>Second file contains `12:10:00.000` through before `12:20:00.000`. | `00:10:00` | Omit, or `00:10:00` | Use `fileNameOffset`, for example `00:10:00`, when the timestamp in the file name represents the file end instead of the file begin. This is independent from `maxFileDuration`. |
| No reliable cadence | `a/file-17.csv`<br>`archive/x9.csv` | File begin and end times are arbitrary and not derivable from a regular timestamp grid. | Hard to configure | Hard to configure | The current data source model expects a time-based file organization. A scanner or index of actual file ranges would be a better design. |

### Practical Rules

| Situation | Recommended configuration |
| --- | --- |
| File names, folders, and contents all use the same cadence | Set `filePeriod` to that cadence; omit `maxFileDuration`. |
| File names are less precise than row timestamps | Set `filePeriod` to the naming/storage cadence; set `maxFileDuration` to the maximum real elapsed file duration, including the precision margin. |
| File names are irregular but storage is still processed on a regular cadence | Set `filePeriod` to the storage cadence; set `maxFileDuration` to the maximum real file coverage. |
| Files are named by their end time | Set `filePeriod` to the naming cadence and set `fileNameOffset`; `maxFileDuration` is only needed if the real file duration can exceed `filePeriod`. |
| Files are shorter than the naming cadence | Keep `filePeriod` at the naming cadence; optionally reduce `maxFileDuration` to the real maximum to avoid extra candidate reads. |
| You do not know the file cadence | The current configuration model may not be sufficient. Consider scanning all files or maintaining an index outside this data source. |

## Glob And Wildcard Searches

`maxFileDuration` does not define the glob pattern. The path template and `filePeriod` define which candidate paths are generated. `maxFileDuration` only expands the time range backward before those candidate paths are generated.

Example file source:

```json
{
  "pathSegments": ["'DATA'", "yyyy-MM-dd", "HH"],
  "fileTemplate": "'DAQ_5ms-'yyyy-MM-dd-HH-mm-ss'.csv'",
  "fileDateTimePreselector": "DAQ_5ms-(.{19})\\.csv",
  "fileDateTimeSelector": "yyyy-MM-dd-HH-mm-ss",
  "filePeriod": "00:10:00",
  "maxFileDuration": "00:10:01"
}
```

For a read beginning at `2026-08-10T12:32:00Z`, the lookback start is approximately `12:21:59` because `maxFileDuration` is `00:10:01`.

The algorithm then maps that enlarged range onto the `filePeriod` grid. With a 10-minute grid, candidate buckets include:

```text
2026-08-10T12:20:00Z
2026-08-10T12:30:00Z
```

Those buckets produce paths from `pathSegments`, for example:

```text
2026-08-10/12/
```

The file name template contains wildcard-like sections or requires a preselector, so files in that folder can be matched and then parsed:

```text
2026-08-10/12/DAQ_5ms-2026-08-10-12-23-26.csv
2026-08-10/12/DAQ_5ms-2026-08-10-12-33-26.csv
```

If the read begins at `2026-08-10T13:02:00Z`, the lookback start is approximately `12:51:59`. Candidate buckets include:

```text
2026-08-10T12:50:00Z
2026-08-10T13:00:00Z
```

Those buckets can produce two folder searches:

```text
2026-08-10/12/
2026-08-10/13/
```

This is the key point: `maxFileDuration` caused the earlier `12:50` bucket to be considered, but the buckets are still spaced by `filePeriod`. The algorithm does not create a `00:10:01` search grid.

Using `maxFileDuration` as the grid would be wrong because a `00:10:01` grid drifts away from a 10-minute file organization:

```text
12:00:00
12:10:01
12:20:02
12:30:03
```

The actual file organization may be:

```text
12:00:00
12:10:00
12:20:00
12:30:00
```

## StructuredFileDataSource Settings

### `StructuredFileDataSourceSettings<TAdditionalSettings, TAdditionalFileSourceSettings>`

| JSON property | .NET type | Required | Description |
| --- | --- | --- | --- |
| `fileSourceGroupsMap` | `Dictionary<string, Dictionary<string, IReadOnlyList<FileSource<TAdditionalFileSourceSettings>>>>` | Yes | Maps source groups and resource-specific file source lists. Each list can contain multiple time-limited file source definitions. |
| `additionalSettings` | `TAdditionalSettings` | Depends on derived source | Top-level settings used by the derived source implementation. |

### `FileSource<TAdditionalSettings>`

| JSON property | .NET type | Required | Description |
| --- | --- | --- | --- |
| `begin` | `DateTime` | Yes | The UTC date/time from which this file source definition applies. Multiple file sources can be used when file layout changes over time. |
| `pathSegments` | `string[]` | Yes | Date/time format strings for folder segments. Literal text must be quoted using .NET date/time format escaping, for example `"'DATA'"`, `"yyyy-MM"`, `"dd"`. |
| `fileTemplate` | `string` | Yes | Date/time format string for file names. Literal parts must be quoted. Unknown or variable characters can be represented with template wildcards when combined with the implementation's matching logic. |
| `fileDateTimePreselector` | `string?` | No | Optional regular expression used to select the relevant date/time substring from a file name. This is needed when file names contain random IDs, version suffixes, or several file kinds in the same folder. |
| `fileDateTimeSelector` | `string?` | Required when `fileDateTimePreselector` is set | Date/time format used to parse the substring selected by `fileDateTimePreselector`. |
| `filePeriod` | `TimeSpan` | Yes | The regular grid used to locate and process candidate files. It should match the folder/file naming cadence. |
| `maxFileDuration` | `TimeSpan?` | No | Maximum real duration of one file from the parsed file timestamp. If omitted, `filePeriod` is used. Use it when file-name precision, timestamp offsets, or irregular starts can make a previous file relevant for the requested range. |
| `fileNameOffset` | `TimeSpan` | Yes | Offset between the timestamp encoded in the file name and the actual data begin. This is useful for files named by their end time instead of their begin time. |
| `utcOffset` | `TimeSpan` | Yes | Offset applied when file timestamps are expressed in local time rather than UTC. |
| `irregularTimeInterval` | `bool` | Yes | Indicates that file intervals are not aligned exactly to multiples of `filePeriod`. This is common when file names contain irregular start seconds or when file end times are not grid-aligned. |
| `additionalSettings` | `TAdditionalSettings` | Depends on derived source | Per-file-source settings used by the derived implementation, for example delimiter, data-row, timestamp-column, or binary layout settings. |

## `ReadInfo<TAdditionalSettings>`

Derived data sources receive `ReadInfo<TAdditionalSettings>` when asked to read one candidate file. It describes both the physical file and the part of the Nexus read buffer this file should fill.

| Member | Type | Description |
| --- | --- | --- |
| `FilePath` | `string` | Full path of the file to read. |
| `FileSource` | `FileSource<TAdditionalSettings>` | The file source definition that matched this file. Use `FileSource.AdditionalSettings` for per-source parser options. |
| `RegularFileBegin` | `DateTime` | The regular grid begin associated with this file candidate. This is the nominal bucket begin from `filePeriod`, not necessarily the first timestamp represented by the provided buffer. |
| `BufferBegin` | `DateTime` | The date/time represented by index `0` of the provided read buffers. Timestamp-aware readers should calculate buffer indices relative to this value. |
| `FileOffset` | `long` | Element offset within the file for sequential readers. Use this when rows/samples are read by position rather than by timestamp. |
| `FileBlock` | `long` | Number of elements requested from the file for sequential readers. |
| `FileLength` | `long` | Expected total number of elements in the file based on `filePeriod`, `maxFileDuration`, and `samplePeriod`. |

### Timestamp-Aware Readers

Readers that parse timestamps from the file should write values relative to `BufferBegin`:

```csharp
var index = (int)((timestamp - info.BufferBegin).Ticks / samplePeriod.Ticks);
```

Use this for files where each row has its own timestamp column.

### Sequential Readers

Readers that do not parse per-row timestamps should use `FileOffset` and `FileBlock`:

```csharp
ReadSamples(info.FilePath, info.FileOffset, info.FileBlock, destination);
```

Use this for files where the requested sample positions are derived from the file's nominal timing rather than from timestamps inside the file.

## Test Case Description

The test databases cover file and folder structures that contain files with a maximum duration. Files may be shorter than the configured upper limit, but they must not exceed it. The upper limit is part of the source configuration because Nexus cannot know how long a file is before candidate files have been found and read.

| Case | Description | UTC offset | File period | Regex |
| --- | --- | --- | --- | --- |
| A | Text folder, month folder, day folder, datetime file, ignore unrelated folders | `00:00:00` | `00:10:00` | |
| B | Month folder, text folder, day folder, datetime file | `00:00:00` | `00:10:00` | |
| C | Day folder, datetime file | `00:00:00` | `00:10:00` | |
| D | Text folder, datetime file, separate file source groups for separate folders | `02:00:00`, `01:00:00` | `00:10:00`, `1.00:00:00` | |
| E | Month folder, datetime file, separate file source groups for common folder | `00:00:00` | `1.00:00:00`, `00:30:00` | |
| F | Month folder, date + hour + random number file | `00:00:00` | `01:00:00` | x |
| G | Day folder with prefix, time file | `00:00:00` | `00:00:01` | |
| H | Datetime file without parent folders | `00:00:00` | `00:10:00` | |
| I | Datetime folder, text file | `00:00:00` | `00:05:00` | |
| J | Datetime file with varying format and changing root folder, multiple time-limited files sources per group | `00:00:00` | `1.00:00:00` | |
| K | Datetime file without parent folders | `00:00:00` | `1.00:00:00` | |
| L | Datetime file with version suffix | `00:00:00` | `1.00:00:00` | x |
| M | Datetime file with random start time | `00:00:00` | `01:00:00` | x |
| N | Date folder, time file | `12:00:00` | `06:00:00` | |
| O | Datetime file with random start time and interval exceeding end | `00:00:00` | `01:00:00` | x |
| O2 | Datetime file with random start time and interval exceeding end, different time period for test | `00:00:00` | `01:00:00` | x |
| P | Datetime file with file name offset | `00:00:00` | `00:10:00` | |
| Q | Datetime file with file period larger than UTC offset | `01:00:00` | `1.00:00:00` | |
| R | Datetime file with random start time and `maxFileDuration` larger than `filePeriod`, used to verify `ReadInfo.BufferBegin` for timestamp-aware readers | `00:00:00` | `00:00:10` | x |
