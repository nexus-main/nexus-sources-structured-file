# DATABASES/R: BufferBegin Use Cases

This fixture documents why `ReadInfo.BufferBegin` exists.

`BufferBegin` is the timestamp represented by index `0` of the sliced `ReadRequest.Data` and `ReadRequest.Status` buffers passed to a data source implementation.

Timestamp-aware readers should place rows with:

```csharp
var i = (int)((dateTime - info.BufferBegin).Ticks / samplePeriod.Ticks);
```

Sequential readers without row timestamps should continue to use `FileOffset` and `FileBlock`.

## Case 1: File Starts Late

The file `DATA/2020-01-01T00-00-03Z.dat` belongs to the regular period starting at `00:00:00`, but its first row is at `00:00:03`.

For a request from `00:00:00` to `00:00:10`, structured-file passes a buffer slice whose first element represents `00:00:03`:

```text
BufferBegin = 2020-01-01T00:00:03Z
```

The row `00:00:03;30` is written to local index `0` of that slice. In the full request buffer this becomes index `3`, which is the correct timestamp position.

## Case 2: Request Starts Inside A File

For a request from `00:00:04` to `00:00:07`, the same physical file already contains data before the request begin.

```text
BufferBegin = 2020-01-01T00:00:04Z
FileOffset  = 1
```

`FileOffset` tells sequential readers to skip one row. A timestamp-aware reader instead uses `BufferBegin`, so the row `00:00:04;40` is written to local index `0`.

## Case 3: Irregular File Continues Into Later Period

The same irregular file starts at `00:00:03` and may contain data until `00:00:13` because the configured `filePeriod` is 10 seconds.

For a request from `00:00:10` to `00:00:13`, structured-file reads the same physical file again for the later regular period:

```text
BufferBegin = 2020-01-01T00:00:10Z
FileOffset  = 7
```

Rows `00:00:10;100`, `00:00:11;110`, and `00:00:12;120` are written to local indices `0`, `1`, and `2`.

## Why RegularFileBegin Is Not Enough

`RegularFileBegin` is the grid-aligned period begin. It is useful metadata, but it is not necessarily the time represented by index `0` of the buffer slice handed to the plugin.

Using `RegularFileBegin` for timestamp-based row placement mixes file-grid coordinates with buffer-slice coordinates. That can shift rows when files start late, when a request starts inside a file, or when an irregular file is read again in a later regular period.
