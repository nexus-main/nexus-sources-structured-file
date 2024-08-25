using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Text.RegularExpressions;
using Microsoft.Extensions.Logging;
using Nexus.DataModel;
using Nexus.Extensibility;

namespace Nexus.Sources;

/// <summary>
/// A base class to simplify reading data from structured, file-based data sources.
/// </summary>
public abstract class StructuredFileDataSource : IDataSource
{
    // This implementation assumes the following:
    //
    // (1) The top-most folders carry rough date/time information while deeper nested
    // folders carry more fine-grained date/time information. Examples:
    //
    // OK:      /2019/2019-12/2019-12-31_12-00-00.dat
    // OK:      /2019-12/2019-12-31_12-00-00.dat
    // OK:      /2019-12/2019-12-31/2019-12-31_12-00-00.dat
    // OK:      /2019-12/2019-12-31/12-00-00.dat
    //
    // NOT OK:  /2019/12/...
    // NOT OK:  /2019/12-31/...
    // NOT OK:  /2019-12/31/...
    // NOT OK:  /2019-12-31/31/...
    //
    // NOTE: The format of the date/time is only illustrative and is being determined
    // by the specified format provider.
    //
    // (2) The files are always located in the most nested folder and not distributed
    // over the hierarchy.
    //
    // (3) Most nested folders are not empty.
    //
    // (4) File periods are constant (except for partially written files). The current
    // implementation recognizes the first of two or more partially written files within
    // a file period but ignores the rest.
    //
    // (5) UTC offset is a correction factor that should be selected so that the parsed
    // date/time of a file points to the UTC date/time of the very first representation within
    // that file.
    //
    // (6) Only file URLs are supported

    /// <summary>
    /// Gets the root path of the database.
    /// </summary>
    protected string Root { get; private set; } = default!;

    /// <summary>
    /// Gets the data source context. This property is not accessible from within class constructors as it will bet set later.
    /// </summary>
    protected DataSourceContext Context { get; private set; } = default!;

    /// <summary>
    /// Gets the data logger. This property is not accessible from within class constructors as it will bet set later.
    /// </summary>
    protected ILogger Logger { get; private set; } = default!;

    private Func<string, Dictionary<string, IReadOnlyList<FileSource>>> FileSourceProvider { get; set; } = default!;

    #region Protected API as seen by subclass

    /// <summary>
    /// Invoked by Nexus right after construction.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the current operation.</param>
    /// <returns>The task.</returns>
    protected virtual Task InitializeAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// Gets the file source provider that provides information about the file structure within the database.
    /// </summary>
    /// <param name="cancellationToken">A token to cancel the current operation.</param>
    /// <returns>The task.</returns>
    protected abstract Task<Func<string, Dictionary<string, IReadOnlyList<FileSource>>>> GetFileSourceProviderAsync(
        CancellationToken cancellationToken);

    /// <summary>
    /// Gets the catalog registrations that are located under <paramref name="path"/>.
    /// </summary>
    /// <param name="path">The parent path for which to return catalog registrations.</param>
    /// <param name="cancellationToken">A token to cancel the current operation.</param>
    /// <returns>The catalog identifiers task.</returns>
    protected abstract Task<CatalogRegistration[]> GetCatalogRegistrationsAsync(
        string path,
        CancellationToken cancellationToken);

    // GetCatalogAsync:
    // It is not uncommon to have measurement data files with varying channel list over
    // time. This may be caused by updated logger configurations, etc. To support these
    // scenarios, it is easy use an own mechanism. The simplest solution for a data source
    // implementation would be to maintain a list of files that contains one entry per 
    // file version, e.g. [".../fileV1.dat", ".../fileV2.dat" ] or
    // [".../2020-01-01.dat", ".../2020-06-01.dat" ].

    /// <summary>
    /// Gets the requested <see cref="ResourceCatalog"/>.
    /// </summary>
    /// <param name="catalogId">The catalog identifier.</param>
    /// <param name="cancellationToken">A token to cancel the current operation.</param>
    /// <returns>The catalog request task.</returns>
    protected abstract Task<ResourceCatalog> GetCatalogAsync(
        string catalogId,
        CancellationToken cancellationToken);

    /// <summary>
    /// Gets the time range of the <see cref="ResourceCatalog"/>.
    /// </summary>
    /// <param name="catalogId">The catalog identifier.</param>
    /// <param name="cancellationToken">A token to cancel the current operation.</param>
    /// <returns>The time range task.</returns>
    protected virtual Task<(DateTime Begin, DateTime End)> GetTimeRangeAsync(
        string catalogId,
        CancellationToken cancellationToken)
    {
        return Task.Run(() =>
        {
            var minDateTime = DateTime.MaxValue;
            var maxDateTime = DateTime.MinValue;

            if (Directory.Exists(Root))
            {
                foreach (var (key, fileSourceGroup) in FileSourceProvider(catalogId))
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    ValidateFileSourceGroup(fileSourceGroup);

                    foreach (var fileSource in fileSourceGroup)
                    {
                        using var scope = Logger.BeginScope(fileSource);
                        Logger.LogDebug("Analyzing file source");

                        // first
                        var firstUtcDateTime = GetCandidateFiles(Root, DateTime.MinValue, DateTime.MinValue, fileSource, cancellationToken)
                            .Select(file => file.DateTimeOffset.UtcDateTime)
                            .OrderBy(current => current)
                            .FirstOrDefault();

                        if (firstUtcDateTime == default)
                            firstUtcDateTime = DateTime.MaxValue;

                        if (firstUtcDateTime < minDateTime)
                            minDateTime = firstUtcDateTime;

                        // last
                        var lastDateTimeOffset = GetCandidateFiles(Root, DateTime.MaxValue, DateTime.MaxValue, fileSource, cancellationToken)
                            .OrderByDescending(current => current.DateTimeOffset.DateTime)
                            .FirstOrDefault();

                        var lastDateTimeModified = fileSource.IrregularTimeInterval

                            ? lastDateTimeOffset.DateTimeOffset.DateTime
                                .Add(fileSource.FilePeriod)

                            : lastDateTimeOffset.DateTimeOffset.DateTime
                                .RoundDown(fileSource.FilePeriod)
                                .Add(fileSource.FilePeriod);

                        var lastUtcDateTime = new CustomDateTimeOffset(
                            lastDateTimeModified,
                            lastDateTimeOffset.DateTimeOffset.Offset
                        ).UtcDateTime;

                        if (lastUtcDateTime == default)
                            lastUtcDateTime = DateTime.MinValue;

                        if (lastUtcDateTime > maxDateTime)
                            maxDateTime = lastUtcDateTime;

                        Logger.LogDebug("Analyzing file source resulted in begin = {FirstDateTime} and end = {LastDateTime}", firstUtcDateTime, lastUtcDateTime);
                    }
                }
            }
            else
            {
                Logger.LogDebug("Folder {Root} does not exist, return default time range", Root);
            }

            return (minDateTime, maxDateTime);
        });
    }

    /// <summary>
    /// Gets the availability of the <see cref="ResourceCatalog"/>.
    /// </summary>
    /// <param name="catalogId">The catalog identifier</param>
    /// <param name="begin">The begin of the availability period.</param>
    /// <param name="end">The end of the availability period.</param>
    /// <param name="cancellationToken">A token to cancel the current operation.</param>
    /// <returns>The availability task.</returns>
    protected virtual Task<double> GetAvailabilityAsync(
        string catalogId,
        DateTime begin,
        DateTime end,
        CancellationToken cancellationToken)
    {
        if (begin >= end)
            throw new ArgumentException("The start time must be before the end time.");

        EnsureUtc(begin);
        EnsureUtc(end);

        // no true async file enumeration available: https://github.com/dotnet/runtime/issues/809
        return Task.Run(async () =>
        {
            var fileDateTimeCache = new HashSet<long>();

            double availability;

            if (Directory.Exists(Root))
            {
                var summedAvailability = 0.0;
                var fileSourceGroups = FileSourceProvider(catalogId);

                foreach (var (key, fileSourceGroup) in fileSourceGroups)
                {
                    ValidateFileSourceGroup(fileSourceGroup);

                    foreach (var fileSource in fileSourceGroup)
                    {
                        fileDateTimeCache.Clear();

                        using var scope = Logger.BeginScope(fileSource);
                        Logger.LogDebug("Analyzing file source");

                        cancellationToken.ThrowIfCancellationRequested();

                        var candidateFiles = GetCandidateFiles(Root, begin, end, fileSource, cancellationToken);

                        var files = candidateFiles
                            .Where(current => 
                                {
                                    var utcDateTime = current.DateTimeOffset.UtcDateTime;
                                    var isWithinRange = begin <= utcDateTime && utcDateTime < end;

                                    if (isWithinRange)
                                    {
                                        var roundedDateTime = current.DateTimeOffset.UtcDateTime
                                            .RoundDown(fileSource.FilePeriod);

                                        if (fileDateTimeCache.Contains(roundedDateTime.Ticks))
                                        {
                                            return false;
                                        }

                                        else
                                        {
                                            fileDateTimeCache.Add(roundedDateTime.Ticks);
                                            return true;
                                        }
                                    }

                                    else
                                    {
                                        return false;
                                    }
                                })
                            .DistinctBy(current => current.DateTimeOffset.UtcDateTime);

                        var availabilityTasks = files.Select(file =>
                        {
                            var availabilityTask = GetFileAvailabilityAsync(file.FilePath, cancellationToken);

                            _ = availabilityTask.ContinueWith(
                                x => Logger.LogDebug(availabilityTask.Exception, "Could not process file {FilePath}", file.FilePath),
                                TaskContinuationOptions.OnlyOnFaulted
                            );

                            return availabilityTask;
                        });

                        var availabilities = await Task.WhenAll(availabilityTasks);
                        var actual = availabilities.Sum();
                        var total = (end - begin).Ticks / (double)fileSource.FilePeriod.Ticks;

                        summedAvailability += actual / total;
                    }
                }

                availability = summedAvailability / fileSourceGroups.Count;
            }
            else
            {
                availability = 0.0;
                Logger.LogDebug("Folder {Root} does not exist, return default availabilit.", Root);
            }

            return availability;
        });
    }

    /// <summary>
    /// Returns the availability within a file.
    /// </summary>
    /// <param name="filePath">The file path.</param>
    /// <param name="cancellationToken">A token to cancel the current operation.</param>
    /// <returns>the availability within a file.</returns>
    protected virtual Task<double>
        GetFileAvailabilityAsync(string filePath, CancellationToken cancellationToken)
    {
        return Task.FromResult(1.0);
    }

    /// <summary>
    /// Reads a dataset.
    /// </summary>
    /// <param name="begin">The beginning of the period to read.</param>
    /// <param name="end">The end of the period to read.</param>
    /// <param name="requests">The array of read requests.</param>
    /// <param name="progress">An object to report the read progress between 0.0 and 1.0.</param>
    /// <param name="cancellationToken">A token to cancel the current operation.</param>
    /// <returns>The task.</returns>
    protected virtual async Task ReadAsync(
        DateTime begin,
        DateTime end,
        ReadRequest[] requests,
        IProgress<double> progress,
        CancellationToken cancellationToken)
    {
        var fileSourceGroupIndex = 0.0;

        // group by file source ID
        var fileSourceGroups = requests
            .GroupBy(request => request.CatalogItem.Resource.Properties!.GetStringValue(StructuredFileDataModelExtensions.FileSourceIdKey)!)
            .ToList();

        foreach (var group in fileSourceGroups)
        {
            var fileSourceId = group.Key;

            using var scope = Logger.BeginScope(new Dictionary<string, object>()
            {
                ["FileSourceId"] = fileSourceId
            });

            Logger.LogDebug("Read file source group");

            try
            {
                var firstCatalogItem = group.First().CatalogItem;
                var catalogId = firstCatalogItem.Catalog.Id;
                var samplePeriod = firstCatalogItem.Representation.SamplePeriod;
                var fileSourceGroup = FileSourceProvider(catalogId)[fileSourceId];
                var fileSourceCompensatedBegin = begin;

                ValidateFileSourceGroup(fileSourceGroup, samplePeriod);

                while (fileSourceCompensatedBegin < end)
                {
                    // get file source
                    var fileSource = fileSourceGroup.LastOrDefault(fileSource => fileSource.Begin <= fileSourceCompensatedBegin);

                    if (fileSource is null)
                    {
                        Logger.LogDebug("There is no file source available for the begin date/time ({Begin}) of the request", fileSourceCompensatedBegin);

                        fileSource = fileSourceGroup
                            .FirstOrDefault(fileSource => fileSourceCompensatedBegin <= fileSource.Begin && fileSource.Begin < end);

                        if (fileSource is null)
                        {
                            Logger.LogDebug("There is no file source available for the current period ({Begin} - {End})", fileSourceCompensatedBegin, end);
                            return;
                        }

                        else
                        {
                            fileSourceCompensatedBegin = DateTime.SpecifyKind(fileSource.Begin, DateTimeKind.Utc);
                        }
                    }

                    // get next file source
                    var nextFileSource = fileSourceGroup.FirstOrDefault(current => current.Begin > fileSourceCompensatedBegin);

                    var fileSourceEnd = nextFileSource is null
                        ? end
                        : new DateTime(Math.Min(end.Ticks, nextFileSource.Begin.Ticks), DateTimeKind.Utc);

                    // go!
                    var regularFileLength = fileSource.FilePeriod.Ticks / samplePeriod.Ticks;
                    var bufferOffset = (int)((fileSourceCompensatedBegin - begin).Ticks / samplePeriod.Ticks);
                    var currentBegin = fileSourceCompensatedBegin;
                    var totalPeriod = fileSourceEnd - fileSourceCompensatedBegin;
                    var consumedPeriod = TimeSpan.Zero;
                    var remainingPeriod = totalPeriod;

                    while (consumedPeriod < totalPeriod)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        // get regular file begin and paths
                        var (regularUtcFileBegin, fileInfos) = await FindFileBeginAndPathsAsync(currentBegin, fileSource);

                        var consumedFilePeriod = currentBegin - regularUtcFileBegin;
                        var remainingFilePeriod = fileSource.FilePeriod - consumedFilePeriod;
                        var currentPeriod = TimeSpan.FromTicks(Math.Min(remainingFilePeriod.Ticks, remainingPeriod.Ticks));
                        
                        Logger.LogTrace("Process period {CurrentBegin} to {CurrentEnd}", currentBegin, currentBegin + currentPeriod);

                        var fileBlock = (int)(currentPeriod.Ticks / samplePeriod.Ticks);
                        var fileOffset = consumedFilePeriod.Ticks / samplePeriod.Ticks;

                        foreach (var (filePath, fileBeginOffset) in fileInfos)
                        {
                            if (File.Exists(filePath))
                            {
                                // compensate offsets and lengths in case of incomplete or irregular file
                                var fileCompensation = (int)(fileBeginOffset.Ticks / samplePeriod.Ticks);

                                var actualBufferOffset = bufferOffset +
                                    (
                                        fileCompensation < 0 /* = irregular file */

                                            /* The irregular file has data for the current buffer position, no action required */
                                            ? 0

                                            /* The irregular or incomplete file contains no data for the current buffer position, so compensate for it */
                                            : + fileCompensation
                                    );

                                /* The irregular or incomplete file contains not enough data, so make the file block smaller */
                                var actualFileBlock = fileBlock - Math.Abs(fileCompensation);

                                /* Irregular or incomplete file: Compensate the file offset */
                                var actualFileOffset = fileOffset +
                                    (
                                        fileCompensation < 0 /* = irregular file */

                                            /* The irregular file starts earlier than expected, so compensate for it */
                                            ? - fileCompensation

                                            /* The irregular or incomplete file starts later than expected */
                                            : - fileCompensation
                                    );

                                /* The maximum value for fileCompensation is MaxFileBlock = FilePeriod / SamplePeriod
                                 * so there is no need to check for actualFileOffset >= MaxFileBlock.
                                 * However, it might happen that actualFileOffset < 0. This must be compensated. */
                                if (actualFileOffset < 0)
                                    actualFileOffset = 0;

                                Logger.LogTrace("Process file {FilePath}", filePath);

                                try
                                {
                                    var readRequests = group.Select(request =>
                                    {
                                        var catalogItem = request.CatalogItem;
                                        var representation = catalogItem.Representation;

                                        var slicedData = request.Data
                                            .Slice(
                                                start: actualBufferOffset * representation.ElementSize, 
                                                length: actualFileBlock * representation.ElementSize
                                            );

                                        var slicedStatus = request.Status
                                            .Slice(
                                                start: actualBufferOffset, 
                                                length: actualFileBlock
                                            );

                                        var originalName = catalogItem.Resource.Properties?
                                            .GetStringValue(StructuredFileDataModelExtensions.OriginalNameKey)!;

                                        return new StructuredFileReadRequest(
                                            CatalogItem: request.CatalogItem,
                                            Data: slicedData,
                                            Status: slicedStatus,
                                            OriginalName: originalName
                                        );
                                    }).ToArray();

                                    var readInfo = new ReadInfo(
                                        filePath,
                                        fileSource,
                                        regularUtcFileBegin,
                                        actualFileOffset,
                                        actualFileBlock,
                                        regularFileLength
                                    );

                                    await ReadAsync(readInfo, readRequests, cancellationToken);
                                }
                                catch (OutOfMemoryException)
                                {
                                    throw;
                                }
                                catch (Exception ex)
                                {
                                    Logger.LogDebug(ex, "Could not process file {FilePath}", filePath);
                                }
                            }
                            else
                            {
                                Logger.LogDebug("File {FilePath} does not exist", filePath);
                            }
                        }

                        // update loop state
                        bufferOffset += fileBlock;
                        currentBegin += currentPeriod;
                        consumedPeriod += currentPeriod;
                        remainingPeriod -= currentPeriod;
                    }

                    fileSourceCompensatedBegin += totalPeriod;

                    progress.Report(
                        value: (
                            fileSourceGroupIndex +
                            (fileSourceCompensatedBegin - begin).Ticks / (double)(end - begin).Ticks
                        ) / fileSourceGroups.Count
                    );
                }
            }
            catch (OutOfMemoryException)
            {
                throw;
            }
            catch (Exception ex)
            {
                Logger.LogError(ex, "Could not read file source group");
            }

            ++fileSourceGroupIndex;
        }
    }

    /// <summary>
    /// Reads a dataset from the provided file.
    /// </summary>
    /// <param name="info">The common read information.</param>
    /// <param name="readRequests">The array of read requests.</param>
    /// <param name="cancellationToken">A token to cancel the current operation.</param>
    /// <returns>The task.</returns>
    protected abstract Task ReadAsync(
        ReadInfo info,
        StructuredFileReadRequest[] readRequests,
        CancellationToken cancellationToken);

    private protected Task<(DateTime RegularUtcFileBegin, IEnumerable<(string FilePath, TimeSpan FileBeginOffset)>)> 
        FindFileBeginAndPathsAsync(DateTime begin, FileSource fileSource)
    {
        /* This implementation assumes that files are stored in regular time intervals.
         * The files can start late, but MUST end no later than the regular time interval.
         * 
         * There are some cases where two or more files are expected for a given `begin`:
         *
         * 1) A measurement is stopped and restarted frequently and each time a new data file
         *    with the current date is created.
         *
         * 2) A data logger creates versioned files with a granularity of e.g. 1 file per day.
         *    When the version changes, the logger creates a new file with same name but new
         *    version. This could look like this:
         *    2020-01-01T00-00-00Z_v1.dat (contains data from midnight to time t0)
         *    2020-01-01T00-00-00Z_v2.dat (contains data from time t0 + x to next midnight)
         *    Where x is the time period the system was offline to apply the new version.
         */
        var localBegin = begin.Kind == DateTimeKind.Utc
            ? DateTime.SpecifyKind(begin.Add(fileSource.UtcOffset), DateTimeKind.Local)
            : throw new ArgumentException("The begin parameter must of kind UTC.");

        var localFileBegin = localBegin.RoundDown(fileSource.FilePeriod);

        var folderNames = fileSource
            .PathSegments
            .Select(localFileBegin.ToString);

        var folderNameArray = new List<string>() { Root }
            .Concat(folderNames)
            .ToArray();   

        var regularUtcFileBegin = new CustomDateTimeOffset
        (
            DateTime.SpecifyKind(localFileBegin, DateTimeKind.Unspecified),
            fileSource.UtcOffset
        ).UtcDateTime;

        IEnumerable<(string FilePath, TimeSpan FileBeginOffset)> fileInfos;

        if (fileSource.FileTemplate.Contains('?') || fileSource.FileTemplate.Contains('*'))
        {
            var actualUtcFileBegin = fileSource.IrregularTimeInterval
                ? begin - fileSource.FilePeriod
                : regularUtcFileBegin;

            var regularUtcFileEnd = regularUtcFileBegin + fileSource.FilePeriod;

            fileInfos = GetCandidateFiles(
                rootPath: Root,
                begin: regularUtcFileBegin,
                end: regularUtcFileBegin + fileSource.FilePeriod,
                fileSource,
                CancellationToken.None
            )
            .Where(current =>
            {
                if (fileSource.IrregularTimeInterval)
                {
                    return
                        actualUtcFileBegin < current.DateTimeOffset.UtcDateTime &&
                        current.DateTimeOffset.UtcDateTime < regularUtcFileEnd;
                }

                else
                {
                    return
                        actualUtcFileBegin <= current.DateTimeOffset.UtcDateTime &&
                        current.DateTimeOffset.UtcDateTime < regularUtcFileEnd;
                }
            })
            .Select(current => (current.FilePath, current.DateTimeOffset.UtcDateTime - regularUtcFileBegin));
        }

        else
        {
            var folderPath = Path.Combine(folderNameArray);
            var fileName = localFileBegin.ToString(fileSource.FileTemplate);

            fileInfos = [(Path.Combine(folderPath, fileName), TimeSpan.Zero)];
        }

        return Task.FromResult((regularUtcFileBegin, fileInfos));
    }

    /// <summary>
    /// Tries to find the first file for a given <see cref="FileSource"/>. 
    /// </summary>
    /// <param name="fileSource">The file source for which to find the first file.</param>
    /// <param name="filePath">The found file path.</param>
    /// <returns>True when a file was found, false otherwise.</returns>
    protected bool TryGetFirstFile(FileSource fileSource, [NotNullWhen(true)] out string? filePath)
    {
        filePath = GetCandidateFiles(
                    Root,
                    DateTime.MinValue,
                    DateTime.MinValue,
                    fileSource,
                    CancellationToken.None)
            .OrderBy(file => file.DateTimeOffset.UtcDateTime)
            .Select(file => file.FilePath)
            .FirstOrDefault();

        return filePath is not null;
    }

    #endregion

    #region Public API as seen by Nexus and unit tests

    async Task IDataSource.SetContextAsync(
        DataSourceContext context,
        ILogger logger,
        CancellationToken cancellationToken)
    {
        if (context.ResourceLocator is null)
            throw new Exception("The resource locator parameter is required.");

        Root = context.ResourceLocator.ToPath();
        Context = context;
        Logger = logger;

        await InitializeAsync(cancellationToken);
        FileSourceProvider = await GetFileSourceProviderAsync(cancellationToken);
    }

    Task<CatalogRegistration[]> IDataSource.GetCatalogRegistrationsAsync(
        string path,
        CancellationToken cancellationToken)
    {
        return GetCatalogRegistrationsAsync(path, cancellationToken);
    }

    async Task<ResourceCatalog> IDataSource.GetCatalogAsync(
        string catalogId,
        CancellationToken cancellationToken)
    {
        var catalog = await GetCatalogAsync(catalogId, cancellationToken);

        if (catalog.Resources is not null)
        {
            foreach (var resource in catalog.Resources)
            {
                // ensure file source id
                var fileSourceId = resource.Properties?.GetStringValue(StructuredFileDataModelExtensions.FileSourceIdKey);

                if (string.IsNullOrWhiteSpace(fileSourceId))
                    throw new Exception($"The resource {resource.Id} is missing the file source property.");

                // ensure original name
                var originalName = resource.Properties?.GetStringValue(StructuredFileDataModelExtensions.OriginalNameKey);

                if (string.IsNullOrWhiteSpace(originalName))
                    throw new Exception($"The resource {resource.Id} is missing the original name property.");
            }
        }

        return catalog;
    }

    Task<(DateTime Begin, DateTime End)> IDataSource.GetTimeRangeAsync(
        string catalogId,
        CancellationToken cancellationToken)
    {
        return GetTimeRangeAsync(catalogId, cancellationToken);
    }

    Task<double> IDataSource.GetAvailabilityAsync(
        string catalogId,
        DateTime begin,
        DateTime end,
        CancellationToken cancellationToken)
    {
        return GetAvailabilityAsync(catalogId, begin, end, cancellationToken);
    }

    async Task IDataSource.ReadAsync(
        DateTime begin,
        DateTime end,
        ReadRequest[] requests,
        ReadDataHandler readData,
        IProgress<double> progress,
        CancellationToken cancellationToken)
    {
        if (begin >= end)
            throw new ArgumentException("The start time must be before the end time.");

        EnsureUtc(begin);
        EnsureUtc(end);

        Logger.LogDebug("Read catalog items");

        try
        {
            await ReadAsync(begin, end, requests, progress, cancellationToken);
        }
        catch (OutOfMemoryException)
        {
            throw;
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Could not read catalog items");
        }
    }

    #endregion

    #region Helpers

    private static IEnumerable<(string FilePath, CustomDateTimeOffset DateTimeOffset)> GetCandidateFiles(
        string rootPath,
        DateTime begin,
        DateTime end,
        FileSource fileSource,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // initial check
        if (!Directory.Exists(rootPath))
            return Array.Empty<(string, CustomDateTimeOffset)>();

        // get all candidate folders
        var candidateFolders = fileSource.PathSegments.Length >= 1

            ? GetCandidateFolders(
                rootPath,
                default,
                begin,
                end,
                fileSource,
                fileSource.PathSegments,
                cancellationToken)

            : new List<(string, CustomDateTimeOffset)>() { (rootPath, default) };

        var folders = candidateFolders.ToList();

        return candidateFolders.SelectMany(currentFolder =>
        {
            var filePaths = Directory.EnumerateFiles(currentFolder.FolderPath);

            var candidateFiles = filePaths
                .Select(filePath =>
                {
                    var success = TryGetFileBeginByPath(
                        filePath,
                        fileSource,
                        out var fileBegin,
                        folderBegin: currentFolder.DateTime
                    );

                    return (success, filePath, fileBegin);
                })
                .Where(current => current.success)
                .Select(current => (current.filePath, current.fileBegin));

            return candidateFiles;
        });
    }

    private static IEnumerable<(string FolderPath, CustomDateTimeOffset DateTime)> GetCandidateFolders(
        string root,
        CustomDateTimeOffset rootDate,
        DateTime begin,
        DateTime end,
        FileSource fileSource,
        string[] pathSegments,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        // Take into account folders with data from previous day or from subsequent day
        // (depending on the UTC offset sign). But only when begin and end are not special
        // values.
        if (end != default && end != DateTime.MaxValue && fileSource.UtcOffset > TimeSpan.Zero)
            end += fileSource.UtcOffset;

        else if (begin != default && begin != DateTime.MaxValue && fileSource.UtcOffset < TimeSpan.Zero)
            begin -= fileSource.UtcOffset;

        // get all available folders
        var folderPaths = Directory
            .EnumerateDirectories(root)
            .ToList();

        // get all folders that can be parsed
        var hasDateTimeInformation = false;

        var folderNameToDateTimeMap = folderPaths
            .Select(folderPath =>
            {
                var folderName = Path.GetFileName(folderPath);

                var success = CustomDateTimeOffset.TryParseExact(
                    folderName,
                    pathSegments.First(),
                    fileSource.UtcOffset,
                    out var parsedDateTime
                );

                if (parsedDateTime.UtcDateTime == default)
                    parsedDateTime = rootDate;

                else
                    hasDateTimeInformation = true;

                return (folderPath, parsedDateTime);
            })
           .ToDictionary(current => current.folderPath, current => current.parsedDateTime);

        // keep only folders that fall within the wanted time range

        /* The expected segment name is used for two purposes:
         * (1) for "filter by search date" where only the begin date/time matters
         * (2) for "filter by exact match" where any date/time can be put into
         * the ToString() method to remove the quotation marks from the path segment
         */
        var expectedSegmentName = begin.ToString(pathSegments.First());

        var folderCandidates = hasDateTimeInformation

            // filter by search date
            ? FilterBySearchDate(begin, end, folderNameToDateTimeMap, expectedSegmentName)

            // filter by exact match
            : folderNameToDateTimeMap
                .Where(entry => Path.GetFileName(entry.Key) == expectedSegmentName)
                .Select(entry => (entry.Key, entry.Value));

        // go deeper
        if (pathSegments.Length > 1)
        {
            return folderCandidates.SelectMany(current =>
                GetCandidateFolders(
                    current.Key,
                    current.Value,
                    begin,
                    end,
                    fileSource,
                    pathSegments.Skip(1).ToArray(),
                    cancellationToken
                )
            );
        }

        // we have reached the most nested folder level
        else
        {
            return folderCandidates;
        }
    }

    private static IEnumerable<(string Key, CustomDateTimeOffset Value)> FilterBySearchDate(
        DateTime begin,
        DateTime end,
        Dictionary<string, CustomDateTimeOffset> folderNameToDateTimeMap,
        string expectedSegmentName)
    {
        if (begin == DateTime.MinValue && end == DateTime.MinValue)
        {
            var folderCandidate = folderNameToDateTimeMap
                .OrderBy(entry => entry.Value.UtcDateTime)
                .FirstOrDefault();

            return new List<(string, CustomDateTimeOffset)>() { (folderCandidate.Key, folderCandidate.Value) };
        }

        else if (begin == DateTime.MaxValue && end == DateTime.MaxValue)
        {
            var folderCandidate = folderNameToDateTimeMap
               .OrderByDescending(entry => entry.Value.UtcDateTime)
               .FirstOrDefault();

            return new List<(string, CustomDateTimeOffset)>() { (folderCandidate.Key, folderCandidate.Value) };
        }

        else
        {
            return folderNameToDateTimeMap
                .Where(entry =>
                {
                    // Check for the case that the parsed date/time
                    // (1) is more specific (2020-01-01T22) than the search time range (2020-01-01T00 - 2021-01-02T00)
                    // (2) is less specific but in-between (2020-02) the search time range (2020-01-01 - 2021-03-01)
                    if (begin <= entry.Value.UtcDateTime && entry.Value.UtcDateTime < end)
                        return true;

                    // Check for the case that the parsed date/time
                    // (1) is less specific (2020-01) and outside the search time range (2020-01-02 - 2020-01-03)
                    else
                        return Path.GetFileName(entry.Key) == expectedSegmentName;
                })
                .Select(entry => (entry.Key, entry.Value));
        }
    }

    internal static bool TryGetFileBeginByPath(
        string filePath,
        FileSource fileSource,
        out CustomDateTimeOffset fileBegin,
        CustomDateTimeOffset folderBegin = default)
    {
        var fileName = Path.GetFileName(filePath);
        bool isSuccess;

        if (TryGetFileBeginByName_AnyKind(fileName, fileSource, out fileBegin))
        {
            // When TryGetFileBeginByName_AnyKind == true, then the input string was parsed successfully and the
            // result contains date/time information of either kind: date+time, time-only, default.

            // date+time: use file date/time
            if (fileBegin.DateTime.Date != default)
            {
                isSuccess = true;
            }

            // time-only: use combined folder and file date/time
            else if (fileBegin != default)
            {
                // short cut
                if (folderBegin != default)
                {
                    fileBegin = new CustomDateTimeOffset(
                        new DateTime(folderBegin.DateTime.Date.Ticks + fileBegin.DateTime.TimeOfDay.Ticks),
                        fileBegin.Offset);

                    isSuccess = true;
                }

                // long way
                else
                {
                    folderBegin = GetFolderBegin_AnyKind(filePath, fileSource);

                    fileBegin = new CustomDateTimeOffset(
                        new DateTime(folderBegin.DateTime.Ticks + fileBegin.DateTime.TimeOfDay.Ticks),
                        fileBegin.Offset);

                    isSuccess = folderBegin != default;
                }
            }

            // default: use folder date/time
            else
            {
                // short cut
                if (folderBegin != default)
                {
                    fileBegin = folderBegin;
                    isSuccess = true;
                }

                // long way
                else
                {
                    folderBegin = GetFolderBegin_AnyKind(filePath, fileSource);

                    fileBegin = folderBegin;
                    isSuccess = folderBegin != default;
                }
            }
        }

        // no date + no time: failed
        else
        {
            isSuccess = false;
        }

        return isSuccess;
    }

    private static CustomDateTimeOffset GetFolderBegin_AnyKind(string filePath, FileSource fileSource)
    {
        var folderBegin = default(CustomDateTimeOffset);

        var pathSegments = filePath
            .Split('/', '\\');

        pathSegments = pathSegments
            .Skip(pathSegments.Length - fileSource.PathSegments.Length - 1)
            .Take(fileSource.PathSegments.Length)
            .ToArray();

        for (int i = 0; i < pathSegments.Length; i++)
        {
            var folderName = pathSegments[i];
            var folderTemplate = fileSource.PathSegments[i];

            var _ = CustomDateTimeOffset.TryParseExact(
                folderName,
                folderTemplate,
                fileSource.UtcOffset,
                out var currentFolderBegin
            );

            if (currentFolderBegin.UtcDateTime > folderBegin.UtcDateTime)
                folderBegin = currentFolderBegin;
        }

        return folderBegin;
    }

    private static bool TryGetFileBeginByName_AnyKind(
        string fileName,
        FileSource fileSource,
        out CustomDateTimeOffset fileBegin)
    {
        /* (1) Regex is required in scenarios when there are more complex
         * file names, i.e. file names containing an opaque string that
         * changes for every file. This could be a counter, a serial
         * number or some other unpredictable proprietary string.
         *
         * (2) It is also required as a filter if there is more than one
         * file type in the containing folder, e.g. high frequent and
         * averaged data files that are being treated as different sources.
         */

        var fileTemplate = fileSource.FileTemplate;

        if (!string.IsNullOrWhiteSpace(fileSource.FileDateTimePreselector))
        {
            if (string.IsNullOrEmpty(fileSource.FileDateTimeSelector))
                throw new Exception("When a file date/time preselector is provided, the selector itself must be provided too.");

            fileTemplate = fileSource.FileDateTimeSelector;
            var regex = new Regex(fileSource.FileDateTimePreselector);

            fileName = string.Join("", regex
                .Match(fileName)
                .Groups
                .Cast<Group>()
                .Skip(1)
                .Select(match => match.Value)
            );
        }

        var success = CustomDateTimeOffset.TryParseExact(
            fileName,
            fileTemplate,
            fileSource.UtcOffset,
            out fileBegin
        );

        return success;
    }

    private static void EnsureUtc(DateTime dateTime)
    {
        if (dateTime.Kind != DateTimeKind.Utc)
            throw new ArgumentException("UTC date/times are required.");
    }

    private static void ValidateFileSourceGroup(
        IReadOnlyList<FileSource> fileSourceGroup,
        TimeSpan? samplePeriod = default)
    {
        // Are there any file sources?
        if (!fileSourceGroup.Any())
            throw new Exception("The list of file sources must not be empty.");

        // Short-cut for single file source
        if (fileSourceGroup.Count == 1)
            return;

        // Are Begin parameters strictly monotonic increasing?
        var current = fileSourceGroup[0].Begin;

        for (int i = 1; i < fileSourceGroup.Count - 1; i++)
        {
            var next = fileSourceGroup[i].Begin;

            if (next <= current)
                throw new Exception("The file sources begin property must be strictly monotonic increasing.");

            current = next;
        }

        // Is begin a multiple of the sample period?
        if (samplePeriod.HasValue)
        {
            foreach (var fileSource in fileSourceGroup)
            {
                if (fileSource.Begin.Ticks % samplePeriod.Value.Ticks != 0)
                    throw new Exception("The file source begin parameter must be a multiple of the sample period.");
            }
        }
    }

    #endregion
}
