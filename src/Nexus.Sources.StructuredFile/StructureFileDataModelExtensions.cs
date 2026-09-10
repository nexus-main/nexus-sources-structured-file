namespace Nexus.DataModel;

/// <summary>
/// Contains extension methods to make life easier working with the data model types.
/// </summary>
public static class StructuredFileDataModelExtensions
{
    #region Fluent API

    /// <summary>
    /// A constant with the key for a file source id map property.
    /// </summary>
    public const string FileSourceIdMapKey = "file-source-id-map";

    /// <summary>
    /// Adds the provided representations and associates them with file source IDs.
    /// </summary>
    /// <param name="resourceBuilder">The resource builder.</param>
    /// <param name="fileSourceIdsByRepresentation">The file source IDs keyed by representation.</param>
    /// <returns>The resource builder.</returns>
    public static ResourceBuilder AddRepresentations(
        this ResourceBuilder resourceBuilder,
        IReadOnlyDictionary<Representation, string> fileSourceIdsByRepresentation
    )
    {
        var fileSourceIdMap = fileSourceIdsByRepresentation.ToDictionary(
            entry => entry.Key.Id,
            entry => entry.Value
        );

        return resourceBuilder
            .WithProperty(FileSourceIdMapKey, fileSourceIdMap)
            .AddRepresentations(fileSourceIdsByRepresentation.Keys);
    }

    #endregion
}
