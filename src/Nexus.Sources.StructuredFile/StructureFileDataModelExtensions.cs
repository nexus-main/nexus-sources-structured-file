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
    /// Adds a file source id map property that maps representation IDs to file source IDs.
    /// </summary>
    /// <param name="resourceBuilder">The resource builder.</param>
    /// <param name="fileSourceIdMap">The map of representation IDs to file source IDs.</param>
    /// <returns>A resource catalog builder.</returns>
    public static ResourceBuilder WithFileSourceIdMap(this ResourceBuilder resourceBuilder, Dictionary<string, string> fileSourceIdMap)
    {
        return resourceBuilder.WithProperty(FileSourceIdMapKey, fileSourceIdMap);
    }

    #endregion
}
