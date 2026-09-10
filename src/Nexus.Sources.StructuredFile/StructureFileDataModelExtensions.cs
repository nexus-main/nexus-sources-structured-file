using System.Runtime.CompilerServices;

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

    private static readonly ConditionalWeakTable<ResourceBuilder, Dictionary<string, string>> _fileSourceIdMaps = new();

    /// <summary>
    /// Adds a <see cref="Representation"/> and associates it with a file source ID.
    /// </summary>
    /// <param name="resourceBuilder">The resource builder.</param>
    /// <param name="representation">The representation to add.</param>
    /// <param name="fileSourceId">The id of the file source to associate with this representation.</param>
    /// <returns>The resource builder.</returns>
    public static ResourceBuilder AddRepresentation(this ResourceBuilder resourceBuilder, Representation representation, string fileSourceId)
    {
        var map = _fileSourceIdMaps.GetOrCreateValue(resourceBuilder);
        map[representation.Id] = fileSourceId;

        return resourceBuilder
            .AddRepresentation(representation)
            .WithProperty(FileSourceIdMapKey, map);
    }

    #endregion
}
