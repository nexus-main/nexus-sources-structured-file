namespace Nexus.DataModel;

/// <summary>
/// Contains extension methods to make life easier working with the data model types.
/// </summary>
public static class StructuredFileDataModelExtensions
{
    #region Fluent API

    /// <summary>
    /// A constant with the key prefix for file source id properties.
    /// </summary>
    public const string FileSourceIdKey = "file-source-id";

    /// <summary>
    /// Adds a <see cref="Representation"/> and associates it with a file source ID.
    /// </summary>
    /// <param name="resourceBuilder">The resource builder.</param>
    /// <param name="representation">The representation to add.</param>
    /// <param name="fileSourceId">The id of the file source to associate with this representation.</param>
    /// <returns>The resource builder.</returns>
    public static ResourceBuilder AddRepresentation(this ResourceBuilder resourceBuilder, Representation representation, string fileSourceId)
    {
        return resourceBuilder
            .AddRepresentation(representation)
            .WithProperty($"{FileSourceIdKey}:{representation.Id}", fileSourceId);
    }

    #endregion
}
