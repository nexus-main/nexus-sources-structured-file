namespace Nexus.DataModel
{
    /// <summary>
    /// Contains extension methods to make life easier working with the data model types.
    /// </summary>
    public static class StructureFileDataModelExtensions
    {
        #region Fluent API

        /// <summary>
        /// A constant with the key for a file source property.
        /// </summary>
        public const string FileSourceIdKey = "file-source-id";

        /// <summary>
        /// A constant with the key for a original name property.
        /// </summary>
        public const string OriginalNameKey = "original-name";

        /// <summary>
        /// Adds a file source id property.
        /// </summary>
        /// <param name="resourceBuilder">The resource builder.</param>
        /// <param name="fileSourceId">The id of the file source to add.</param>
        /// <returns>A resource catalog builder.</returns>
        public static ResourceBuilder WithFileSourceId(this ResourceBuilder resourceBuilder, string fileSourceId)
        {
            return resourceBuilder.WithProperty(FileSourceIdKey, fileSourceId);
        }

        /// <summary>
        /// Adds an original name property.
        /// </summary>
        /// <param name="resourceBuilder">The resource builder.</param>
        /// <param name="originalName">The original name to add.</param>
        /// <returns>A resource catalog builder.</returns>
        public static ResourceBuilder WithOriginalName(this ResourceBuilder resourceBuilder, string originalName)
        {
            return resourceBuilder.WithProperty(OriginalNameKey, originalName);
        }

        #endregion
    }
}
