
namespace Xtraq.SnapshotBuilder.Metadata;

/// <summary>
/// Interface for resolving function schemas from the database.
/// </summary>
public interface IFunctionSchemaResolver
{
    /// <summary>
    /// Resolves the schema of a function by name.
    /// </summary>
    /// <param name="functionName">The name of the function to resolve.</param>
    /// <param name="defaultSchema">The default schema to use if the function is not found in other schemas.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The schema name if found, otherwise null.</returns>
    Task<string?> ResolveSchemaAsync(string functionName, string? defaultSchema = null, CancellationToken cancellationToken = default);
}
