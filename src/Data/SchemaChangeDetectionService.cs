using System.Data;
using Microsoft.Data.SqlClient;
using Xtraq.Cache;
using Xtraq.Services;

namespace Xtraq.Data;

/// <summary>
/// Service for detecting schema changes using SQL Server system metadata.
/// Provides delta queries to only fetch objects that have been modified since last cache update.
/// </summary>
internal interface ISchemaChangeDetectionService
{
    /// <summary>
    /// Initialize the service with a connection string.
    /// </summary>
    void Initialize(string connectionString);

    /// <summary>
    /// Get all objects of a specific type that have been modified since the specified timestamp.
    /// If sinceUtc is null, returns all objects.
    /// </summary>
    Task<IReadOnlyList<SchemaObjectMetadata>> GetModifiedObjectsAsync(
        SchemaObjectType objectType,
        DateTime? sinceUtc = null,
        IReadOnlyList<string>? schemaFilter = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get dependency information for a specific object.
    /// Returns objects that the specified object depends on.
    /// </summary>
    Task<IReadOnlyList<SchemaObjectRef>> GetDependenciesAsync(
        SchemaObjectRef objectRef,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Get the maximum modification timestamp across all schema objects.
    /// Used to determine the reference timestamp for the next delta query.
    /// </summary>
    Task<DateTime> GetMaxModificationTimeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Flush the object index (object-index.json) to disk if there are pending updates.
    /// </summary>
    Task FlushIndexAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Metadata for a schema object from SQL Server system tables.
/// </summary>
internal sealed class SchemaObjectMetadata
{
    public SchemaObjectType Type { get; set; }
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public DateTime ModifiedUtc { get; set; }
    public int ObjectId { get; set; }
    public string? Definition { get; set; }
    public bool IsSystemObject { get; set; }
}

internal sealed class SchemaChangeDetectionService : ISchemaChangeDetectionService
{
    private readonly IConsoleService _console;
    private readonly ISchemaObjectIndexManager _indexManager;
    private string _connectionString = string.Empty;
    private readonly SemaphoreSlim _indexInitSemaphore = new(1, 1);
    private bool _indexInitialized;

    public SchemaChangeDetectionService(IConsoleService console, ISchemaObjectIndexManager indexManager)
    {
        _console = console ?? throw new ArgumentNullException(nameof(console));
        _indexManager = indexManager ?? throw new ArgumentNullException(nameof(indexManager));
    }

    public void Initialize(string connectionString)
    {
        _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
    }

    public async Task<IReadOnlyList<SchemaObjectMetadata>> GetModifiedObjectsAsync(
        SchemaObjectType objectType,
        DateTime? sinceUtc = null,
        IReadOnlyList<string>? schemaFilter = null,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(_connectionString))
        {
            throw new InvalidOperationException("Service not initialized. Call Initialize() first.");
        }

        await EnsureIndexInitializedAsync(cancellationToken).ConfigureAwait(false);

        var existingIndex = _indexManager.GetEntries(objectType);
        var indexAvailable = existingIndex.Count > 0;
        var performFullScan = !sinceUtc.HasValue || !indexAvailable;

        var query = BuildObjectQuery(objectType, performFullScan ? null : sinceUtc, schemaFilter, out var parameters);
        var results = new List<SchemaObjectMetadata>();

        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            using var command = new SqlCommand(query, connection);
            foreach (var parameter in parameters)
            {
                command.Parameters.Add(parameter);
            }

            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var metadata = new SchemaObjectMetadata
                {
                    Type = objectType,
                    Schema = reader.GetString("schema_name"),
                    Name = reader.GetString("object_name"),
                    ModifiedUtc = reader.GetDateTime("modify_date").ToUniversalTime(),
                    ObjectId = reader.GetInt32("object_id"),
                    IsSystemObject = reader.GetBoolean("is_ms_shipped")
                };

                // Get definition for programmable objects
                try
                {
                    var definitionOrdinal = reader.GetOrdinal("definition");
                    if (!reader.IsDBNull(definitionOrdinal))
                    {
                        metadata.Definition = reader.GetString(definitionOrdinal);
                    }
                }
                catch (IndexOutOfRangeException)
                {
                    // Column doesn't exist, which is fine for some object types
                }

                results.Add(metadata);
            }
        }
        catch (Exception ex)
        {
            _console.Verbose($"[schema-detection] Failed to query {objectType}: {ex.Message}");
            throw;
        }

        var updatedEntries = results
            .Select(static metadata => new SchemaObjectIndexEntry
            {
                Type = metadata.Type,
                Schema = metadata.Schema,
                Name = metadata.Name,
                LastModifiedUtc = metadata.ModifiedUtc
            })
            .ToList();

        if (performFullScan)
        {
            _indexManager.ReplaceEntries(objectType, updatedEntries);

            if (indexAvailable)
            {
                // Compare with previous snapshot to determine actual deltas.
                var previousMap = existingIndex.ToDictionary(
                    entry => BuildKey(entry.Schema, entry.Name),
                    entry => entry,
                    StringComparer.OrdinalIgnoreCase);
                var modified = new List<SchemaObjectMetadata>();
                var currentMap = updatedEntries.ToDictionary(
                    entry => BuildKey(entry.Schema, entry.Name),
                    entry => entry,
                    StringComparer.OrdinalIgnoreCase);

                foreach (var metadata in results)
                {
                    var key = BuildKey(metadata.Schema, metadata.Name);
                    if (!previousMap.TryGetValue(key, out var previous) || previous.LastModifiedUtc != metadata.ModifiedUtc)
                    {
                        modified.Add(metadata);
                    }
                }

                var removed = previousMap.Keys.Except(currentMap.Keys, StringComparer.OrdinalIgnoreCase).ToList();
                if (removed.Count > 0)
                {
                    foreach (var key in removed)
                    {
                        _console.Verbose($"[schema-detection] {objectType} removed from catalog: {key}");
                    }
                }

                results = modified;
            }
        }
        else
        {
            _indexManager.UpsertEntries(updatedEntries);
        }

        _console.Verbose($"[schema-detection] Found {results.Count} {objectType} objects" +
                        (performFullScan ? " (full scan)" : sinceUtc.HasValue ? $" modified since {sinceUtc}" : ""));

        return results;
    }

    public async Task<IReadOnlyList<SchemaObjectRef>> GetDependenciesAsync(
        SchemaObjectRef objectRef,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(_connectionString))
        {
            throw new InvalidOperationException("Service not initialized. Call Initialize() first.");
        }

        const string query = @"
            SELECT DISTINCT
                ref_schema.name AS ref_schema_name,
                ref_obj.name AS ref_object_name,
                ref_obj.type_desc AS ref_object_type
            FROM sys.sql_expression_dependencies sed
            INNER JOIN sys.objects obj ON sed.referencing_id = obj.object_id
            INNER JOIN sys.schemas src_schema ON obj.schema_id = src_schema.schema_id
            INNER JOIN sys.objects ref_obj ON sed.referenced_id = ref_obj.object_id
            INNER JOIN sys.schemas ref_schema ON ref_obj.schema_id = ref_schema.schema_id
            WHERE src_schema.name = @Schema 
              AND obj.name = @Name
              AND ref_obj.is_ms_shipped = 0
            ORDER BY ref_schema_name, ref_object_name";

        var results = new List<SchemaObjectRef>();

        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            using var command = new SqlCommand(query, connection);
            command.Parameters.AddWithValue("@Schema", objectRef.Schema);
            command.Parameters.AddWithValue("@Name", objectRef.Name);

            using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
            {
                var refSchema = reader.GetString("ref_schema_name");
                var refName = reader.GetString("ref_object_name");
                var refTypeDesc = reader.GetString("ref_object_type");

                if (TryMapSqlObjectType(refTypeDesc, out var objectType))
                {
                    results.Add(new SchemaObjectRef(objectType, refSchema, refName));
                }
            }
        }
        catch (Exception ex)
        {
            _console.Verbose($"[schema-detection] Failed to get dependencies for {objectRef}: {ex.Message}");
        }

        return results;
    }

    public async Task<DateTime> GetMaxModificationTimeAsync(CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrEmpty(_connectionString))
        {
            throw new InvalidOperationException("Service not initialized. Call Initialize() first.");
        }

        const string query = @"
            SELECT MAX(modify_date) as max_modify_date
            FROM sys.objects 
            WHERE is_ms_shipped = 0";

        try
        {
            using var connection = new SqlConnection(_connectionString);
            await connection.OpenAsync(cancellationToken).ConfigureAwait(false);

            using var command = new SqlCommand(query, connection);
            var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);

            return result is DateTime dt ? dt.ToUniversalTime() : DateTime.UtcNow;
        }
        catch (Exception ex)
        {
            _console.Verbose($"[schema-detection] Failed to get max modification time: {ex.Message}");
            return DateTime.UtcNow;
        }
    }

    public Task FlushIndexAsync(CancellationToken cancellationToken = default)
    {
        return _indexManager.FlushAsync(cancellationToken);
    }

    private static string BuildObjectQuery(
        SchemaObjectType objectType,
        DateTime? sinceUtc,
        IReadOnlyList<string>? schemaFilter,
        out List<SqlParameter> parameters)
    {
        parameters = new List<SqlParameter>();

        static string BuildSchemaFilter(IReadOnlyList<string>? schemas, List<SqlParameter> parameterBag)
        {
            if (schemas == null || schemas.Count == 0)
            {
                return string.Empty;
            }

            var builder = new StringBuilder(" AND s.name IN (");
            for (var index = 0; index < schemas.Count; index++)
            {
                if (index > 0)
                {
                    builder.Append(", ");
                }

                var parameterName = $"@Schema{index}";
                builder.Append(parameterName);
                parameterBag.Add(new SqlParameter(parameterName, SqlDbType.NVarChar, 128)
                {
                    Value = schemas[index]
                });
            }

            builder.Append(')');
            return builder.ToString();
        }

        static string NormalizeSinceFilter(DateTime? filterValue, string? predicateColumn, List<SqlParameter> parameterBag)
        {
            if (!filterValue.HasValue)
            {
                return string.Empty;
            }

            if (string.IsNullOrWhiteSpace(predicateColumn))
            {
                return string.Empty;
            }

            var normalized = DateTime.SpecifyKind(filterValue.Value, DateTimeKind.Utc);
            var parameter = new SqlParameter("@SinceUtc", SqlDbType.DateTime2)
            {
                Value = normalized
            };

            parameterBag.Add(parameter);
            return $" AND {predicateColumn} > @SinceUtc";
        }

        var baseQuery = objectType switch
        {
            SchemaObjectType.StoredProcedure => @"
                SELECT 
                    o.object_id,
                    s.name as schema_name,
                    o.name as object_name,
                    o.modify_date,
                    o.is_ms_shipped,
                    m.definition
                FROM sys.objects o
                INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                LEFT JOIN sys.sql_modules m ON o.object_id = m.object_id
                WHERE o.type IN ('P')
                  AND o.is_ms_shipped = 0",

            SchemaObjectType.ScalarFunction => @"
                SELECT 
                    o.object_id,
                    s.name as schema_name,
                    o.name as object_name,
                    o.modify_date,
                    o.is_ms_shipped,
                    m.definition
                FROM sys.objects o
                INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                LEFT JOIN sys.sql_modules m ON o.object_id = m.object_id
                WHERE o.type IN ('FN')
                  AND o.is_ms_shipped = 0",

            SchemaObjectType.TableValuedFunction => @"
                SELECT 
                    o.object_id,
                    s.name as schema_name,
                    o.name as object_name,
                    o.modify_date,
                    o.is_ms_shipped,
                    m.definition
                FROM sys.objects o
                INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                LEFT JOIN sys.sql_modules m ON o.object_id = m.object_id
                WHERE o.type IN ('TF', 'IF')
                  AND o.is_ms_shipped = 0",

            SchemaObjectType.View => @"
                SELECT 
                    o.object_id,
                    s.name as schema_name,
                    o.name as object_name,
                    o.modify_date,
                    o.is_ms_shipped,
                    m.definition
                FROM sys.objects o
                INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                LEFT JOIN sys.sql_modules m ON o.object_id = m.object_id
                WHERE o.type IN ('V')
                  AND o.is_ms_shipped = 0",

            SchemaObjectType.Table => @"
                SELECT 
                    o.object_id,
                    s.name as schema_name,
                    o.name as object_name,
                    o.modify_date,
                    o.is_ms_shipped,
                    CAST(NULL as nvarchar(max)) as definition
                FROM sys.objects o
                INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
                WHERE o.type IN ('U')
                  AND o.is_ms_shipped = 0",

            SchemaObjectType.UserDefinedTableType => @"
                SELECT 
                    tt.type_table_object_id as object_id,
                    s.name as schema_name,
                    tt.name as object_name,
                    COALESCE(o.modify_date, o.create_date, CAST('0001-01-01T00:00:00' AS datetime2)) as modify_date,
                    ISNULL(o.is_ms_shipped, 0) as is_ms_shipped,
                    CAST(NULL as nvarchar(max)) as definition
                FROM sys.table_types tt
                INNER JOIN sys.schemas s ON tt.schema_id = s.schema_id
                LEFT JOIN sys.objects o ON o.object_id = tt.type_table_object_id
                WHERE tt.is_user_defined = 1",

            SchemaObjectType.UserDefinedDataType => @"
                SELECT 
                    t.user_type_id as object_id,
                    s.name as schema_name,
                    t.name as object_name,
                    COALESCE(o.modify_date, o.create_date, CAST('0001-01-01T00:00:00' AS datetime2)) as modify_date,
                    ISNULL(o.is_ms_shipped, 0) as is_ms_shipped,
                    CAST(NULL as nvarchar(max)) as definition
                FROM sys.types t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                LEFT JOIN sys.objects o ON o.object_id = t.user_type_id
                WHERE t.is_user_defined = 1 AND t.is_table_type = 0",

            _ => throw new ArgumentException($"Unsupported object type: {objectType}")
        };

        var predicateColumn = objectType switch
        {
            SchemaObjectType.UserDefinedTableType => "COALESCE(o.modify_date, o.create_date)",
            SchemaObjectType.UserDefinedDataType => null,
            _ => "o.modify_date"
        };

        baseQuery += NormalizeSinceFilter(sinceUtc, predicateColumn, parameters);
        baseQuery += BuildSchemaFilter(schemaFilter, parameters);

        var orderByIdentifier = objectType switch
        {
            SchemaObjectType.UserDefinedTableType => "tt.name",
            SchemaObjectType.UserDefinedDataType => "t.name",
            _ => "o.name"
        };

        baseQuery += $" ORDER BY s.name, {orderByIdentifier}";

        return baseQuery;
    }

    private static string BuildKey(string schema, string name)
    {
        return string.IsNullOrWhiteSpace(schema) ? name : $"{schema}.{name}";
    }

    private async Task EnsureIndexInitializedAsync(CancellationToken cancellationToken)
    {
        if (_indexInitialized)
        {
            return;
        }

        await _indexInitSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_indexInitialized)
            {
                return;
            }

            await _indexManager.InitializeAsync(cancellationToken).ConfigureAwait(false);
            _indexInitialized = true;
        }
        finally
        {
            _indexInitSemaphore.Release();
        }
    }

    private static bool TryMapSqlObjectType(string sqlTypeDesc, out SchemaObjectType objectType)
    {
        objectType = sqlTypeDesc switch
        {
            "SQL_STORED_PROCEDURE" => SchemaObjectType.StoredProcedure,
            "SQL_SCALAR_FUNCTION" => SchemaObjectType.ScalarFunction,
            "SQL_TABLE_VALUED_FUNCTION" => SchemaObjectType.TableValuedFunction,
            "SQL_INLINE_TABLE_VALUED_FUNCTION" => SchemaObjectType.TableValuedFunction,
            "VIEW" => SchemaObjectType.View,
            "USER_TABLE" => SchemaObjectType.Table,
            "TYPE_TABLE" => SchemaObjectType.UserDefinedTableType,
            _ => default
        };

        return objectType != default;
    }
}
