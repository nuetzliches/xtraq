using System.Text.Json.Serialization;
using Xtraq.SnapshotBuilder.Writers;
using Xtraq.Utils;

namespace Xtraq.Services;

internal interface ISchemaSnapshotService
{
    SchemaSnapshot? Load(string fingerprint);
    void Save(SchemaSnapshot snapshot);
    string BuildFingerprint(string serverName, string databaseName, IEnumerable<string>? includedSchemas, int procedureCount, int udttCount, int parserVersion);
}

internal sealed class SchemaSnapshotService : ISchemaSnapshotService
{
    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    private string? EnsureDir()
    {
        var working = DirectoryUtils.GetWorkingDirectory();
        if (string.IsNullOrEmpty(working)) return null;
        var dir = Path.Combine(working, ".xtraq", "cache");
        try { Directory.CreateDirectory(dir); } catch { }
        return dir;
    }

    public string BuildFingerprint(string serverName, string databaseName, IEnumerable<string>? includedSchemas, int procedureCount, int udttCount, int parserVersion)
    {
        var parts = new[]
        {
            serverName?.Trim().ToLowerInvariant() ?? "?",
            databaseName?.Trim().ToLowerInvariant() ?? "?",
            string.Join(';', (includedSchemas ?? Array.Empty<string>()).OrderBy(s => s, StringComparer.OrdinalIgnoreCase)),
            procedureCount.ToString(),
            udttCount.ToString(),
            parserVersion.ToString()
        };
        var raw = string.Join('|', parts);
        var hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(raw)));
        return hash.Substring(0, 16);
    }

    public SchemaSnapshot? Load(string fingerprint)
    {
        try
        {
            var cacheDir = EnsureDir();
            if (string.IsNullOrEmpty(cacheDir)) return null;

            var pathToLoad = Path.Combine(cacheDir, fingerprint + ".json");
            if (!File.Exists(pathToLoad)) return null;

            var json = File.ReadAllText(pathToLoad);
            if (string.IsNullOrWhiteSpace(json))
            {
                return null;
            }

            SchemaCacheDocument? cacheDocument = null;
            try
            {
                cacheDocument = JsonSerializer.Deserialize<SchemaCacheDocument>(json, _jsonOptions);
            }
            catch (JsonException)
            {
                cacheDocument = null;
            }

            if (cacheDocument != null)
            {
                if (cacheDocument.CacheVersion < SchemaCacheDocument.CurrentVersion && RequiresLegacyConversion(cacheDocument, json))
                {
                    try
                    {
                        var legacy = JsonSerializer.Deserialize<LegacySchemaCacheDocument>(json, _jsonOptions);
                        cacheDocument = ConvertLegacyCacheDocument(legacy) ?? cacheDocument;
                    }
                    catch (JsonException)
                    {
                        // ignore legacy conversion failures; fall back to existing document
                    }
                }

                return ConvertFromCacheDocument(cacheDocument);
            }

            try
            {
                return JsonSerializer.Deserialize<SchemaSnapshot>(json, _jsonOptions);
            }
            catch (JsonException)
            {
                return null;
            }
        }
        catch { return null; }
    }

    public void Save(SchemaSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        if (string.IsNullOrEmpty(snapshot.Fingerprint)) return;
        try
        {
            var dir = EnsureDir();
            if (dir == null) return;
            var path = Path.Combine(dir, snapshot.Fingerprint + ".json");

            var schemaNames = (snapshot.Schemas ?? new List<SnapshotSchema>())
                .Select(s => s?.Name)
                .Where(n => !string.IsNullOrWhiteSpace(n))
                .Select(static n => n!.Trim())
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
                .ToList();

            var procedures = (snapshot.Procedures ?? new List<SnapshotProcedure>())
                .Where(p => !string.IsNullOrWhiteSpace(p?.Schema) && !string.IsNullOrWhiteSpace(p?.Name))
                .Select(p => new SchemaCacheProcedure
                {
                    Schema = p.Schema,
                    Name = p.Name
                })
                .OrderBy(p => p.Schema, StringComparer.OrdinalIgnoreCase)
                .ThenBy(p => p.Name, StringComparer.OrdinalIgnoreCase)
                .ToList();

            var tables = (snapshot.Tables ?? new List<SnapshotTable>())
                .Where(t => !string.IsNullOrWhiteSpace(t?.Schema) && !string.IsNullOrWhiteSpace(t?.Name))
                .Select(t => new SchemaCacheTable
                {
                    Schema = t.Schema,
                    Name = t.Name,
                    ColumnCount = t.Columns?.Count ?? 0,
                    ColumnsHash = ComputeTableColumnsHash(t.Columns)
                })
                .OrderBy(t => t.Schema, StringComparer.OrdinalIgnoreCase)
                .ThenBy(t => t.Name, StringComparer.OrdinalIgnoreCase)
                .ToList();

            var document = new SchemaCacheDocument
            {
                CacheVersion = SchemaCacheDocument.CurrentVersion,
                SchemaVersion = snapshot.SchemaVersion,
                Fingerprint = snapshot.Fingerprint,
                Database = snapshot.Database != null
                    ? new SchemaCacheDatabase
                    {
                        ServerHash = snapshot.Database.ServerHash,
                        Name = snapshot.Database.Name
                    }
                    : null,
                Schemas = schemaNames.Select(n => new SchemaCacheSchema { Name = n }).ToList(),
                Procedures = procedures,
                Tables = tables
            };

            var json = JsonSerializer.Serialize(document, _jsonOptions);
            File.WriteAllText(path, json);

            PruneLegacySnapshots(dir, snapshot.Fingerprint);

        }
        catch { /* swallow snapshot write errors */ }
    }

    private static bool RequiresLegacyConversion(SchemaCacheDocument document, string json)
    {
        if (document == null)
        {
            return false;
        }

        if (document.Procedures == null || document.Procedures.Count == 0)
        {
            return ContainsInputsMarker(json);
        }

        var allHaveParameters = document.Procedures.All(p => p != null && p.Parameters != null && p.Parameters.Count > 0);
        if (allHaveParameters)
        {
            return false;
        }

        return ContainsInputsMarker(json);
    }

    private static bool ContainsInputsMarker(string json)
    {
        if (string.IsNullOrEmpty(json))
        {
            return false;
        }

        return json.IndexOf("\"Inputs\"", StringComparison.OrdinalIgnoreCase) >= 0;
    }

    private static SchemaCacheDocument? ConvertLegacyCacheDocument(LegacySchemaCacheDocument? legacy)
    {
        if (legacy == null)
        {
            return null;
        }

        var convertedProcedures = (legacy.Procedures ?? new List<LegacySchemaCacheProcedure>())
            .Where(p => !string.IsNullOrWhiteSpace(p?.Schema) && !string.IsNullOrWhiteSpace(p?.Name))
            .Select(p => new SchemaCacheProcedure
            {
                Schema = p.Schema,
                Name = p.Name,
                Parameters = CloneParameters(p.Inputs)
            })
            .ToList();

        return new SchemaCacheDocument
        {
            CacheVersion = SchemaCacheDocument.CurrentVersion,
            SchemaVersion = legacy.SchemaVersion,
            Fingerprint = legacy.Fingerprint ?? string.Empty,
            Database = legacy.Database,
            Schemas = legacy.Schemas ?? new List<SchemaCacheSchema>(),
            Procedures = convertedProcedures,
            Tables = new List<SchemaCacheTable>()
        };
    }

    private static SchemaSnapshot? ConvertFromCacheDocument(SchemaCacheDocument? document)
    {
        if (document == null)
        {
            return null;
        }

        var schemaNames = (document.Schemas ?? new List<SchemaCacheSchema>())
            .Select(s => s?.Name)
            .Where(n => !string.IsNullOrWhiteSpace(n))
            .Select(static n => n!.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
            .ToList();

        var snapshot = new SchemaSnapshot
        {
            SchemaVersion = document.SchemaVersion,
            Fingerprint = document.Fingerprint ?? string.Empty,
            Database = document.Database != null
                ? new SnapshotDatabase
                {
                    ServerHash = document.Database.ServerHash,
                    Name = document.Database.Name
                }
                : null,
            Schemas = schemaNames.Select(n => new SnapshotSchema
            {
                Name = n,
                TableTypeRefs = new List<string>()
            }).ToList(),
            Procedures = (document.Procedures ?? new List<SchemaCacheProcedure>())
                .Where(p => !string.IsNullOrWhiteSpace(p?.Schema) && !string.IsNullOrWhiteSpace(p?.Name))
                .Select(p => new SnapshotProcedure
                {
                    Schema = p.Schema,
                    Name = p.Name,
                    Inputs = CloneParameters(p.Parameters),
                    ResultSets = new List<SnapshotResultSet>()
                })
                .ToList(),
            UserDefinedTableTypes = new List<SnapshotUdtt>(),
            Tables = (document.Tables ?? new List<SchemaCacheTable>())
                .Where(t => !string.IsNullOrWhiteSpace(t?.Schema) && !string.IsNullOrWhiteSpace(t?.Name))
                .Select(t => new SnapshotTable
                {
                    Schema = t.Schema,
                    Name = t.Name,
                    ColumnCount = t.ColumnCount,
                    ColumnsHash = t.ColumnsHash,
                    Columns = new List<SnapshotTableColumn>()
                })
                .ToList(),
            Views = new List<SnapshotView>(),
            UserDefinedTypes = new List<SnapshotUserDefinedType>(),
            Parser = null,
            Stats = null
        };

        return snapshot;
    }

    private static void PruneLegacySnapshots(string directory, string activeFingerprint, int retentionCount = 2)
    {
        if (string.IsNullOrEmpty(directory) || !Directory.Exists(directory) || retentionCount <= 0)
        {
            return;
        }

        try
        {
            var fingerprintFiles = Directory.GetFiles(directory, "*.json", SearchOption.TopDirectoryOnly)
                .Select(file => new FileInfo(file))
                .Where(static info => IsFingerprintFile(info.Name))
                .OrderByDescending(static info => info.LastWriteTimeUtc)
                .ToList();

            if (fingerprintFiles.Count <= retentionCount)
            {
                return;
            }

            var keep = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            if (!string.IsNullOrWhiteSpace(activeFingerprint))
            {
                var active = fingerprintFiles.FirstOrDefault(info => string.Equals(Path.GetFileNameWithoutExtension(info.Name), activeFingerprint, StringComparison.OrdinalIgnoreCase));
                if (active != null)
                {
                    keep.Add(active.FullName);
                }
            }

            foreach (var info in fingerprintFiles)
            {
                if (keep.Count >= retentionCount)
                {
                    break;
                }

                if (!keep.Contains(info.FullName))
                {
                    keep.Add(info.FullName);
                }
            }

            foreach (var info in fingerprintFiles)
            {
                if (keep.Contains(info.FullName))
                {
                    continue;
                }

                try
                {
                    File.Delete(info.FullName);
                }
                catch
                {
                    // best-effort cleanup
                }
            }
        }
        catch
        {
            // ignore prune failures; cache hygiene is best-effort
        }
    }

    private static bool IsFingerprintFile(string fileName)
    {
        if (string.IsNullOrWhiteSpace(fileName) || !fileName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var nameWithoutExtension = Path.GetFileNameWithoutExtension(fileName);
        if (nameWithoutExtension.Length != 16)
        {
            return false;
        }

        foreach (var ch in nameWithoutExtension)
        {
            var isHex = (ch >= '0' && ch <= '9') || (ch >= 'A' && ch <= 'F');
            if (!isHex)
            {
                return false;
            }
        }

        return true;
    }

    private static List<SnapshotInput> CloneParameters(IEnumerable<SnapshotInput>? inputs)
    {
        var result = new List<SnapshotInput>();
        if (inputs == null)
        {
            return result;
        }

        foreach (var input in inputs)
        {
            var clone = CloneParameter(input);
            if (clone != null)
            {
                result.Add(clone);
            }
        }

        return result;
    }

    private static SnapshotInput? CloneParameter(SnapshotInput? source)
    {
        if (source == null)
        {
            return null;
        }

        var normalizedTypeRef = NormalizeOrNull(source.TypeRef);
        var normalizedTableTypeRef = TableTypeRefFormatter.Normalize(source.TableTypeRef);

        if (string.IsNullOrEmpty(normalizedTableTypeRef))
        {
            var legacyRef = TableTypeRefFormatter.Combine(source.TableTypeSchema, source.TableTypeName);
            normalizedTableTypeRef = TableTypeRefFormatter.Normalize(legacyRef);
        }

        var (inferredCatalog, inferredSchema, inferredName) = TableTypeRefFormatter.Split(normalizedTableTypeRef);

        return new SnapshotInput
        {
            Name = source.Name ?? string.Empty,
            TypeRef = normalizedTypeRef,
            TableTypeRef = normalizedTableTypeRef,
            TableTypeCatalog = NormalizeOrNull(source.TableTypeCatalog) ?? NormalizeOrNull(inferredCatalog),
            TableTypeSchema = NormalizeOrNull(inferredSchema),
            TableTypeName = NormalizeOrNull(inferredName),
            IsOutput = source.IsOutput == true ? true : null,
            IsNullable = source.IsNullable == true ? true : null,
            MaxLength = source.MaxLength.HasValue && source.MaxLength.Value > 0 ? source.MaxLength : null,
            HasDefaultValue = source.HasDefaultValue == true ? true : null,
            TypeSchema = NormalizeOrNull(source.TypeSchema),
            TypeName = NormalizeOrNull(source.TypeName),
            Precision = source.Precision.HasValue && source.Precision.Value > 0 ? source.Precision : null,
            Scale = source.Scale.HasValue && source.Scale.Value > 0 ? source.Scale : null
        };
    }

    private static string? NormalizeOrNull(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return null;
        }

        var trimmed = value.Trim();
        return trimmed.Length == 0 ? null : trimmed;
    }

    private static string ComputeTableColumnsHash(IReadOnlyList<SnapshotTableColumn>? columns)
    {
        if (columns == null || columns.Count == 0)
        {
            return string.Empty;
        }

        var parts = new List<string>(columns.Count);
        foreach (var column in columns)
        {
            if (column == null || string.IsNullOrWhiteSpace(column.Name))
            {
                continue;
            }

            var part = string.Join("|", new[]
            {
                column.Name.Trim(),
                NormalizeTypeRef(column.TypeRef),
                column.IsNullable == true ? "1" : "0",
                FormatNumeric(column.MaxLength),
                FormatNumeric(column.Precision),
                FormatNumeric(column.Scale),
                FormatBool(column.IsIdentity),
                FormatBool(column.HasDefaultValue),
                NormalizeExpression(column.DefaultDefinition),
                NormalizeDescriptor(column.DefaultConstraintName),
                FormatBool(column.IsComputed),
                NormalizeExpression(column.ComputedDefinition),
                FormatBool(column.IsComputedPersisted),
                FormatBool(column.IsRowGuid),
                FormatBool(column.IsSparse),
                FormatBool(column.IsHidden),
                FormatBool(column.IsColumnSet),
                NormalizeDescriptor(column.GeneratedAlwaysType)
            });

            parts.Add(part);
        }

        if (parts.Count == 0)
        {
            return string.Empty;
        }

        var payload = string.Join(";", parts);
        return string.IsNullOrEmpty(payload)
            ? string.Empty
            : SnapshotWriterUtilities.ComputeHash(payload);
    }

    private static string NormalizeTypeRef(string? typeRef)
    {
        if (string.IsNullOrWhiteSpace(typeRef))
        {
            return string.Empty;
        }

        return typeRef.Trim().ToLowerInvariant();
    }

    private static string FormatNumeric(int? value)
    {
        if (!value.HasValue)
        {
            return string.Empty;
        }

        if (value.Value <= 0)
        {
            return string.Empty;
        }

        return value.Value.ToString(CultureInfo.InvariantCulture);
    }

    private static string FormatBool(bool? value)
        => value == true ? "1" : "0";

    private static string NormalizeDescriptor(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        return value.Trim();
    }

    private static string NormalizeExpression(string? value)
    {
        var normalized = SnapshotWriterUtilities.NormalizeSqlExpression(value);
        return string.IsNullOrWhiteSpace(normalized) ? string.Empty : normalized;
    }

    private sealed class SchemaCacheDocument
    {
        public const int CurrentVersion = 3;
        public int CacheVersion { get; set; } = CurrentVersion;
        public int SchemaVersion { get; set; }
        public string Fingerprint { get; set; } = string.Empty;
        public SchemaCacheDatabase? Database { get; set; }
        public List<SchemaCacheSchema> Schemas { get; set; } = new();
        public List<SchemaCacheProcedure> Procedures { get; set; } = new();
        public List<SchemaCacheTable> Tables { get; set; } = new();
    }

    private sealed class SchemaCacheSchema
    {
        public string Name { get; set; } = string.Empty;
    }

    private sealed class SchemaCacheProcedure
    {
        public string Schema { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public List<SnapshotInput>? Parameters { get; set; }

        [JsonPropertyName("Inputs")]
        public List<SnapshotInput>? LegacyInputs
        {
            set
            {
                if (value == null || value.Count == 0)
                {
                    return;
                }

                Parameters = value;
            }
        }
    }

    private sealed class SchemaCacheTable
    {
        public string Schema { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public int ColumnCount { get; set; }
        public string ColumnsHash { get; set; } = string.Empty;
    }

    private sealed class SchemaCacheDatabase
    {
        public string ServerHash { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
    }

    private sealed class LegacySchemaCacheDocument
    {
        public int CacheVersion { get; set; }
        public int SchemaVersion { get; set; }
        public string Fingerprint { get; set; } = string.Empty;
        public SchemaCacheDatabase? Database { get; set; }
        public List<SchemaCacheSchema> Schemas { get; set; } = new();
        public List<LegacySchemaCacheProcedure> Procedures { get; set; } = new();
    }

    private sealed class LegacySchemaCacheProcedure
    {
        public string Schema { get; set; } = string.Empty;
        public string Name { get; set; } = string.Empty;
        public List<SnapshotInput> Inputs { get; set; } = new();
    }

}

internal sealed class SchemaSnapshot
{
    public int SchemaVersion { get; set; } = 1;
    public string Fingerprint { get; set; } = string.Empty;
    [JsonIgnore] // Excluded from persisted snapshot to avoid nondeterministic Git diffs
    public DateTime GeneratedUtc { get; set; } = DateTime.UtcNow;
    public SnapshotDatabase? Database { get; set; }
    public List<SnapshotProcedure> Procedures { get; set; } = new();
    public List<SnapshotSchema> Schemas { get; set; } = new();
    public List<SnapshotUdtt> UserDefinedTableTypes { get; set; } = new();
    // Lightweight baseline snapshot for tables (schema, name, columns only) to support AST type resolution
    public List<SnapshotTable> Tables { get; set; } = new();
    // Views mirror tables (currently without dependency tracking; planned for v5)
    public List<SnapshotView> Views { get; set; } = new();
    // User-defined scalar types (no table types) required before tables/views to resolve alias types
    // Name kept as 'UserDefinedTypes' for clarity compared to table types
    public List<SnapshotUserDefinedType> UserDefinedTypes { get; set; } = new();
    // Preview: Functions (scalar + TVF) captured independently of schema allow-list.
    public int? FunctionsVersion { get; set; } // set to 1 when functions populated
    public List<SnapshotFunction> Functions { get; set; } = new();
    public SnapshotParserInfo? Parser { get; set; }
    public SnapshotStats? Stats { get; set; }
}

internal sealed class SnapshotDatabase
{
    public string ServerHash { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
}

internal sealed class SnapshotProcedure
{
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public List<SnapshotInput> Inputs { get; set; } = new();
    public List<SnapshotResultSet> ResultSets { get; set; } = new();
}

internal sealed class SnapshotInput
{
    public string Name { get; set; } = string.Empty;
    public string? TypeRef { get; set; }
    public string? TableTypeRef { get; set; }
    public string? TableTypeCatalog { get; set; }
    public string? TableTypeSchema { get; set; }
    public string? TableTypeName { get; set; }
    public bool? IsOutput { get; set; }
    public bool? IsNullable { get; set; }
    public int? MaxLength { get; set; }
    public bool? HasDefaultValue { get; set; } // persist only when true
    public string? TypeSchema { get; set; }
    public string? TypeName { get; set; }
    public int? Precision { get; set; }
    public int? Scale { get; set; }
}

internal sealed class SnapshotResultSet
{
    public bool ReturnsJson { get; set; }
    public bool ReturnsJsonArray { get; set; }
    public string? JsonRootProperty { get; set; }
    public List<SnapshotResultColumn> Columns { get; set; } = new();
    public string? ExecSourceSchemaName { get; set; }
    public string? ExecSourceProcedureName { get; set; }
    public bool? HasSelectStar { get; set; } // nullable to allow pruning when false
    public SnapshotColumnReference? Reference { get; set; }
}

internal sealed class SnapshotResultColumn
{
    public string Name { get; set; } = string.Empty;
    public string? Alias { get; set; }
    public string? SourceColumn { get; set; }
    public string? TypeRef { get; set; }
    public string? UserTypeRef { get; set; }
    public string? SqlTypeName { get; set; }
    public bool? IsNullable { get; set; }
    public int? MaxLength { get; set; }
    // Precision and scale for decimal/numeric (or time/datetime2 when needed). Zero/null entries are pruned.
    public int? Precision { get; set; }
    public int? Scale { get; set; }
    // Identity marker (persist true only). Relevant for tables/views/stored procedure outputs; optional for procedure result sets when sourced from DMV data.
    public bool? IsIdentity { get; set; }
    // Flattened nested JSON structure (v6): when IsNestedJson=true these flags describe the nested JSON under this column
    public bool? IsNestedJson { get; set; }
    public bool? ReturnsJson { get; set; }
    public bool? ReturnsJsonArray { get; set; }
    public bool? ReturnsUnknownJson { get; set; }
    public string? JsonRootProperty { get; set; }
    public bool? JsonIncludeNullValues { get; set; }
    public string? JsonElementClrType { get; set; }
    public string? JsonElementSqlType { get; set; }
    public List<SnapshotResultColumn> Columns { get; set; } = new(); // renamed from JsonColumns in v7
    // Deferred function expansion: persist reference & flag
    public SnapshotColumnReference? Reference { get; set; }
    public bool? DeferredJsonExpansion { get; set; }
    public string? FunctionRef { get; set; }
}

internal sealed class SnapshotColumnReference
{
    public string Kind { get; set; } = string.Empty; // Function | View | Procedure
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
}

internal sealed class SnapshotSchema
{
    public string Name { get; set; } = string.Empty;
    public List<string> TableTypeRefs { get; set; } = new(); // schema.name
}

internal sealed class SnapshotUdtt
{
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public int? UserTypeId { get; set; }
    public List<SnapshotUdttColumn> Columns { get; set; } = new();
    public string Hash { get; set; } = string.Empty;
}

internal sealed class SnapshotUdttColumn
{
    public string Name { get; set; } = string.Empty;
    public string? TypeRef { get; set; }
    public bool? IsNullable { get; set; }
    public int? MaxLength { get; set; }
    public int? Precision { get; set; }
    public int? Scale { get; set; }
}

internal sealed class SnapshotFunction
{
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public bool? IsTableValued { get; set; } // false values are pruned before persisting (write true only)
    public string? ReturnSqlType { get; set; } // empty for TVF or omitted for JSON projections
    public int? ReturnMaxLength { get; set; } // only for scalar functions
    public bool? ReturnIsNullable { get; set; } // only for scalar functions (pruned for JSON projections)
    public List<SnapshotFunctionParameter> Parameters { get; set; } = new();
    public List<SnapshotFunctionColumn>? Columns { get; set; } = new(); // used for TVF or JSON columns (nested structures supported)
    public bool? ReturnsJson { get; set; } // heuristic derived from the definition (FOR JSON)
    public bool? ReturnsJsonArray { get; set; } // true when FOR JSON is used without WITHOUT_ARRAY_WRAPPER
    public string? JsonRootProperty { get; set; } // optional; can be derived from the path (future AST analysis)
    public bool? JsonIncludeNullValues { get; set; }
    public bool? IsEncrypted { get; set; } // only persisted when true
    public List<string>? Dependencies { get; set; } = new(); // list of other functions (schema.name) directly depended on
}

internal sealed class SnapshotFunctionParameter
{
    public string Name { get; set; } = string.Empty;
    public string? TableTypeRef { get; set; }
    public string? TableTypeCatalog { get; set; }
    public string? TableTypeSchema { get; set; }
    public string? TableTypeName { get; set; }
    public string? TypeRef { get; set; }
    public bool? IsOutput { get; set; }
    public bool? IsNullable { get; set; }
    public int? MaxLength { get; set; }
    // Note: default value metadata is currently not persisted to stay aligned with stored procedure inputs
    public bool? HasDefaultValue { get; set; } // persist true only
}

internal sealed class SnapshotFunctionColumn
{
    public string Name { get; set; } = string.Empty;
    public string? TypeRef { get; set; }
    public bool? IsNullable { get; set; }
    public int? MaxLength { get; set; }
    public int? Precision { get; set; }
    public int? Scale { get; set; }
    public bool? IsIdentity { get; set; }
    // Nested JSON support (similar to result set columns but lightweight)
    public bool? IsNestedJson { get; set; } // true when the substructure contains an object or array
    public bool? ReturnsJson { get; set; } // Flags JSON subselect usage
    public bool? ReturnsJsonArray { get; set; } // true when the subselect returns an array
    public string? JsonRootProperty { get; set; } // Root('x') or the implicit alias
    public bool? JsonIncludeNullValues { get; set; }
    public List<SnapshotFunctionColumn>? Columns { get; set; } = new(); // supports recursive nesting
}

/// --- New baseline snapshot models (priority 1) ---
internal sealed class SnapshotTable
{
    public string? Catalog { get; set; }
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    [JsonIgnore]
    public int? ColumnCount { get; set; }

    [JsonIgnore]
    public string? ColumnsHash { get; set; }

    public List<SnapshotTableColumn> Columns { get; set; } = new();
}

internal sealed class SnapshotTableColumn
{
    public string Name { get; set; } = string.Empty;
    public string? TypeRef { get; set; }
    public bool? IsNullable { get; set; } // false values are pruned during persistence to match other models (writer implementation pending)
    public int? MaxLength { get; set; } // null when the length is zero or not applicable
    public bool? IsIdentity { get; set; } // persist true only
    public int? Precision { get; set; }
    public int? Scale { get; set; }
    public bool? HasDefaultValue { get; set; }
    public string? DefaultConstraintName { get; set; }
    public string? DefaultDefinition { get; set; }
    public bool? IsComputed { get; set; }
    public string? ComputedDefinition { get; set; }
    public bool? IsComputedPersisted { get; set; }
    public bool? IsRowGuid { get; set; }
    public bool? IsSparse { get; set; }
    public bool? IsHidden { get; set; }
    public bool? IsColumnSet { get; set; }
    public string? GeneratedAlwaysType { get; set; }
}

internal sealed class SnapshotView
{
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public List<SnapshotViewColumn> Columns { get; set; } = new();
}

internal sealed class SnapshotViewColumn
{
    public string Name { get; set; } = string.Empty;
    public string? TypeRef { get; set; }
    public bool? IsNullable { get; set; }
    public int? MaxLength { get; set; }
    public int? Precision { get; set; }
    public int? Scale { get; set; }
}

internal sealed class SnapshotUserDefinedType
{
    public string? Catalog { get; set; }
    public string Schema { get; set; } = string.Empty; // sys / dbo / user-defined schema
    public string Name { get; set; } = string.Empty;
    public string BaseSqlTypeName { get; set; } = string.Empty; // e.g. nvarchar, int, decimal
    public int? MaxLength { get; set; } // for (n)varchar, varbinary
    public int? Precision { get; set; } // for decimal/numeric types
    public int? Scale { get; set; } // for decimal/numeric types
    public bool? IsNullable { get; set; } // when discoverable (scalar UDTs often have no direct nullable flag)
}

internal sealed class SnapshotParserInfo
{
    public string ToolVersion { get; set; } = string.Empty;
    public int ResultSetParserVersion { get; set; }
}

internal sealed class SnapshotStats
{
    public int ProcedureTotal { get; set; }
    public int ProcedureSkipped { get; set; }
    public int ProcedureLoaded { get; set; }
    public int UdttTotal { get; set; }
    // Extension (priority 1): baseline counters for new snapshot artifacts
    public int TableTotal { get; set; }
    public int ViewTotal { get; set; }
    public int UserDefinedTypeTotal { get; set; }
}
