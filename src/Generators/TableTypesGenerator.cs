using Xtraq.Configuration;
using Xtraq.Engine;
using Xtraq.Metadata;
using Xtraq.Services;

namespace Xtraq.Generators;

internal sealed record TableTypeGenerationResult(int TotalArtifacts, IReadOnlyDictionary<string, int> ArtifactsPerSchema);

internal sealed class TableTypesGenerator : GeneratorBase
{
    private static readonly HashSet<string> ValueTypes = new(StringComparer.Ordinal)
    {
        "bool",
        "byte",
        "short",
        "int",
        "long",
        "decimal",
        "double",
        "float",
        "DateTime",
        "Guid"
    };

    private readonly XtraqConfiguration _cfg;
    private readonly ITableTypeMetadataProvider _provider;
    private readonly string _projectRoot;

    public TableTypesGenerator(XtraqConfiguration cfg, ITableTypeMetadataProvider provider, ITemplateRenderer renderer, ITemplateLoader? loader = null, string? projectRoot = null)
        : base(renderer, loader, cfg)
    {
        _cfg = cfg;
        _provider = provider;
        _projectRoot = projectRoot ?? Directory.GetCurrentDirectory();
    }

    /// <summary>
    /// Generates table type artifacts, optionally restricting emission to the supplied dependencies.
    /// </summary>
    /// <param name="requiredTableTypeReferences">Normalized schema-qualified table type names that must be emitted.</param>
    /// <returns>Aggregated artifact counts per schema.</returns>
    public TableTypeGenerationResult Generate(IReadOnlyCollection<string>? requiredTableTypeReferences = null)
    {
        var types = _provider.GetAll();
        var resolver = new NamespaceResolver(_cfg);
        var resolvedBase = resolver.Resolve();
        var outputDirName = _cfg.OutputDir ?? "Xtraq";
        var rootOut = Path.Combine(_projectRoot, outputDirName);
        // Updated rule: namespace = <BaseRoot>.<OutputDirName> (no duplicate suffixing); schema added per file.
        var ns = resolvedBase.EndsWith($".{outputDirName}", StringComparison.Ordinal)
            ? resolvedBase
            : resolvedBase + "." + outputDirName;
        Directory.CreateDirectory(rootOut);
        var written = 0;
        var artifactsPerSchema = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        if (!Templates.TryLoad("TableType", out var tableTypeTemplate))
        {
            throw new InvalidOperationException("TableType template 'TableType.xqt' not found – generation aborted.");
        }

        // Load optional shared header template
        var headerBlock = Templates.HeaderBlock;

        // Ensure interface exists (ITableType) and has correct namespace; rewrite if outdated
        var interfacePath = Path.Combine(rootOut, "ITableType.cs");
        bool mustWriteInterface = true;
        if (File.Exists(interfacePath))
        {
            var existing = File.ReadAllText(interfacePath);
            if (existing.Contains($"namespace {ns};"))
            {
                mustWriteInterface = false; // already correct
            }
        }
        if (mustWriteInterface)
        {
            string ifaceCode;
            if (Templates.TryLoad("ITableType", out var ifaceTpl))
            {
                var ifaceModel = new { Namespace = ns, HEADER = headerBlock };
                ifaceCode = Templates.RenderRawTemplate(ifaceTpl, ifaceModel);
            }
            else
            {
                ifaceCode = $"{headerBlock}namespace {ns};\n\npublic interface ITableType {{}}\n";
            }
            File.WriteAllText(interfacePath, ifaceCode, Encoding.UTF8);
            written++;
        }

        var normalizedDependencies = NormalizeDependencies(requiredTableTypeReferences);
        if (normalizedDependencies.Count > 0)
        {
            var beforeFilter = types.Count;
            types = types.Where(t => normalizedDependencies.Contains(NormalizeTypeIdentity(t.Catalog, t.Schema, t.Name))).ToList();
            var removed = beforeFilter - types.Count;
            try
            {
                Console.Out.WriteLine($"[xtraq] Info: TableTypes dependency filter applied -> {types.Count}/{beforeFilter} retained (removed {removed}).");
            }
            catch
            {
                // logging best-effort only
            }
        }
        else
        {
            if (types.Count > 0)
            {
                try
                {
                    Console.Out.WriteLine("[xtraq] Info: No table type dependencies detected -> skipping emission.");
                }
                catch
                {
                    // logging best-effort only
                }
            }

            types = Array.Empty<TableTypeInfo>();
        }
        foreach (var tt in types.OrderBy(t => t.Schema).ThenBy(t => t.Name))
        {
            var schemaPascal = ToPascalCase(tt.Schema);
            var schemaDir = Path.Combine(rootOut, schemaPascal);
            Directory.CreateDirectory(schemaDir);
            var cols = tt.Columns.Select(c =>
            {
                var propertyName = Xtraq.Utils.NameSanitizer.SanitizeIdentifier(c.Name);
                var clrType = MapSqlToClr(c.SqlType, c.IsNullable);
                var (propertyInitializer, builderInitializer) = GetInitializers(clrType);
                return new
                {
                    c.Name,
                    PropertyName = propertyName,
                    ClrType = clrType,
                    PropertyInitializer = propertyInitializer,
                    BuilderInitializer = builderInitializer
                };
            }).ToList();
            // Consistent naming with input mapping: CLR type uses Pascal(TableTypeName) without an extra 'Table' suffix
            var typeName = Xtraq.Utils.NameSanitizer.SanitizeIdentifier(tt.Name); // do not append suffix
            // Remove obsolete file with 'Table' suffix if present
            var obsolete = Path.Combine(schemaDir, typeName + "Table.cs");
            if (File.Exists(obsolete))
            {
                try { File.Delete(obsolete); } catch { }
            }
            var model = new
            {
                Namespace = ns + "." + schemaPascal,
                Schema = schemaPascal,
                Name = tt.Name,
                TypeName = typeName,
                TableTypeName = tt.Name, // original SQL UDTT name
                Columns = cols.Select((c, idx) => new
                {
                    c.PropertyName,
                    c.ClrType,
                    c.PropertyInitializer,
                    c.BuilderInitializer,
                    Separator = idx == cols.Count - 1 ? string.Empty : ","
                }).ToList(),
                ColumnsCount = cols.Count,
                // Deterministic placeholder instead of real timestamp
                GeneratedAt = "<generated>"
            };
            var extendedModel = new { model.Namespace, model.Schema, model.Name, model.TypeName, model.TableTypeName, model.Columns, model.ColumnsCount, model.GeneratedAt, HEADER = headerBlock };
            var code = Templates.RenderRawTemplate(tableTypeTemplate, extendedModel);
            var fileName = typeName + ".cs";
            File.WriteAllText(Path.Combine(schemaDir, fileName), code, Encoding.UTF8);
            written++;
            if (!artifactsPerSchema.TryGetValue(schemaPascal, out var currentCount))
            {
                artifactsPerSchema[schemaPascal] = 1;
            }
            else
            {
                artifactsPerSchema[schemaPascal] = currentCount + 1;
            }
        }
        return new TableTypeGenerationResult(
            written,
            new Dictionary<string, int>(artifactsPerSchema, StringComparer.OrdinalIgnoreCase)); // includes interface (even if 0 types)
    }

    private static HashSet<string> NormalizeDependencies(IReadOnlyCollection<string>? required)
    {
        var normalized = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        if (required is not { Count: > 0 })
        {
            return normalized;
        }

        foreach (var entry in required)
        {
            if (string.IsNullOrWhiteSpace(entry))
            {
                continue;
            }

            var normalizedEntry = TableTypeRefFormatter.Normalize(entry) ?? entry;
            if (!string.IsNullOrWhiteSpace(normalizedEntry))
            {
                normalized.Add(normalizedEntry);
                var parts = normalizedEntry.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
                if (parts.Length == 3)
                {
                    normalized.Add(string.Join('.', parts[1], parts[2]));
                }
            }
        }

        return normalized;
    }

    private static string NormalizeTypeIdentity(string? catalog, string schema, string name)
    {
        var combined = TableTypeRefFormatter.Combine(catalog, schema, name) ?? TableTypeRefFormatter.Combine(schema, name) ?? name;
        return TableTypeRefFormatter.Normalize(combined) ?? combined;
    }

    private static string ToPascalCase(string input)
    {
        if (string.IsNullOrWhiteSpace(input)) return string.Empty;
        var parts = input.Split(new[] { '-', '_', ' ', '.', '/', '\\' }, StringSplitOptions.RemoveEmptyEntries)
            .Select(p => p.Trim())
            .Where(p => p.Length > 0)
            .Select(p => char.ToUpperInvariant(p[0]) + (p.Length > 1 ? p.Substring(1).ToLowerInvariant() : string.Empty));
        var candidate = string.Concat(parts);
        candidate = new string(candidate.Where(ch => char.IsLetterOrDigit(ch) || ch == '_').ToArray());
        if (string.IsNullOrEmpty(candidate)) candidate = "Schema";
        if (char.IsDigit(candidate[0])) candidate = "N" + candidate;
        return candidate;
    }

    // Sanitization centralized via NameSanitizer.SanitizeIdentifier

    private static string MapSqlToClr(string sql, bool nullable)
    {
        sql = sql.ToLowerInvariant();
        string core = sql switch
        {
            var s when s.StartsWith("int") => "int",
            var s when s.StartsWith("bigint") => "long",
            var s when s.StartsWith("smallint") => "short",
            var s when s.StartsWith("tinyint") => "byte",
            var s when s.StartsWith("bit") => "bool",
            var s when s.StartsWith("decimal") || s.StartsWith("numeric") => "decimal",
            var s when s.StartsWith("float") => "double",
            var s when s.StartsWith("real") => "float",
            var s when s.Contains("date") || s.Contains("time") => "DateTime",
            var s when s.Contains("uniqueidentifier") => "Guid",
            var s when s.Contains("binary") || s.Contains("varbinary") => "byte[]",
            var s when s.Contains("char") || s.Contains("text") => "string",
            _ => "string"
        };
        if (core == "string")
        {
            return nullable ? "string?" : "string";
        }

        if (core == "byte[]")
        {
            return nullable ? "byte[]?" : "byte[]";
        }

        if (nullable)
        {
            core += "?";
        }

        return core;
    }

    private static (string PropertyInitializer, string BuilderInitializer) GetInitializers(string clrType)
    {
        if (string.IsNullOrWhiteSpace(clrType))
        {
            return (string.Empty, string.Empty);
        }

        if (clrType.EndsWith("?", StringComparison.Ordinal))
        {
            return (string.Empty, string.Empty);
        }

        if (ValueTypes.Contains(clrType))
        {
            return (string.Empty, string.Empty);
        }

        if (clrType.Equals("string", StringComparison.Ordinal))
        {
            return (" = string.Empty;", " = string.Empty");
        }

        if (clrType.Equals("byte[]", StringComparison.Ordinal))
        {
            return (" = Array.Empty<byte>();", " = Array.Empty<byte>()");
        }

        return (" = default!;", " = default!");
    }

}
