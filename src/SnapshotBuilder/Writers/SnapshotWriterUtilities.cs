using Xtraq.Data.Models;
using Xtraq.Data.Queries;
using Xtraq.SnapshotBuilder.Models;
using Xtraq.Utils;

namespace Xtraq.SnapshotBuilder.Writers;

internal static class SnapshotWriterUtilities
{
    internal readonly record struct TypeRefParts(string? Catalog, string? Schema, string? Name);
    internal readonly record struct TableRefParts(string? Catalog, string? Schema, string? Name);

    internal static string NormalizeParameterName(string? raw)
    {
        if (string.IsNullOrWhiteSpace(raw))
        {
            return string.Empty;
        }

        return raw.TrimStart('@');
    }

    internal static string? BuildTypeRef(StoredProcedureInput input)
    {
        if (input == null)
        {
            return null;
        }

        if (input.IsTableType && !string.IsNullOrWhiteSpace(input.UserTypeSchemaName) && !string.IsNullOrWhiteSpace(input.UserTypeName))
        {
            return BuildTypeRef(null, input.UserTypeSchemaName, input.UserTypeName);
        }

        if (!string.IsNullOrWhiteSpace(input.UserTypeSchemaName) && !string.IsNullOrWhiteSpace(input.UserTypeName))
        {
            return BuildTypeRef(null, input.UserTypeSchemaName, input.UserTypeName);
        }

        if (!string.IsNullOrWhiteSpace(input.SqlTypeName))
        {
            var normalized = NormalizeSqlTypeName(input.SqlTypeName);
            if (!string.IsNullOrWhiteSpace(normalized))
            {
                return BuildTypeRef("sys", normalized);
            }
        }

        return null;
    }

    internal static string? BuildTypeRef(Column column)
    {
        if (column == null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(column.UserTypeSchemaName) && !string.IsNullOrWhiteSpace(column.UserTypeName))
        {
            return BuildTypeRef(column.CatalogName, column.UserTypeSchemaName, column.UserTypeName);
        }

        if (!string.IsNullOrWhiteSpace(column.SqlTypeName))
        {
            var normalized = NormalizeSqlTypeName(column.SqlTypeName);
            if (!string.IsNullOrWhiteSpace(normalized))
            {
                return BuildTypeRef("sys", normalized);
            }
        }

        return null;
    }

    internal static string? BuildTypeRef(ProcedureResultColumn column)
    {
        if (column == null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(column.UserTypeRef))
        {
            var parts = SplitTypeRefParts(column.UserTypeRef);
            var normalized = BuildTypeRef(parts.Catalog, parts.Schema, parts.Name);
            if (!string.IsNullOrWhiteSpace(normalized))
            {
                return normalized;
            }
        }

        if (!string.IsNullOrWhiteSpace(column.UserTypeSchemaName) && !string.IsNullOrWhiteSpace(column.UserTypeName))
        {
            return BuildTypeRef(column.SourceCatalog, column.UserTypeSchemaName, column.UserTypeName);
        }

        var sqlType = column.SqlTypeName;
        if (string.IsNullOrWhiteSpace(sqlType) && !string.IsNullOrWhiteSpace(column.CastTargetType))
        {
            sqlType = column.CastTargetType;
        }

        if (!string.IsNullOrWhiteSpace(sqlType))
        {
            var normalized = NormalizeSqlTypeName(sqlType);
            if (!string.IsNullOrWhiteSpace(normalized))
            {
                return BuildTypeRef("sys", normalized);
            }
        }

        return null;
    }

    internal static string? BuildTypeRef(FunctionParamRow parameter)
    {
        if (parameter == null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(parameter.user_type_schema_name) && !string.IsNullOrWhiteSpace(parameter.user_type_name))
        {
            return BuildTypeRef(null, parameter.user_type_schema_name, parameter.user_type_name);
        }

        var sqlType = parameter.system_type_name ?? parameter.base_type_name;
        if (!string.IsNullOrWhiteSpace(sqlType))
        {
            var normalized = NormalizeSqlTypeName(sqlType);
            if (!string.IsNullOrWhiteSpace(normalized))
            {
                return BuildTypeRef("sys", normalized);
            }
        }

        return null;
    }

    internal static string? BuildTypeRef(FunctionColumnRow column)
    {
        if (column == null)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(column.user_type_schema_name) && !string.IsNullOrWhiteSpace(column.user_type_name))
        {
            return BuildTypeRef(null, column.user_type_schema_name, column.user_type_name);
        }

        var sqlType = column.system_type_name ?? column.base_type_name;
        if (!string.IsNullOrWhiteSpace(sqlType))
        {
            var normalized = NormalizeSqlTypeName(sqlType);
            if (!string.IsNullOrWhiteSpace(normalized))
            {
                return BuildTypeRef("sys", normalized);
            }
        }

        return null;
    }

    internal static string? BuildTypeRef(string? schema, string? name)
    {
        return BuildTypeRef(null, schema, name);
    }

    internal static string? BuildTypeRef(string? catalog, string? schema, string? name)
    {
        if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(name))
        {
            return null;
        }

        var schemaPart = schema.Trim();
        var namePart = name.Trim();
        if (schemaPart.Length == 0 || namePart.Length == 0)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(catalog))
        {
            var catalogPart = catalog.Trim();
            if (catalogPart.Length > 0)
            {
                return string.Concat(catalogPart, ".", schemaPart, ".", namePart);
            }
        }

        return string.Concat(schemaPart, ".", namePart);
    }

    internal static TypeRefParts SplitTypeRefParts(string? typeRef)
    {
        if (string.IsNullOrWhiteSpace(typeRef))
        {
            return default;
        }

        var parts = typeRef.Trim().Split('.', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length == 0)
        {
            return default;
        }

        if (parts.Length == 1)
        {
            return new TypeRefParts(null, null, parts[0]);
        }

        if (parts.Length == 2)
        {
            var schema = string.IsNullOrWhiteSpace(parts[0]) ? null : parts[0];
            var name = string.IsNullOrWhiteSpace(parts[1]) ? null : parts[1];
            return new TypeRefParts(null, schema, name);
        }

        var catalog = string.IsNullOrWhiteSpace(parts[0]) ? null : parts[0];
        var schemaPart = string.IsNullOrWhiteSpace(parts[^2]) ? null : parts[^2];
        var namePart = string.IsNullOrWhiteSpace(parts[^1]) ? null : parts[^1];
        return new TypeRefParts(catalog, schemaPart, namePart);
    }

    internal static (string? Schema, string? Name) SplitTypeRef(string? typeRef)
    {
        var parts = SplitTypeRefParts(typeRef);
        return (parts.Schema, parts.Name);
    }

    internal static void RegisterTypeRef(ISet<string>? collector, string? typeRef)
    {
        if (collector == null)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(typeRef))
        {
            return;
        }

        var trimmed = typeRef.Trim();
        var parts = trimmed.Split('.', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length < 2)
        {
            return;
        }

        var name = parts[^1];
        var schema = parts[^2];
        if (string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(name))
        {
            return;
        }

        if (string.Equals(schema, "sys", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        collector.Add(BuildKey(schema, name));

        if (parts.Length >= 3)
        {
            var catalog = parts[^3];
            if (!string.IsNullOrWhiteSpace(catalog))
            {
                collector.Add(string.Concat(catalog.Trim(), ".", BuildKey(schema, name)));
            }
        }
    }

    internal static void RegisterTableRef(ISet<string>? collector, ProcedureResultColumn column)
    {
        if (collector == null || column == null)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(column.SourceCatalog) || string.IsNullOrWhiteSpace(column.SourceSchema) || string.IsNullOrWhiteSpace(column.SourceTable))
        {
            return;
        }

        var key = BuildTableRefKey(column.SourceCatalog!, column.SourceSchema!, column.SourceTable!);
        if (!string.IsNullOrWhiteSpace(key))
        {
            collector.Add(key);
        }
    }

    internal static string? NormalizeSqlTypeName(string? sqlTypeName)
    {
        if (string.IsNullOrWhiteSpace(sqlTypeName))
        {
            return null;
        }

        var trimmed = sqlTypeName.Trim();
        var parenthesisIndex = trimmed.IndexOf('(');
        if (parenthesisIndex >= 0)
        {
            trimmed = trimmed[..parenthesisIndex];
        }

        var normalized = trimmed.Trim().ToLowerInvariant();
        return normalized.Length == 0 ? null : normalized;
    }

    internal static bool ShouldEmitIsNullable(bool value, string? typeRefOrTypeName)
        => ShouldEmitIsNullable(value ? (bool?)true : null, typeRefOrTypeName);

    internal static bool ShouldEmitIsNullable(bool? value, string? typeRefOrTypeName)
    {
        if (!value.HasValue || !value.Value)
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(typeRefOrTypeName))
        {
            return true;
        }

        var (schema, _) = SplitTypeRef(typeRefOrTypeName);
        if (string.IsNullOrWhiteSpace(schema))
        {
            return true;
        }
        return true;
    }

    internal static bool ShouldEmitMaxLength(int value, string? typeRef)
    {
        if (value <= 0)
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(typeRef))
        {
            return true;
        }

        var (schema, name) = SplitTypeRef(typeRef);
        if (!string.Equals(schema, "sys", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return !IsFixedSizeSysType(name);
    }

    internal static bool ShouldEmitMaxLength(int? value, string? typeRef)
    {
        if (!value.HasValue)
        {
            return false;
        }

        return ShouldEmitMaxLength(value.Value, typeRef);
    }

    internal static bool ShouldEmitPrecision(int? precision, string? typeRef)
    {
        if (!precision.HasValue || precision.Value <= 0)
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(typeRef))
        {
            return true;
        }

        var (schema, name) = SplitTypeRef(typeRef);
        if (!string.Equals(schema, "sys", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return name is "decimal" or "numeric" or "datetime2" or "datetimeoffset" or "time";
    }

    internal static bool ShouldEmitScale(int? scale, string? typeRef)
    {
        if (!scale.HasValue || scale.Value <= 0)
        {
            return false;
        }

        if (string.IsNullOrWhiteSpace(typeRef))
        {
            return true;
        }

        var (schema, name) = SplitTypeRef(typeRef);
        if (!string.Equals(schema, "sys", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        return name is "decimal" or "numeric" or "datetime2" or "datetimeoffset" or "time";
    }

    private static bool IsFixedSizeSysType(string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return false;
        }

        return name switch
        {
            "bigint" or
            "int" or
            "smallint" or
            "tinyint" or
            "bit" or
            "date" or
            "datetime" or
            "datetime2" or
            "datetimeoffset" or
            "smalldatetime" or
            "time" or
            "float" or
            "real" or
            "money" or
            "smallmoney" or
            "uniqueidentifier" or
            "rowversion" or
            "timestamp" or
            "sql_variant" or
            "hierarchyid" or
            "geometry" or
            "geography" or
            "xml" or
            "text" or
            "ntext" or
            "image" or
            "sysname" => true,
            _ => false
        };
    }

    internal static string BuildKey(string schema, string name)
    {
        if (string.IsNullOrWhiteSpace(schema))
        {
            return name ?? string.Empty;
        }

        return $"{schema}.{name}";
    }

    internal static string? ComposeSchemaObjectRef(string? schema, string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return null;
        }

        var cleanName = name.Trim();
        if (cleanName.Length == 0)
        {
            return null;
        }

        var cleanSchema = string.IsNullOrWhiteSpace(schema) ? null : schema.Trim();
        return cleanSchema != null ? string.Concat(cleanSchema, ".", cleanName) : cleanName;
    }

    internal static string BuildTableRefKey(string catalog, string schema, string name)
    {
        if (string.IsNullOrWhiteSpace(catalog) || string.IsNullOrWhiteSpace(schema) || string.IsNullOrWhiteSpace(name))
        {
            return string.Empty;
        }

        return string.Concat(catalog.Trim(), ".", schema.Trim(), ".", name.Trim());
    }

    internal static TableRefParts SplitTableRefParts(string? tableRef)
    {
        if (string.IsNullOrWhiteSpace(tableRef))
        {
            return default;
        }

        var parts = tableRef.Trim().Split('.', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
        if (parts.Length != 3)
        {
            return default;
        }

        var catalog = string.IsNullOrWhiteSpace(parts[0]) ? null : parts[0];
        var schema = string.IsNullOrWhiteSpace(parts[1]) ? null : parts[1];
        var name = string.IsNullOrWhiteSpace(parts[2]) ? null : parts[2];
        return new TableRefParts(catalog, schema, name);
    }

    internal static string BuildArtifactFileName(string schema, string name)
        => BuildArtifactFileName(null, schema, name);

    internal static string BuildArtifactFileName(string? catalog, string schema, string name)
    {
        var segments = new List<string>(3);
        if (!string.IsNullOrWhiteSpace(catalog))
        {
            segments.Add(NameSanitizer.SanitizeForFile(catalog));
        }

        var schemaSafe = NameSanitizer.SanitizeForFile(schema ?? string.Empty);
        var nameSafe = NameSanitizer.SanitizeForFile(name ?? string.Empty);

        if (!string.IsNullOrWhiteSpace(schemaSafe))
        {
            segments.Add(schemaSafe);
        }

        if (!string.IsNullOrWhiteSpace(nameSafe))
        {
            segments.Add(nameSafe);
        }

        if (segments.Count == 0)
        {
            return "artifact.json";
        }

        return string.Join('.', segments) + ".json";
    }

    internal static string? NormalizeSqlExpression(string? expression)
    {
        if (string.IsNullOrWhiteSpace(expression))
        {
            return null;
        }

        var normalized = expression.Trim();
        while (HasRedundantOuterParentheses(normalized))
        {
            normalized = normalized[1..^1].Trim();
        }

        return normalized.Length == 0 ? null : normalized;
    }

    private static bool HasRedundantOuterParentheses(string expression)
    {
        if (string.IsNullOrEmpty(expression) || expression.Length < 2)
        {
            return false;
        }

        if (expression[0] != '(' || expression[^1] != ')')
        {
            return false;
        }

        var depth = 0;
        for (var i = 0; i < expression.Length - 1; i++)
        {
            var ch = expression[i];
            if (ch == '(')
            {
                depth++;
            }
            else if (ch == ')')
            {
                depth--;
                if (depth == 0 && i < expression.Length - 1)
                {
                    return false;
                }
            }
        }

        return depth > 0;
    }

    internal static string ComputeHash(string content)
        => ComputeHash(Encoding.UTF8.GetBytes(content ?? string.Empty));

    internal static string ComputeHash(byte[] content)
        => ComputeHash(content.AsSpan());

    internal static string ComputeHash(ReadOnlySpan<byte> content)
    {
        var hashBytes = SHA256.HashData(content);
        return Convert.ToHexString(hashBytes).Substring(0, 16);
    }

    internal static async Task PersistSnapshotAsync(string filePath, byte[] content, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var tempPath = filePath + ".tmp";
        await File.WriteAllBytesAsync(tempPath, content, cancellationToken).ConfigureAwait(false);

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (File.Exists(filePath))
            {
                try
                {
                    File.Replace(tempPath, filePath, null);
                }
                catch (PlatformNotSupportedException)
                {
                    File.Copy(tempPath, filePath, overwrite: true);
                }
                catch (IOException)
                {
                    File.Copy(tempPath, filePath, overwrite: true);
                }
            }
            else
            {
                File.Move(tempPath, filePath);
            }
        }
        finally
        {
            if (File.Exists(tempPath))
            {
                File.Delete(tempPath);
            }
        }
    }

    internal static string? BuildSqlTypeName(FunctionParamRow parameter)
    {
        if (parameter == null)
        {
            return null;
        }

        return BuildSqlTypeNameCore(parameter.system_type_name ?? parameter.base_type_name, parameter.precision, parameter.scale, parameter.max_length, parameter.normalized_length);
    }

    internal static string? BuildSqlTypeName(FunctionColumnRow column)
    {
        if (column == null)
        {
            return null;
        }

        return BuildSqlTypeNameCore(column.system_type_name ?? column.base_type_name, column.precision, column.scale, column.max_length, column.normalized_length);
    }

    private static string? BuildSqlTypeNameCore(string? rawType, int precision, int scale, int maxLength, int normalizedLength)
    {
        var normalized = NormalizeSqlTypeName(rawType);
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return null;
        }

        if (normalized is "decimal" or "numeric")
        {
            if (precision > 0)
            {
                return string.Concat(normalized, "(", precision.ToString(), ",", Math.Max(0, scale).ToString(), ")");
            }
        }

        if (normalized is "datetime2" or "datetimeoffset" or "time")
        {
            if (scale > 0)
            {
                return string.Concat(normalized, "(", Math.Max(0, scale).ToString(), ")");
            }
        }

        if (normalized is "binary" or "varbinary" or "char" or "nchar" or "varchar" or "nvarchar")
        {
            if (maxLength == -1)
            {
                return string.Concat(normalized, "(max)");
            }

            var length = normalizedLength > 0 ? normalizedLength : maxLength;
            if (length > 0)
            {
                return string.Concat(normalized, "(", length.ToString(), ")");
            }
        }

        return normalized;
    }
}
