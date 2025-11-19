using Xtraq.Data.Models;
using Xtraq.Services;
using Xtraq.SnapshotBuilder.Models;
using Xtraq.SnapshotBuilder.Utils;
using Xtraq.Utils;

namespace Xtraq.SnapshotBuilder.Writers;

internal static class ProcedureSnapshotDocumentBuilder
{
    internal static byte[] BuildProcedureJson(
        ProcedureDescriptor descriptor,
        IReadOnlyList<StoredProcedureInput> parameters,
        ProcedureModel? procedure,
        ISet<string>? requiredTypeRefs,
        ISet<string>? requiredTableRefs,
        IJsonFunctionEnhancementService? jsonEnhancementService = null)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream, new JsonWriterOptions { Indented = true }))
        {
            writer.WriteStartObject();
            writer.WriteString("Schema", descriptor?.Schema ?? string.Empty);
            writer.WriteString("Name", descriptor?.Name ?? string.Empty);

            WriteParameters(writer, parameters, requiredTypeRefs);
            WriteResultSets(writer, procedure?.ResultSets, requiredTypeRefs, requiredTableRefs, jsonEnhancementService, descriptor);

            writer.WriteEndObject();
        }

        return stream.ToArray();
    }

    private static void WriteParameters(Utf8JsonWriter writer, IReadOnlyList<StoredProcedureInput> parameters, ISet<string>? requiredTypeRefs)
    {
        writer.WritePropertyName("Parameters");
        writer.WriteStartArray();
        if (parameters != null)
        {
            foreach (var input in parameters)
            {
                if (input == null)
                {
                    continue;
                }

                writer.WriteStartObject();
                var name = SnapshotWriterUtilities.NormalizeParameterName(input.Name);
                if (!string.IsNullOrWhiteSpace(name))
                {
                    writer.WriteString("Name", name);
                }

                var rawTypeRef = SnapshotWriterUtilities.BuildTypeRef(input);
                var normalizedTypeRef = TableTypeRefFormatter.Normalize(rawTypeRef);
                var typeRefToPersist = !string.IsNullOrWhiteSpace(normalizedTypeRef) ? normalizedTypeRef : rawTypeRef;
                if (!string.IsNullOrWhiteSpace(typeRefToPersist))
                {
                    writer.WriteString("TypeRef", typeRefToPersist);
                    SnapshotWriterUtilities.RegisterTypeRef(requiredTypeRefs, typeRefToPersist);
                }

                if (input.IsTableType)
                {
                    var candidateRef = string.IsNullOrWhiteSpace(normalizedTypeRef)
                        ? TableTypeRefFormatter.Normalize(TableTypeRefFormatter.Combine(input.UserTypeSchemaName, input.UserTypeName))
                        : normalizedTypeRef;

                    if (!string.IsNullOrWhiteSpace(candidateRef))
                    {
                        var (catalogSegment, schemaSegment, nameSegment) = TableTypeRefFormatter.Split(candidateRef);
                        writer.WriteString("TableTypeRef", candidateRef);
                        if (!string.IsNullOrWhiteSpace(catalogSegment))
                        {
                            writer.WriteString("TableTypeCatalog", catalogSegment);
                        }
                        if (!string.IsNullOrWhiteSpace(schemaSegment))
                        {
                            writer.WriteString("TableTypeSchema", schemaSegment);
                        }
                        else if (!string.IsNullOrWhiteSpace(input.UserTypeSchemaName))
                        {
                            writer.WriteString("TableTypeSchema", input.UserTypeSchemaName);
                        }

                        if (!string.IsNullOrWhiteSpace(nameSegment))
                        {
                            writer.WriteString("TableTypeName", nameSegment);
                        }
                        else if (!string.IsNullOrWhiteSpace(input.UserTypeName))
                        {
                            writer.WriteString("TableTypeName", input.UserTypeName);
                        }
                    }
                }

                if (!input.IsTableType)
                {
                    if (SnapshotWriterUtilities.ShouldEmitIsNullable(input.IsNullable, typeRefToPersist))
                    {
                        writer.WriteBoolean("IsNullable", true);
                    }

                    if (SnapshotWriterUtilities.ShouldEmitMaxLength(input.MaxLength, typeRefToPersist))
                    {
                        writer.WriteNumber("MaxLength", input.MaxLength);
                    }

                    var precision = input.Precision;
                    if (SnapshotWriterUtilities.ShouldEmitPrecision(precision, typeRefToPersist))
                    {
                        writer.WriteNumber("Precision", precision.GetValueOrDefault());
                    }

                    var scale = input.Scale;
                    if (SnapshotWriterUtilities.ShouldEmitScale(scale, typeRefToPersist))
                    {
                        writer.WriteNumber("Scale", scale.GetValueOrDefault());
                    }
                }

                if (input.IsOutput)
                {
                    writer.WriteBoolean("IsOutput", true);
                }

                if (input.HasDefaultValue)
                {
                    writer.WriteBoolean("HasDefaultValue", true);
                }

                writer.WriteEndObject();
            }
        }

        writer.WriteEndArray();
    }

    private static void WriteResultSets(
        Utf8JsonWriter writer,
        IReadOnlyList<ProcedureResultSet>? resultSets,
        ISet<string>? requiredTypeRefs,
        ISet<string>? requiredTableRefs,
        IJsonFunctionEnhancementService? jsonEnhancementService = null,
        ProcedureDescriptor? descriptor = null)
    {
        writer.WritePropertyName("ResultSets");
        writer.WriteStartArray();
        if (resultSets != null)
        {
            foreach (var set in resultSets)
            {
                if (set == null || !ShouldIncludeResultSet(set))
                {
                    continue;
                }

                writer.WriteStartObject();
                if (set.ReturnsJson)
                {
                    writer.WriteBoolean("ReturnsJson", true);
                }

                if (set.ReturnsJsonArray)
                {
                    writer.WriteBoolean("ReturnsJsonArray", true);
                }
                else if (set.ReturnsJson && !set.ReturnsJsonArray)
                {
                    writer.WriteBoolean("ReturnsJsonArray", false);
                }

                if (!string.IsNullOrWhiteSpace(set.JsonRootProperty))
                {
                    writer.WriteString("JsonRootProperty", set.JsonRootProperty);
                }

                if (set.JsonIncludeNullValues)
                {
                    writer.WriteBoolean("JsonIncludeNullValues", true);
                }

                var hasColumns = set.Columns != null && set.Columns.Count > 0;
                var procedureRef = BuildProcedureRef(set);
                if (!string.IsNullOrWhiteSpace(procedureRef))
                {
                    writer.WriteString("ProcedureRef", procedureRef);
                }

                if (set.HasSelectStar)
                {
                    writer.WriteBoolean("HasSelectStar", true);
                }

                if (hasColumns)
                {
                    writer.WritePropertyName("Columns");
                    writer.WriteStartArray();
                    foreach (var column in set.Columns!)
                    {
                        if (column == null)
                        {
                            continue;
                        }

                        WriteResultColumn(writer, column, requiredTypeRefs, requiredTableRefs, jsonEnhancementService, descriptor);
                    }

                    writer.WriteEndArray();
                }

                writer.WriteEndObject();
            }
        }

        writer.WriteEndArray();
    }

    private static void WriteResultColumn(
        Utf8JsonWriter writer,
        ProcedureResultColumn column,
        ISet<string>? requiredTypeRefs,
        ISet<string>? requiredTableRefs,
        IJsonFunctionEnhancementService? jsonEnhancementService = null,
        ProcedureDescriptor? descriptor = null)
    {
        writer.WriteStartObject();
        if (!string.IsNullOrWhiteSpace(column.Name))
        {
            writer.WriteString("Name", column.Name);
        }

        if (!string.IsNullOrWhiteSpace(column.Alias))
        {
            writer.WriteString("Alias", column.Alias);
        }

        SnapshotWriterUtilities.RegisterTableRef(requiredTableRefs, column);

        var typeRef = SnapshotWriterUtilities.BuildTypeRef(column);
        var userTypeRef = column.UserTypeRef;
        if (!string.IsNullOrWhiteSpace(userTypeRef))
        {
            var (catalogSegment, schemaSegment, nameSegment) = TypeRefUtilities.Split(userTypeRef);
            var hasUserSchema = !string.IsNullOrWhiteSpace(schemaSegment);
            var hasUserName = !string.IsNullOrWhiteSpace(nameSegment);
            var isSystemSchema = hasUserSchema && string.Equals(schemaSegment, "sys", StringComparison.OrdinalIgnoreCase);

            if (hasUserSchema && hasUserName && !isSystemSchema)
            {
                writer.WriteString("UserTypeRef", userTypeRef);
                SnapshotWriterUtilities.RegisterTypeRef(requiredTypeRefs, userTypeRef);

                if (!string.IsNullOrWhiteSpace(typeRef) && string.Equals(typeRef, userTypeRef, StringComparison.OrdinalIgnoreCase))
                {
                    typeRef = null;
                }
            }
        }

        if (!string.IsNullOrWhiteSpace(typeRef) && column.ReturnsJson != true && column.IsNestedJson != true)
        {
            writer.WriteString("TypeRef", typeRef);
            SnapshotWriterUtilities.RegisterTypeRef(requiredTypeRefs, typeRef);
        }

        if (column.IsNestedJson == true && column.ReturnsJson != true)
        {
            writer.WriteBoolean("IsNestedJson", true);
        }

        if (column.ReturnsJson == true)
        {
            writer.WriteBoolean("ReturnsJson", true);
        }

        if (column.ReturnsJsonArray == true)
        {
            writer.WriteBoolean("ReturnsJsonArray", true);
        }
        else if (column.ReturnsJson == true && column.ReturnsJsonArray == false)
        {
            writer.WriteBoolean("ReturnsJsonArray", false);
        }

        if (column.ReturnsUnknownJson == true)
        {
            writer.WriteBoolean("ReturnsUnknownJson", true);
        }

        if (!string.IsNullOrWhiteSpace(column.JsonRootProperty))
        {
            writer.WriteString("JsonRootProperty", column.JsonRootProperty);
        }

        if (column.JsonIncludeNullValues == true)
        {
            writer.WriteBoolean("JsonIncludeNullValues", true);
        }

        if (!string.IsNullOrWhiteSpace(column.JsonElementClrType))
        {
            writer.WriteString("JsonElementClrType", column.JsonElementClrType);
        }

        if (!string.IsNullOrWhiteSpace(column.JsonElementSqlType))
        {
            writer.WriteString("JsonElementSqlType", column.JsonElementSqlType);
        }

        var normalizedCandidate = SnapshotWriterUtilities.NormalizeSqlTypeName(column.SqlTypeName);
        if (string.IsNullOrWhiteSpace(normalizedCandidate))
        {
            normalizedCandidate = SnapshotWriterUtilities.NormalizeSqlTypeName(column.CastTargetType);
        }

        var attemptedUserTypeResolution = false;
        string? sqlTypeName = null;

        if (!string.IsNullOrWhiteSpace(column.UserTypeRef))
        {
            var candidate = normalizedCandidate;
            if (string.IsNullOrWhiteSpace(candidate))
            {
                candidate = SnapshotWriterUtilities.NormalizeSqlTypeName(column.UserTypeRef);
            }

            if (!string.IsNullOrWhiteSpace(candidate))
            {
                attemptedUserTypeResolution = true;
                sqlTypeName = TryResolveUserDefinedType(column, candidate);

                if (string.IsNullOrWhiteSpace(sqlTypeName))
                {
                    var fallbackCandidate = SnapshotWriterUtilities.NormalizeSqlTypeName(column.UserTypeRef);
                    if (!string.IsNullOrWhiteSpace(fallbackCandidate) && !string.Equals(fallbackCandidate, candidate, StringComparison.OrdinalIgnoreCase))
                    {
                        sqlTypeName = TryResolveUserDefinedType(column, fallbackCandidate);
                    }
                }
            }
        }

        sqlTypeName ??= DeriveSqlTypeName(column, typeRef, normalizedCandidate, allowUserTypeResolution: !attemptedUserTypeResolution);
        if (!string.IsNullOrWhiteSpace(sqlTypeName))
        {
            writer.WriteString("SqlTypeName", sqlTypeName);
        }

        var emitIsNullable = column.ForcedNullable == true || SnapshotWriterUtilities.ShouldEmitIsNullable(column.IsNullable, typeRef);
        if (emitIsNullable)
        {
            writer.WriteBoolean("IsNullable", true);
        }

        var columnMaxLength = column.MaxLength ?? column.CastTargetLength;
        if (SnapshotWriterUtilities.ShouldEmitMaxLength(columnMaxLength, typeRef))
        {
            writer.WriteNumber("MaxLength", columnMaxLength.GetValueOrDefault());
        }

        if (ShouldEmitNestedColumns(column))
        {
            writer.WritePropertyName("Columns");
            writer.WriteStartArray();
            foreach (var child in column.Columns)
            {
                if (child == null)
                {
                    continue;
                }

                WriteResultColumn(writer, child, requiredTypeRefs, requiredTableRefs, jsonEnhancementService, descriptor);
            }

            writer.WriteEndArray();
        }

        var functionRef = BuildFunctionRef(column, jsonEnhancementService, descriptor);
        if (!string.IsNullOrWhiteSpace(functionRef))
        {
            writer.WriteString("FunctionRef", functionRef);
        }

        if (column.DeferredJsonExpansion == true)
        {
            writer.WriteBoolean("DeferredJsonExpansion", true);
        }

        writer.WriteEndObject();
    }

    private static bool ShouldEmitNestedColumns(ProcedureResultColumn column)
    {
        if (column == null || column.Columns == null || column.Columns.Count == 0)
        {
            return false;
        }

        if (column.ReturnsJson == true || column.ReturnsJsonArray == true || column.IsNestedJson == true)
        {
            return true;
        }

        if (column.DeferredJsonExpansion == true)
        {
            return true;
        }

        foreach (var child in column.Columns)
        {
            if (child == null)
            {
                continue;
            }

            if (child.ReturnsJson == true || child.ReturnsJsonArray == true || child.IsNestedJson == true)
            {
                return true;
            }

            if (child.Columns != null && child.Columns.Count > 0)
            {
                return true;
            }
        }

        return false;
    }

    private static string? DeriveSqlTypeName(ProcedureResultColumn column, string? typeRef, string? normalizedCandidate = null, bool allowUserTypeResolution = true)
    {
        if (column == null)
        {
            return null;
        }

        if (column.ReturnsJson == true || column.IsNestedJson == true)
        {
            return null;
        }

        if (!string.IsNullOrWhiteSpace(typeRef))
        {
            var (schema, _) = SnapshotWriterUtilities.SplitTypeRef(typeRef);
            if (string.Equals(schema, "sys", StringComparison.OrdinalIgnoreCase))
            {
                return null;
            }
        }

        static string? NormalizeCandidate(string? raw)
        {
            if (string.IsNullOrWhiteSpace(raw))
            {
                return null;
            }

            return SnapshotWriterUtilities.NormalizeSqlTypeName(raw);
        }

        static bool IsSchemaQualified(string value)
            => value.IndexOf('.') >= 0 || value.IndexOf('[') >= 0;

        var candidate = normalizedCandidate ?? NormalizeCandidate(column.SqlTypeName);
        if (!string.IsNullOrWhiteSpace(candidate))
        {
            if (allowUserTypeResolution)
            {
                var resolved = TryResolveUserDefinedType(column, candidate);
                if (!string.IsNullOrWhiteSpace(resolved))
                {
                    return resolved;
                }
            }

            if (allowUserTypeResolution || !IsSchemaQualified(candidate))
            {
                return candidate;
            }

            candidate = null;
        }

        candidate = NormalizeCandidate(column.CastTargetType);
        if (!string.IsNullOrWhiteSpace(candidate))
        {
            return candidate;
        }

        var aggregate = column.AggregateFunction;
        if (string.IsNullOrWhiteSpace(aggregate))
        {
            aggregate = TryExtractFunctionName(column.RawExpression);
        }

        if (!string.IsNullOrWhiteSpace(aggregate))
        {
            switch (aggregate.Trim().ToLowerInvariant())
            {
                case "count":
                    return "int";
                case "count_big":
                    return "bigint";
                case "sum":
                case "avg":
                    return "decimal(18,2)";
                case "min":
                case "max":
                    if (column.HasIntegerLiteral)
                    {
                        return "int";
                    }

                    if (column.HasDecimalLiteral)
                    {
                        return "decimal(18,2)";
                    }

                    break;
            }
        }

        if (!string.IsNullOrWhiteSpace(column.RawExpression))
        {
            var raw = column.RawExpression.Trim();
            if (raw.StartsWith("EXISTS", StringComparison.OrdinalIgnoreCase))
            {
                return "bit";
            }

            if (LooksLikeBooleanCase(raw))
            {
                return "bit";
            }
        }

        return null;
    }

    private static string? TryResolveUserDefinedType(ProcedureResultColumn column, string normalizedCandidate)
    {
        if (column == null)
        {
            return null;
        }

        var debugEnabled = EnvironmentHelper.IsTrue("XTRAQ_DEBUG_USER_TYPE_RESOLUTION");

        var typeRef = column.UserTypeRef;
        if (string.IsNullOrWhiteSpace(typeRef))
        {
            typeRef = column.SqlTypeName;
        }

        if (string.IsNullOrWhiteSpace(typeRef))
        {
            typeRef = normalizedCandidate;
        }

        var parts = SnapshotWriterUtilities.SplitTypeRefParts(typeRef);
        if (string.IsNullOrWhiteSpace(parts.Schema) || string.Equals(parts.Schema, "sys", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        var length = column.MaxLength ?? column.CastTargetLength;
        var precision = column.CastTargetPrecision;
        var scale = column.CastTargetScale;

        foreach (var resolver in EnumerateTypeMetadataResolvers())
        {
            try
            {
                var resolved = resolver.Resolve(typeRef, length, precision, scale);
                if (!resolved.HasValue)
                {
                    if (debugEnabled)
                    {
                        Console.WriteLine($"[writer-type-resolve] miss typeRef={typeRef} candidate={normalizedCandidate}");
                    }
                    continue;
                }

                var formattedType = resolved.Value.SqlType;
                if (!string.IsNullOrWhiteSpace(formattedType))
                {
                    column.SqlTypeName = formattedType;
                    if (!column.MaxLength.HasValue && resolved.Value.MaxLength.HasValue)
                    {
                        column.MaxLength = resolved.Value.MaxLength;
                    }

                    if (!column.IsNullable.HasValue && resolved.Value.IsNullable.HasValue)
                    {
                        column.IsNullable = resolved.Value.IsNullable;
                    }

                    if (debugEnabled)
                    {
                        Console.WriteLine($"[writer-type-resolve] hit typeRef={typeRef} sqlType={formattedType}");
                    }
                    return formattedType;
                }

                var baseType = SnapshotWriterUtilities.NormalizeSqlTypeName(resolved.Value.BaseSqlType);
                if (string.IsNullOrWhiteSpace(baseType))
                {
                    if (debugEnabled)
                    {
                        Console.WriteLine($"[writer-type-resolve] empty-base typeRef={typeRef}");
                    }
                    continue;
                }

                column.SqlTypeName = baseType;

                if (!column.MaxLength.HasValue && resolved.Value.MaxLength.HasValue)
                {
                    column.MaxLength = resolved.Value.MaxLength;
                }

                if (!column.IsNullable.HasValue && resolved.Value.IsNullable.HasValue)
                {
                    column.IsNullable = resolved.Value.IsNullable;
                }

                if (debugEnabled)
                {
                    Console.WriteLine($"[writer-type-resolve] base-hit typeRef={typeRef} baseType={baseType}");
                }
                return baseType;
            }
            catch
            {
                if (debugEnabled)
                {
                    Console.WriteLine($"[writer-type-resolve] error typeRef={typeRef}");
                }
                // Ignore resolver failures and continue probing remaining roots.
            }
        }

        if (debugEnabled)
        {
            Console.WriteLine($"[writer-type-resolve] exhausted typeRef={typeRef}");
        }

        return null;
    }

    private static IEnumerable<Xtraq.Metadata.TypeMetadataResolver> EnumerateTypeMetadataResolvers()
    {
        var yielded = false;

        foreach (var root in SnapshotRootLocator.EnumerateSnapshotRoots())
        {
            Xtraq.Metadata.TypeMetadataResolver? resolver;
            try
            {
                resolver = SnapshotTypeResolverCache.Get(root);
            }
            catch
            {
                continue;
            }

            yielded = true;
            yield return resolver;
        }

        if (!yielded)
        {
            yield return new Xtraq.Metadata.TypeMetadataResolver();
        }
    }

    private static bool LooksLikeBooleanCase(string rawExpression)
    {
        if (string.IsNullOrWhiteSpace(rawExpression))
        {
            return false;
        }

        if (!rawExpression.StartsWith("CASE", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        static bool ContainsPattern(string source, string pattern)
            => source.IndexOf(pattern, StringComparison.OrdinalIgnoreCase) >= 0;

        var hasThenOneElseZero = ContainsPattern(rawExpression, " THEN 1") && ContainsPattern(rawExpression, " ELSE 0");
        var hasThenZeroElseOne = ContainsPattern(rawExpression, " THEN 0") && ContainsPattern(rawExpression, " ELSE 1");
        return hasThenOneElseZero || hasThenZeroElseOne;
    }

    private static string? TryExtractFunctionName(string? rawExpression)
    {
        if (string.IsNullOrWhiteSpace(rawExpression))
        {
            return null;
        }

        var match = Regex.Match(rawExpression, "^\\s*([A-Za-z0-9_]+)\\s*\\(");
        return match.Success ? match.Groups[1].Value : null;
    }

    private static bool ShouldIncludeResultSet(ProcedureResultSet set)
    {
        if (set == null)
        {
            return false;
        }

        if (set.ReturnsJson || set.ReturnsJsonArray)
        {
            return true;
        }

        if (set.Columns != null && set.Columns.Count > 0)
        {
            return true;
        }

        if (set.Reference != null && !string.IsNullOrWhiteSpace(set.Reference.Name))
        {
            return true;
        }

        return false;
    }

    private static string? BuildProcedureRef(ProcedureResultSet set)
    {
        if (set?.Reference != null && set.Reference.Kind == ProcedureReferenceKind.Procedure)
        {
            return SnapshotWriterUtilities.ComposeSchemaObjectRef(set.Reference.Schema, set.Reference.Name);
        }

        return null;
    }

    private static string? BuildFunctionRef(ProcedureResultColumn column, IJsonFunctionEnhancementService? jsonEnhancementService = null, ProcedureDescriptor? descriptor = null)
    {
        if (column?.Reference == null)
        {
            return null;
        }

        if (column.Reference.Kind != ProcedureReferenceKind.Function)
        {
            return null;
        }

        if (column.ReturnsUnknownJson == true)
        {
            return null;
        }

        var originalFunctionRef = SnapshotWriterUtilities.ComposeSchemaObjectRef(column.Reference.Schema, column.Reference.Name);

        if (IsBuiltInJsonFunction(column.Reference.Name))
        {
            return null;
        }

        return originalFunctionRef;
    }

    private static bool IsBuiltInJsonFunction(string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            return false;
        }

        return name.Equals("JSON_QUERY", StringComparison.OrdinalIgnoreCase) ||
               name.Equals("JSON_VALUE", StringComparison.OrdinalIgnoreCase) ||
               name.Equals("JSON_MODIFY", StringComparison.OrdinalIgnoreCase);
    }
}
