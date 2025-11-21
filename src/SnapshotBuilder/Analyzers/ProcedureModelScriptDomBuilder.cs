using Microsoft.SqlServer.TransactSql.ScriptDom;
using Xtraq.Metadata;
using Xtraq.Schema;
using Xtraq.SnapshotBuilder.Models;
using Xtraq.SnapshotBuilder.Utils;
using Xtraq.Utils;
using ProcedureReferenceModel = Xtraq.SnapshotBuilder.Models.ProcedureReference;
using ScriptDomParameterModifier = Microsoft.SqlServer.TransactSql.ScriptDom.ParameterModifier;

namespace Xtraq.SnapshotBuilder.Analyzers;

/// <summary>
/// Builds <see cref="ProcedureModel"/> instances directly from ScriptDom ASTs without relying on the legacy StoredProcedureContentModel.
/// </summary>
internal sealed class ProcedureModelScriptDomBuilder : IProcedureAstBuilder, IProcedureModelBuilder
{
    private readonly IEnhancedSchemaMetadataProvider? _schemaMetadataProvider;

    public ProcedureModelScriptDomBuilder(IEnhancedSchemaMetadataProvider? schemaMetadataProvider = null)
    {
        _schemaMetadataProvider = schemaMetadataProvider;
    }

    /// <summary>
    /// Builds a <see cref="ProcedureModel"/> using the legacy model builder signature.
    /// </summary>
    /// <param name="definition">The raw stored procedure definition.</param>
    /// <param name="defaultSchema">Default schema used for unqualified identifiers.</param>
    /// <param name="defaultCatalog">Default catalog used for unqualified identifiers.</param>
    /// <param name="verboseParsing">Enables verbose diagnostics during parsing.</param>
    /// <returns>The constructed <see cref="ProcedureModel"/> or <c>null</c> when parsing fails.</returns>
    public ProcedureModel? Build(string? definition, string? defaultSchema, string? defaultCatalog, bool verboseParsing)
    {
        return Build(new ProcedureAstBuildRequest(definition, defaultSchema, defaultCatalog, verboseParsing));
    }

    public ProcedureModel? Build(ProcedureAstBuildRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var definition = request.Definition;
        if (string.IsNullOrWhiteSpace(definition))
        {
            return null;
        }

        var fragment = ProcedureModelScriptDomParser.Parse(definition);
        if (fragment == null)
        {
            return null;
        }

        var model = new ProcedureModel();
        var visitor = new ProcedureVisitor(request.DefaultSchema, request.DefaultCatalog, request.VerboseParsing, _schemaMetadataProvider);
        fragment.Accept(visitor);

        model.ExecutedProcedures.AddRange(visitor.ExecutedProcedures);
        model.ResultSets.AddRange(visitor.ResultSets);

        return model;
    }

    private sealed class ProcedureVisitor : TSqlFragmentVisitor
    {
        private sealed class ColumnSourceInfo
        {
            public string? Catalog { get; init; }
            public string? Schema { get; init; }
            public string? Table { get; init; }
            public string? Column { get; init; }
            public ProcedureReferenceModel? Reference { get; init; }
            public bool? ReturnsJson { get; init; }
            public bool? ReturnsJsonArray { get; init; }
            public bool? ReturnsUnknownJson { get; init; }
            public bool? IsNestedJson { get; init; }
            public string? SqlTypeName { get; init; }
            public int? MaxLength { get; init; }
            public int? Precision { get; init; }
            public int? Scale { get; init; }
            public bool? IsNullable { get; init; }
            public string? UserTypeSchema { get; init; }
            public string? UserTypeName { get; init; }
            public string? CastTargetType { get; init; }
            public int? CastTargetLength { get; init; }
            public int? CastTargetPrecision { get; init; }
            public int? CastTargetScale { get; init; }
        }

        private sealed class ColumnSourceCandidate
        {
            public string? Catalog { get; init; }
            public string? Schema { get; init; }
            public string? Table { get; init; }
            public string Column { get; init; } = string.Empty;
            public ProcedureResultColumn? Probe { get; init; }
        }

        private sealed class TableAliasInfo
        {
            public string? Catalog { get; set; }
            public string? Schema { get; set; }
            public string Name { get; set; } = string.Empty;
            public Dictionary<string, ColumnSourceInfo>? Columns { get; set; }
            public bool IsCte { get; set; }
            public bool ForceNullableColumns { get; set; }
            public bool IsFunction { get; set; }
        }

        private sealed class JsonPathBinding
        {
            public TableAliasInfo Alias { get; init; } = null!;
            public Dictionary<string, ColumnSourceInfo> Columns { get; init; } = new(StringComparer.OrdinalIgnoreCase);
        }

        private sealed class FunctionInfo
        {
            public string Schema { get; init; } = "dbo";
            public string Name { get; init; } = string.Empty;
            public IReadOnlyList<FunctionColumnInfo> Columns { get; init; } = Array.Empty<FunctionColumnInfo>();
        }

        private sealed class FunctionColumnInfo
        {
            public string Name { get; init; } = string.Empty;
            public string? TypeRef { get; init; }
            public string? SqlTypeName { get; init; }
            public int? MaxLength { get; init; }
            public int? Precision { get; init; }
            public int? Scale { get; init; }
            public bool? IsNullable { get; init; }
        }

        private readonly string? _defaultSchema;
        private readonly string? _defaultCatalog;
        private readonly bool _verboseParsing;
        private readonly IEnhancedSchemaMetadataProvider? _schemaMetadataProvider;
        private readonly List<ProcedureExecutedProcedureCall> _executedProcedures = new();
        private readonly List<ProcedureResultSet> _resultSets = new();
        private readonly Stack<Dictionary<string, TableAliasInfo>> _aliasScopes = new();
        private readonly Dictionary<string, Dictionary<string, ColumnSourceInfo>> _cteColumnCache = new(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<QuerySpecification> _cteQuerySpecifications = new();
        private readonly Dictionary<string, TableTypeBinding> _tableTypeBindings = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, ColumnSourceInfo> _scalarVariableMetadata = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, Dictionary<string, ColumnSourceInfo>> _fallbackTableColumnCache = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, Dictionary<string, ColumnSourceInfo>> _fallbackFunctionColumnCache = new(StringComparer.OrdinalIgnoreCase);
        private bool _inTopLevelSelect;
        private int _selectStatementDepth;
        private int _selectInsertSourceDepth;

        private sealed class TableTypeBinding
        {
            public string? Catalog { get; init; }
            public string? Schema { get; init; }
            public string Name { get; init; } = string.Empty;
            public Dictionary<string, ColumnSourceInfo> Columns { get; init; } = new(StringComparer.OrdinalIgnoreCase);
            public ProcedureReferenceModel? Reference { get; init; }
        }

        private static readonly Lazy<Dictionary<string, TableTypeInfo>> TableTypeMetadataCache = new(LoadTableTypeMetadata, LazyThreadSafetyMode.ExecutionAndPublication);
        private static readonly Lazy<IReadOnlyList<TypeMetadataResolver>> TypeMetadataResolvers = new(LoadTypeMetadataResolvers, LazyThreadSafetyMode.ExecutionAndPublication);
        private static readonly Lazy<IReadOnlyDictionary<string, FunctionInfo>> FunctionMetadataLookup = new(LoadFunctionMetadata, LazyThreadSafetyMode.ExecutionAndPublication);

        private static readonly IReadOnlyDictionary<string, int> SqlTypePrecedence = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase)
        {
            ["sql_variant"] = 100,
            ["datetimeoffset"] = 95,
            ["datetime2"] = 94,
            ["datetime"] = 93,
            ["smalldatetime"] = 92,
            ["date"] = 91,
            ["time"] = 90,
            ["float"] = 85,
            ["real"] = 84,
            ["decimal"] = 83,
            ["numeric"] = 83,
            ["money"] = 82,
            ["smallmoney"] = 81,
            ["bigint"] = 80,
            ["int"] = 79,
            ["smallint"] = 78,
            ["tinyint"] = 77,
            ["bit"] = 76,
            ["ntext"] = 75,
            ["text"] = 74,
            ["image"] = 73,
            ["nvarchar"] = 72,
            ["nchar"] = 71,
            ["varchar"] = 70,
            ["char"] = 69,
            ["varbinary"] = 68,
            ["binary"] = 67,
            ["uniqueidentifier"] = 66
        };

        public ProcedureVisitor(string? defaultSchema, string? defaultCatalog, bool verboseParsing, IEnhancedSchemaMetadataProvider? schemaMetadataProvider)
        {
            _defaultSchema = string.IsNullOrWhiteSpace(defaultSchema) ? null : defaultSchema;
            _defaultCatalog = string.IsNullOrWhiteSpace(defaultCatalog) ? null : defaultCatalog;
            _verboseParsing = verboseParsing;
            _schemaMetadataProvider = schemaMetadataProvider;
        }

        public IReadOnlyList<ProcedureExecutedProcedureCall> ExecutedProcedures => _executedProcedures;
        public IReadOnlyList<ProcedureResultSet> ResultSets => _resultSets;

        public override void ExplicitVisit(WithCtesAndXmlNamespaces node)
        {
            ProcessCommonTableExpressions(node);
            base.ExplicitVisit(node);
        }

        public override void ExplicitVisit(ExecuteSpecification node)
        {
            if (node?.ExecutableEntity is ExecutableProcedureReference procedureRef)
            {
                var call = MapProcedureReference(procedureRef.ProcedureReference?.ProcedureReference?.Name);
                if (call != null)
                {
                    _executedProcedures.Add(call);
                }
            }

            base.ExplicitVisit(node);
        }

        public override void ExplicitVisit(SelectStatement node)
        {
            if (node?.WithCtesAndXmlNamespaces != null)
            {
                ProcessCommonTableExpressions(node.WithCtesAndXmlNamespaces);
            }

            _selectStatementDepth++;
            base.ExplicitVisit(node);
            _selectStatementDepth--;
        }

        public override void ExplicitVisit(DeclareVariableStatement node)
        {
            if (node?.Declarations != null)
            {
                foreach (var declaration in node.Declarations)
                {
                    if (declaration == null)
                    {
                        continue;
                    }

                    var variableName = NormalizeVariableName(declaration.VariableName?.Value);
                    if (string.IsNullOrWhiteSpace(variableName))
                    {
                        continue;
                    }

                    var dataType = declaration.DataType;
                    if (dataType is UserDataTypeReference userType)
                    {
                        var (schema, name) = ExtractSchemaAndName(userType.Name);
                        var binding = CreateTableTypeBinding(schema, name, _defaultCatalog);
                        if (binding != null)
                        {
                            _tableTypeBindings[variableName] = binding;
                            continue;
                        }

                        var scalarInfo = BuildScalarVariableMetadata(userType, schema, name);
                        if (scalarInfo != null)
                        {
                            _scalarVariableMetadata[variableName] = scalarInfo;
                        }

                        continue;
                    }

                    var tableBinding = TryBuildInlineTableVariableBinding(variableName, dataType);
                    if (tableBinding != null)
                    {
                        _tableTypeBindings[variableName] = tableBinding;
                        continue;
                    }

                    var info = BuildScalarVariableMetadata(dataType, null, null);
                    if (info != null)
                    {
                        _scalarVariableMetadata[variableName] = info;
                    }
                }
            }

            base.ExplicitVisit(node);
        }

        public override void ExplicitVisit(DeclareTableVariableStatement node)
        {
            if (node?.Body?.VariableName?.Value is not string rawVariable || string.IsNullOrWhiteSpace(rawVariable))
            {
                base.ExplicitVisit(node);
                return;
            }

            var variableName = NormalizeVariableName(rawVariable);
            var columnDefinitions = node.Body?.Definition?.ColumnDefinitions;
            if (columnDefinitions == null || columnDefinitions.Count == 0)
            {
                base.ExplicitVisit(node);
                return;
            }

            var binding = BuildInlineTableVariableBinding(variableName, columnDefinitions);
            if (binding != null)
            {
                _tableTypeBindings[variableName] = binding;
            }

            base.ExplicitVisit(node);
        }

        public override void ExplicitVisit(ProcedureParameter node)
        {
            if (node == null)
            {
                base.ExplicitVisit(node);
                return;
            }

            var variableName = NormalizeVariableName(node.VariableName?.Value);
            if (!string.IsNullOrWhiteSpace(variableName))
            {
                ColumnSourceInfo? scalarInfo = null;

                if (node.DataType is UserDataTypeReference userType)
                {
                    var (schema, name) = ExtractSchemaAndName(userType.Name);
                    if (node.Modifier == ScriptDomParameterModifier.ReadOnly)
                    {
                        var binding = CreateTableTypeBinding(schema, name, _defaultCatalog);
                        if (binding != null)
                        {
                            _tableTypeBindings[variableName] = binding;
                        }
                        else
                        {
                            scalarInfo = BuildScalarVariableMetadata(userType, schema, name);
                        }
                    }
                    else
                    {
                        scalarInfo = BuildScalarVariableMetadata(userType, schema, name);
                    }
                }
                else
                {
                    scalarInfo = BuildScalarVariableMetadata(node.DataType, null, null);
                }

                if (scalarInfo != null)
                {
                    _scalarVariableMetadata[variableName] = scalarInfo;
                }
            }

            base.ExplicitVisit(node);
        }

        public override void ExplicitVisit(SelectInsertSource node)
        {
            _selectInsertSourceDepth++;
            base.ExplicitVisit(node);
            _selectInsertSourceDepth--;
        }

        public override void ExplicitVisit(MergeStatement node)
        {
            if (node?.MergeSpecification != null)
            {
                ProcessMergeOutput(node.MergeSpecification);
            }

            base.ExplicitVisit(node);
        }

        public override void ExplicitVisit(QuerySpecification node)
        {
            var shouldCapture = ShouldCaptureResultSet();
            if (_cteQuerySpecifications.Contains(node))
            {
                shouldCapture = false;
            }
            var wasTopLevel = _inTopLevelSelect;

            Dictionary<string, TableAliasInfo>? currentScope = null;
            if (node?.FromClause != null)
            {
                currentScope = BuildAliasScope(node.FromClause);
                if (currentScope.Count > 0)
                {
                    _aliasScopes.Push(currentScope);
                    if (IsAliasDebugEnabled())
                    {
                        Console.WriteLine($"[alias-scope-push] depth={_aliasScopes.Count} keys={string.Join(",", currentScope.Keys)}");
                    }
                }
            }

            var producesVisibleResult = ProducesVisibleResult(node);
            var shouldMarkTopLevel = shouldCapture && !_inTopLevelSelect;

            if (shouldMarkTopLevel)
            {
                _inTopLevelSelect = true;
                if (producesVisibleResult)
                {
                    _resultSets.Add(BuildResultSet(node));
                }
            }

            base.ExplicitVisit(node);

            if (currentScope is { Count: > 0 })
            {
                if (IsAliasDebugEnabled())
                {
                    Console.WriteLine($"[alias-scope-pop] depth={_aliasScopes.Count}");
                }

                _aliasScopes.Pop();
            }

            if (shouldMarkTopLevel)
            {
                _inTopLevelSelect = wasTopLevel;
            }
        }

        private static bool ProducesVisibleResult(QuerySpecification? query)
        {
            if (query?.SelectElements == null || query.SelectElements.Count == 0)
            {
                return false;
            }

            foreach (var element in query.SelectElements)
            {
                if (element is SelectScalarExpression || element is SelectStarExpression)
                {
                    return true;
                }
            }

            return false;
        }

        private bool ShouldCaptureResultSet()
        {
            if (_selectInsertSourceDepth > 0)
            {
                return false;
            }

            return _selectStatementDepth == 1;
        }

        private void ProcessMergeOutput(MergeSpecification merge)
        {
            if (merge == null)
            {
                return;
            }

            var output = merge.OutputClause;
            if (output == null)
            {
                return;
            }

            if (OutputClauseTargetsIntoTable(output))
            {
                return;
            }

            if (output.SelectColumns == null || output.SelectColumns.Count == 0)
            {
                return;
            }

            var scalarElements = new List<SelectScalarExpression>(output.SelectColumns.Count);
            foreach (var element in output.SelectColumns)
            {
                if (element is SelectScalarExpression scalar)
                {
                    scalarElements.Add(scalar);
                }
            }

            if (scalarElements.Count == 0)
            {
                return;
            }

            var scope = BuildMergeOutputScope(merge);
            var resultSet = BuildMergeOutputResultSet(scalarElements, scope);

            if (resultSet.Columns.Count == 0)
            {
                return;
            }

            _resultSets.Add(resultSet);
        }

        private static bool OutputClauseTargetsIntoTable(OutputClause output)
        {
            if (output == null)
            {
                return false;
            }

            var intoTableProperty = output.GetType().GetProperty("IntoTable", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            if (intoTableProperty != null)
            {
                var intoValue = intoTableProperty.GetValue(output);
                if (intoValue != null)
                {
                    return true;
                }
            }

            var legacyIntoProperty = output.GetType().GetProperty("OutputIntoTable", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            if (legacyIntoProperty != null)
            {
                var intoValue = legacyIntoProperty.GetValue(output);
                if (intoValue != null)
                {
                    return true;
                }
            }

            return false;
        }

        private ProcedureExecutedProcedureCall? MapProcedureReference(MultiPartIdentifier? identifier)
        {
            if (identifier == null || identifier.Identifiers.Count == 0)
            {
                return null;
            }

            var name = identifier.Identifiers[^1].Value;
            if (string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            string? catalog = null;
            string? schema = null;
            if (identifier.Identifiers.Count >= 2)
            {
                var schemaIdentifier = identifier.Identifiers[^2].Value;
                schema = string.IsNullOrWhiteSpace(schemaIdentifier) ? null : schemaIdentifier;
            }

            if (identifier.Identifiers.Count >= 3)
            {
                var catalogIdentifier = identifier.Identifiers[^3].Value;
                catalog = string.IsNullOrWhiteSpace(catalogIdentifier) ? null : catalogIdentifier;
            }

            schema ??= _defaultSchema;
            catalog ??= _defaultCatalog;

            return new ProcedureExecutedProcedureCall
            {
                Catalog = catalog,
                Schema = schema,
                Name = name,
                IsCaptured = true
            };
        }

        private ProcedureResultSet BuildResultSet(QuerySpecification? query)
        {
            var resultSet = new ProcedureResultSet();
            if (query?.SelectElements == null || query.SelectElements.Count == 0)
            {
                return resultSet;
            }

            var usedColumnNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var jsonPathBindings = new Dictionary<string, JsonPathBinding>(StringComparer.OrdinalIgnoreCase);
            var aliasHints = new Dictionary<string, TableAliasInfo>(StringComparer.OrdinalIgnoreCase);

            foreach (var element in query.SelectElements)
            {
                if (element is SelectScalarExpression scalar)
                {
                    var column = BuildColumnFromScalar(scalar);
                    if (column != null)
                    {
                        var activeScope = _aliasScopes.Count > 0 ? _aliasScopes.Peek() : null;
                        NormalizeColumnSource(column, activeScope);
                        RegisterAliasHint(column, activeScope, aliasHints);
                        ApplyAliasColumnFallback(column, activeScope, aliasHints);
                        ApplyJsonPathMetadata(column, jsonPathBindings);
                        RegisterJsonPathBinding(column, jsonPathBindings, activeScope);
                        EnsureUniqueColumnName(column, usedColumnNames);
                        resultSet.Columns.Add(column);
                    }
                }
                else if (element is SelectStarExpression star)
                {
                    resultSet.HasSelectStar = true;
                    var activeScope = _aliasScopes.Count > 0 ? _aliasScopes.Peek() : null;
                    var expandedColumns = ExpandSelectStarColumns(star, activeScope);

                    if (expandedColumns.Count == 0)
                    {
                        var qualifier = star.Qualifier;
                        if (qualifier != null && qualifier.Identifiers != null && qualifier.Identifiers.Count > 0)
                        {
                            var column = new ProcedureResultColumn
                            {
                                Name = qualifier.Identifiers[^1].Value,
                                ExpressionKind = ProcedureResultColumnExpressionKind.ColumnRef
                            };
                            EnsureUniqueColumnName(column, usedColumnNames);
                            resultSet.Columns.Add(column);
                        }

                        continue;
                    }

                    foreach (var expanded in expandedColumns)
                    {
                        NormalizeColumnSource(expanded, activeScope);
                        RegisterAliasHint(expanded, activeScope, aliasHints);
                        ApplyAliasColumnFallback(expanded, activeScope, aliasHints);
                        ApplyJsonPathMetadata(expanded, jsonPathBindings);
                        RegisterJsonPathBinding(expanded, jsonPathBindings, activeScope);
                        EnsureUniqueColumnName(expanded, usedColumnNames);
                        resultSet.Columns.Add(expanded);
                    }
                }
            }

            ApplyResultSetMetadata(resultSet, query);
            ApplyNullLiteralFallback(resultSet);

            return resultSet;
        }

        private void ApplyResultSetMetadata(ProcedureResultSet resultSet, QuerySpecification? query)
        {
            if (resultSet == null || query == null)
            {
                return;
            }

            if (query.ForClause is not JsonForClause jsonClause)
            {
                return;
            }

            resultSet.ReturnsJson = true;

            var withoutArrayWrapper = false;
            var includeNullValues = false;
            string? rootProperty = null;

            if (jsonClause.Options != null)
            {
                foreach (var option in jsonClause.Options)
                {
                    if (option == null)
                    {
                        continue;
                    }

                    switch (option.OptionKind)
                    {
                        case JsonForClauseOptions.WithoutArrayWrapper:
                            withoutArrayWrapper = true;
                            break;
                        case JsonForClauseOptions.IncludeNullValues:
                            includeNullValues = true;
                            break;
                        case JsonForClauseOptions.Root when option.Value is Literal literal:
                            rootProperty ??= ExtractJsonLiteralValue(literal);
                            break;
                    }
                }
            }

            resultSet.ReturnsJsonArray = !withoutArrayWrapper;
            resultSet.JsonIncludeNullValues = includeNullValues;
            if (!string.IsNullOrWhiteSpace(rootProperty))
            {
                resultSet.JsonRootProperty ??= rootProperty;
            }

            if (resultSet.Columns == null || resultSet.Columns.Count == 0)
            {
                return;
            }

            foreach (var column in resultSet.Columns)
            {
                ApplyJsonNullLiteralFallback(column);
            }
        }

        private static void ApplyJsonNullLiteralFallback(ProcedureResultColumn? column)
        {
            if (column == null)
            {
                return;
            }

            if (column.HasNullLiteral && string.IsNullOrWhiteSpace(column.SqlTypeName) && string.IsNullOrWhiteSpace(column.UserTypeName))
            {
                column.SqlTypeName = "nvarchar(max)";
                column.IsNullable ??= true;
            }

            if (column.Columns == null || column.Columns.Count == 0)
            {
                return;
            }

            foreach (var nested in column.Columns)
            {
                ApplyJsonNullLiteralFallback(nested);
            }
        }

        private static void ApplyNullLiteralFallback(ProcedureResultSet resultSet)
        {
            if (resultSet == null || resultSet.ReturnsJson == true)
            {
                return;
            }

            if (resultSet.Columns == null || resultSet.Columns.Count == 0)
            {
                return;
            }

            foreach (var column in resultSet.Columns)
            {
                ApplyNullLiteralFallback(column);
            }
        }

        private static void ApplyNullLiteralFallback(ProcedureResultColumn? column)
        {
            if (column == null)
            {
                return;
            }

            if (column.HasNullLiteral && string.IsNullOrWhiteSpace(column.SqlTypeName) && string.IsNullOrWhiteSpace(column.UserTypeName))
            {
                column.SqlTypeName = column.ReturnsJson == true ? "nvarchar(max)" : "int";
                column.IsNullable ??= true;
            }

            if (column.Columns == null || column.Columns.Count == 0)
            {
                return;
            }

            foreach (var nested in column.Columns)
            {
                ApplyNullLiteralFallback(nested);
            }
        }

        private void ApplyAliasColumnFallback(ProcedureResultColumn column, Dictionary<string, TableAliasInfo>? scope, Dictionary<string, TableAliasInfo> aliasHints)
        {
            var needsFallback = NeedsAliasColumnFallback(column);

            if (!needsFallback || scope == null || scope.Count == 0)
            {
                return;
            }

            var prefix = ExtractJsonPathPrefix(column.Name);
            var hintKey = string.IsNullOrWhiteSpace(prefix) ? string.Empty : prefix;
            aliasHints.TryGetValue(hintKey, out var preferredAlias);

            if (IsAliasDebugEnabled())
            {
                Console.WriteLine($"[alias-fallback-check] column={column.Name} needs={needsFallback} scope={scope.Count} prefix={prefix ?? "<root>"} preferred={(preferredAlias != null ? preferredAlias.Name : "<null>")}");
            }

            var leaf = ExtractJsonPathLeaf(column.Name) ?? column.Name;
            var candidateNames = new List<string?> { leaf };
            if (!string.IsNullOrWhiteSpace(column.Name) && !string.Equals(column.Name, leaf, StringComparison.OrdinalIgnoreCase))
            {
                candidateNames.Add(column.Name);
            }

            var matches = new List<(TableAliasInfo Alias, ColumnSourceInfo Info)>();
            var processedAliases = new HashSet<TableAliasInfo>();

            foreach (var entry in scope)
            {
                var aliasKey = entry.Key;
                var aliasInfo = entry.Value;
                if (aliasInfo == null)
                {
                    continue;
                }

                if (!processedAliases.Add(aliasInfo))
                {
                    continue;
                }

                if (preferredAlias != null && !ReferenceEquals(aliasInfo, preferredAlias))
                {
                    continue;
                }

                if (preferredAlias == null && !string.IsNullOrWhiteSpace(prefix) &&
                    !string.Equals(aliasKey, prefix, StringComparison.OrdinalIgnoreCase) &&
                    !string.Equals(aliasInfo.Name, prefix, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                EnsureAliasColumns(aliasInfo);
                if (aliasInfo.Columns == null || aliasInfo.Columns.Count == 0)
                {
                    continue;
                }

                foreach (var candidate in candidateNames)
                {
                    if (string.IsNullOrWhiteSpace(candidate))
                    {
                        continue;
                    }

                    if (!aliasInfo.Columns.TryGetValue(candidate!, out var columnInfo) || columnInfo == null)
                    {
                        continue;
                    }

                    matches.Add((aliasInfo, columnInfo));

                    if (IsAliasDebugEnabled())
                    {
                        Console.WriteLine($"[alias-fallback-match] column={column.Name} candidate={candidate} aliasKey={aliasKey} aliasName={aliasInfo.Name}");
                    }

                    break;
                }
            }

            if (IsAliasDebugEnabled())
            {
                Console.WriteLine($"[alias-fallback-result] column={column.Name} matches={matches.Count}");
            }

            if (matches.Count != 1)
            {
                return;
            }

            var (matchAlias, matchInfo) = matches[0];
            ApplyAliasColumnMetadata(column, matchAlias, matchInfo, leaf);
        }

        private static bool NeedsAliasColumnFallback(ProcedureResultColumn column)
        {
            if (column == null)
            {
                return false;
            }

            if (!string.IsNullOrWhiteSpace(column.SqlTypeName) ||
                !string.IsNullOrWhiteSpace(column.UserTypeName) ||
                !string.IsNullOrWhiteSpace(column.SourceColumn) ||
                column.Reference != null)
            {
                return false;
            }

            if (column.ExpressionKind != ProcedureResultColumnExpressionKind.Computed)
            {
                return false;
            }

            if (!string.IsNullOrWhiteSpace(column.CastTargetType))
            {
                return false;
            }

            return true;
        }

        private void EnsureAliasColumns(TableAliasInfo aliasInfo)
        {
            if (aliasInfo == null)
            {
                return;
            }

            if (aliasInfo.Columns != null && aliasInfo.Columns.Count > 0)
            {
                return;
            }

            var resolvedColumns = ResolveAliasColumns(aliasInfo);
            if (resolvedColumns != null && resolvedColumns.Count > 0)
            {
                aliasInfo.Columns = CloneAliasColumns(resolvedColumns, aliasInfo.ForceNullableColumns);
            }
        }

        private void ApplyAliasColumnMetadata(ProcedureResultColumn column, TableAliasInfo aliasInfo, ColumnSourceInfo columnInfo, string? fallbackColumnName)
        {
            if (column == null || aliasInfo == null || columnInfo == null)
            {
                return;
            }

            if (!string.IsNullOrWhiteSpace(columnInfo.Schema))
            {
                column.SourceSchema ??= columnInfo.Schema;
            }
            else if (!string.IsNullOrWhiteSpace(aliasInfo.Schema))
            {
                column.SourceSchema ??= aliasInfo.Schema;
            }

            if (!string.IsNullOrWhiteSpace(columnInfo.Catalog))
            {
                column.SourceCatalog ??= columnInfo.Catalog;
            }
            else if (!string.IsNullOrWhiteSpace(aliasInfo.Catalog))
            {
                column.SourceCatalog ??= aliasInfo.Catalog;
            }

            if (!string.IsNullOrWhiteSpace(columnInfo.Table))
            {
                column.SourceTable ??= columnInfo.Table;
            }
            else if (!string.IsNullOrWhiteSpace(aliasInfo.Name))
            {
                column.SourceTable ??= aliasInfo.Name;
            }

            if (!string.IsNullOrWhiteSpace(columnInfo.Column))
            {
                column.SourceColumn ??= columnInfo.Column;
            }
            else if (!string.IsNullOrWhiteSpace(fallbackColumnName))
            {
                column.SourceColumn ??= fallbackColumnName;
            }

            if (column.Reference == null && columnInfo.Reference != null)
            {
                column.Reference = CloneReference(columnInfo.Reference);
            }

            if (columnInfo.ReturnsJson.HasValue)
            {
                column.ReturnsJson ??= columnInfo.ReturnsJson;
            }

            if (columnInfo.ReturnsJsonArray.HasValue)
            {
                column.ReturnsJsonArray ??= columnInfo.ReturnsJsonArray;
            }

            if (columnInfo.ReturnsUnknownJson.HasValue)
            {
                column.ReturnsUnknownJson ??= columnInfo.ReturnsUnknownJson;
            }

            if (columnInfo.IsNestedJson.HasValue)
            {
                column.IsNestedJson ??= columnInfo.IsNestedJson;
            }

            ApplyTypeMetadata(column, columnInfo);

            if (aliasInfo.ForceNullableColumns)
            {
                column.IsNullable = true;
                column.ForcedNullable ??= true;
            }
        }

        private void ApplyJsonPathMetadata(ProcedureResultColumn column, Dictionary<string, JsonPathBinding> bindings)
        {
            if (column == null || string.IsNullOrWhiteSpace(column.Name) || bindings == null || bindings.Count == 0)
            {
                return;
            }

            if (!string.IsNullOrWhiteSpace(column.SqlTypeName) || !string.IsNullOrWhiteSpace(column.UserTypeName))
            {
                return;
            }

            var prefix = ExtractJsonPathPrefix(column.Name);
            if (string.IsNullOrWhiteSpace(prefix) || !bindings.TryGetValue(prefix, out var binding) || binding?.Columns == null || binding.Columns.Count == 0)
            {
                return;
            }

            var leaf = ExtractJsonPathLeaf(column.Name);
            if (string.IsNullOrWhiteSpace(leaf) || !binding.Columns.TryGetValue(leaf, out var sourceInfo) || sourceInfo == null)
            {
                return;
            }

            if (string.IsNullOrWhiteSpace(column.SourceSchema))
            {
                column.SourceSchema = sourceInfo.Schema ?? binding.Alias.Schema;
            }

            if (string.IsNullOrWhiteSpace(column.SourceCatalog))
            {
                column.SourceCatalog = sourceInfo.Catalog ?? binding.Alias.Catalog;
            }

            if (string.IsNullOrWhiteSpace(column.SourceTable))
            {
                column.SourceTable = sourceInfo.Table ?? binding.Alias.Name;
            }

            if (string.IsNullOrWhiteSpace(column.SourceColumn))
            {
                column.SourceColumn = sourceInfo.Column ?? leaf;
            }

            ApplyScalarMetadata(column, sourceInfo);
            column.IsNullable = true;
        }

        private void RegisterJsonPathBinding(ProcedureResultColumn column, Dictionary<string, JsonPathBinding> bindings, Dictionary<string, TableAliasInfo>? scope)
        {
            if (column == null || string.IsNullOrWhiteSpace(column.Name) || bindings == null)
            {
                return;
            }

            var prefix = ExtractJsonPathPrefix(column.Name);
            if (string.IsNullOrWhiteSpace(prefix) || bindings.ContainsKey(prefix))
            {
                return;
            }

            var aliasInfo = ResolveJsonPathAlias(column, scope);
            if (aliasInfo == null)
            {
                return;
            }

            if (aliasInfo.Columns == null || aliasInfo.Columns.Count == 0)
            {
                var resolvedColumns = ResolveAliasColumns(aliasInfo);
                if (resolvedColumns != null && resolvedColumns.Count > 0)
                {
                    aliasInfo.Columns = CloneAliasColumns(resolvedColumns, aliasInfo.ForceNullableColumns);
                }
            }

            if (aliasInfo.Columns == null || aliasInfo.Columns.Count == 0)
            {
                return;
            }

            bindings[prefix] = new JsonPathBinding
            {
                Alias = aliasInfo,
                Columns = aliasInfo.Columns
            };
        }

        private void RegisterAliasHint(ProcedureResultColumn column, Dictionary<string, TableAliasInfo>? scope, Dictionary<string, TableAliasInfo> hints)
        {
            if (column == null || hints == null)
            {
                return;
            }

            var prefix = ExtractJsonPathPrefix(column.Name);
            var key = string.IsNullOrWhiteSpace(prefix) ? string.Empty : prefix;

            if (hints.ContainsKey(key))
            {
                return;
            }

            if (string.IsNullOrWhiteSpace(column.SourceAlias) && string.IsNullOrWhiteSpace(column.SourceTable))
            {
                return;
            }

            var aliasInfo = ResolveJsonPathAlias(column, scope);
            if (aliasInfo == null)
            {
                return;
            }

            hints[key] = aliasInfo;
        }

        private TableAliasInfo? ResolveJsonPathAlias(ProcedureResultColumn column, Dictionary<string, TableAliasInfo>? scope)
        {
            if (column == null)
            {
                return null;
            }

            if (!string.IsNullOrWhiteSpace(column.SourceAlias))
            {
                if (scope != null && scope.TryGetValue(column.SourceAlias, out var scopedAlias) && scopedAlias != null)
                {
                    return scopedAlias;
                }

                var resolvedAlias = ResolveTableAlias(column.SourceAlias);
                if (resolvedAlias != null)
                {
                    return resolvedAlias;
                }
            }

            if (!string.IsNullOrWhiteSpace(column.SourceTable))
            {
                if (scope != null)
                {
                    foreach (var entry in scope.Values)
                    {
                        if (entry != null && string.Equals(entry.Name, column.SourceTable, StringComparison.OrdinalIgnoreCase))
                        {
                            return entry;
                        }
                    }
                }

                var resolvedTable = ResolveTableAlias(column.SourceTable);
                if (resolvedTable != null)
                {
                    return resolvedTable;
                }
            }

            return null;
        }

        private static string? ExtractJsonPathPrefix(string? columnName)
        {
            if (string.IsNullOrWhiteSpace(columnName))
            {
                return null;
            }

            var lastIndex = columnName.LastIndexOf('.');
            if (lastIndex <= 0)
            {
                return null;
            }

            return columnName[..lastIndex];
        }

        private static string? ExtractJsonPathLeaf(string? columnName)
        {
            if (string.IsNullOrWhiteSpace(columnName))
            {
                return null;
            }

            var lastIndex = columnName.LastIndexOf('.');
            if (lastIndex < 0 || lastIndex == columnName.Length - 1)
            {
                return columnName;
            }

            return columnName[(lastIndex + 1)..];
        }

        private ProcedureResultColumn? BuildColumnFromScalar(SelectScalarExpression scalar)
        {
            if (scalar == null)
            {
                return null;
            }

            string? identifierFallback = null;
            if (scalar.Expression is ColumnReferenceExpression initialColumnRef && initialColumnRef.MultiPartIdentifier != null && initialColumnRef.MultiPartIdentifier.Count > 0)
            {
                identifierFallback = initialColumnRef.MultiPartIdentifier.Identifiers[^1].Value;
            }

            var aliasValue = scalar.ColumnName?.Value;
            var normalizedAlias = string.IsNullOrWhiteSpace(aliasValue) ? null : aliasValue.Trim();

            var column = new ProcedureResultColumn
            {
                Name = normalizedAlias,
                Alias = normalizedAlias,
                RawExpression = scalar.Expression?.ToString()
            };

            switch (scalar.Expression)
            {
                case ColumnReferenceExpression columnRef:
                    PopulateColumnReference(column, columnRef);
                    break;
                case IIfCall iifCall:
                    PopulateIIfCall(column, iifCall);
                    break;
                case FunctionCall functionCall:
                    PopulateFunctionCall(column, functionCall);
                    break;
                case CastCall castCall:
                    PopulateCast(column, castCall);
                    break;
                case ScalarSubquery scalarSubquery:
                    PopulateScalarSubquery(column, scalarSubquery);
                    break;
                case Literal literal:
                    PopulateLiteral(column, literal);
                    break;
                case VariableReference variableRef:
                    PopulateVariableReference(column, variableRef);
                    break;
                default:
                    PopulateComputedExpression(column, scalar.Expression);
                    break;
            }

            if (string.IsNullOrWhiteSpace(column.Name))
            {
                column.Name = !string.IsNullOrWhiteSpace(identifierFallback) ? identifierFallback : column.SourceColumn;
            }

            return column;
        }

        private void PopulateVariableReference(ProcedureResultColumn column, VariableReference variableRef)
        {
            column.ExpressionKind = ProcedureResultColumnExpressionKind.Computed;
            var variableName = NormalizeVariableName(variableRef?.Name);
            if (string.IsNullOrWhiteSpace(variableName))
            {
                return;
            }

            if (_scalarVariableMetadata.TryGetValue(variableName, out var metadata) && metadata != null)
            {
                column.SourceColumn ??= variableName;
                ApplyScalarMetadata(column, metadata);
            }
        }

        private void PopulateScalarSubquery(ProcedureResultColumn column, ScalarSubquery scalarSubquery)
        {
            column.ExpressionKind = ProcedureResultColumnExpressionKind.Computed;

            if (scalarSubquery?.QueryExpression is not QuerySpecification query)
            {
                return;
            }

            if (query.SelectElements == null || query.SelectElements.Count == 0)
            {
                return;
            }

            ApplyForJsonMetadata(column, query);

            Dictionary<string, TableAliasInfo>? scope = null;
            var nestedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var aliasHints = new Dictionary<string, TableAliasInfo>(StringComparer.OrdinalIgnoreCase);
            var jsonBindings = new Dictionary<string, JsonPathBinding>(StringComparer.OrdinalIgnoreCase);

            try
            {
                if (query.FromClause != null)
                {
                    scope = BuildAliasScope(query.FromClause);
                    if (scope.Count > 0)
                    {
                        _aliasScopes.Push(scope);
                    }
                }

                foreach (var element in query.SelectElements)
                {
                    if (element is not SelectScalarExpression nestedScalar)
                    {
                        continue;
                    }

                    var nestedColumn = BuildColumnFromScalar(nestedScalar);
                    if (nestedColumn == null)
                    {
                        continue;
                    }

                    if (scope is { Count: > 0 })
                    {
                        NormalizeColumnSource(nestedColumn, scope);
                        RegisterAliasHint(nestedColumn, scope, aliasHints);
                        ApplyAliasColumnFallback(nestedColumn, scope, aliasHints);
                    }

                    ApplyJsonPathMetadata(nestedColumn, jsonBindings);
                    RegisterJsonPathBinding(nestedColumn, jsonBindings, scope);
                    EnsureUniqueColumnName(nestedColumn, nestedNames);
                    column.Columns.Add(nestedColumn);
                }
            }
            finally
            {
                if (scope is { Count: > 0 })
                {
                    _aliasScopes.Pop();
                }
            }

            var shouldFlattenProbe = false;
            if (column.Columns.Count == 1)
            {
                var probe = column.Columns[0];
                if (probe != null)
                {
                    ApplyProbeMetadata(column, probe);
                    column.SourceSchema ??= probe.SourceSchema;
                    column.SourceTable ??= probe.SourceTable;
                    column.SourceColumn ??= probe.SourceColumn;

                    if (column.Reference == null && probe.Reference != null)
                    {
                        column.Reference = CloneReference(probe.Reference);
                    }

                    if (probe.ReturnsJson.HasValue)
                    {
                        column.ReturnsJson ??= probe.ReturnsJson;
                    }

                    if (probe.ReturnsJsonArray.HasValue)
                    {
                        column.ReturnsJsonArray ??= probe.ReturnsJsonArray;
                    }

                    if (probe.ReturnsUnknownJson.HasValue)
                    {
                        column.ReturnsUnknownJson ??= probe.ReturnsUnknownJson;
                    }

                    if (probe.IsNestedJson.HasValue)
                    {
                        column.IsNestedJson ??= probe.IsNestedJson;
                    }

                    shouldFlattenProbe = ShouldFlattenScalarSubquery(column, probe);
                }
            }

            if (column.ReturnsJson == true && column.ReturnsJsonArray == true && column.Columns.Count == 1)
            {
                var nested = column.Columns[0];
                if (nested != null && nested.ReturnsJson == true && nested.ReturnsJsonArray == true && nested.Columns.Count == 1)
                {
                    var item = nested.Columns[0];
                    if (item != null)
                    {
                        column.Columns[0] = item;
                    }
                }
            }
            else if (shouldFlattenProbe)
            {
                column.Columns.Clear();
            }
        }

        private static bool ShouldFlattenScalarSubquery(ProcedureResultColumn container, ProcedureResultColumn probe)
        {
            if (container == null || probe == null)
            {
                return false;
            }

            if (container.ReturnsJson == true || container.ReturnsJsonArray == true || container.IsNestedJson == true)
            {
                return false;
            }

            if (container.DeferredJsonExpansion == true)
            {
                return false;
            }

            if (probe.ReturnsJson == true || probe.ReturnsJsonArray == true || probe.IsNestedJson == true)
            {
                return false;
            }

            if (probe.DeferredJsonExpansion == true)
            {
                return false;
            }

            if (probe.Columns != null && probe.Columns.Count > 0)
            {
                return false;
            }

            return true;
        }

        private static void ApplyForJsonMetadata(ProcedureResultColumn column, QuerySpecification query)
        {
            if (column == null || query?.ForClause is not JsonForClause jsonClause)
            {
                return;
            }

            column.ReturnsJson ??= true;
            column.IsNestedJson ??= true;
            column.ReturnsUnknownJson = false;
            column.SqlTypeName ??= "nvarchar(max)";
            column.IsNullable ??= true;

            var withoutArrayWrapper = false;
            var includeNullValues = false;
            string? rootProperty = null;

            if (jsonClause.Options != null)
            {
                foreach (var option in jsonClause.Options)
                {
                    switch (option?.OptionKind)
                    {
                        case JsonForClauseOptions.WithoutArrayWrapper:
                            withoutArrayWrapper = true;
                            break;
                        case JsonForClauseOptions.IncludeNullValues:
                            includeNullValues = true;
                            break;
                        case JsonForClauseOptions.Root when option.Value is Literal literal:
                            rootProperty ??= ExtractJsonLiteralValue(literal);
                            break;
                    }
                }
            }

            column.ReturnsJsonArray ??= !withoutArrayWrapper;
            if (includeNullValues)
            {
                column.JsonIncludeNullValues = true;
            }
            if (!string.IsNullOrWhiteSpace(rootProperty) && string.IsNullOrWhiteSpace(column.JsonRootProperty))
            {
                column.JsonRootProperty = rootProperty;
            }
        }

        private static string? ExtractJsonLiteralValue(Literal literal)
        {
            return literal switch
            {
                StringLiteral stringLiteral when !string.IsNullOrWhiteSpace(stringLiteral.Value) => stringLiteral.Value,
                IntegerLiteral integerLiteral when !string.IsNullOrWhiteSpace(integerLiteral.Value) => integerLiteral.Value,
                NumericLiteral numericLiteral when !string.IsNullOrWhiteSpace(numericLiteral.Value) => numericLiteral.Value,
                _ => null
            };
        }

        private void PopulateComputedExpression(ProcedureResultColumn column, ScalarExpression? expression)
        {
            column.ExpressionKind = ProcedureResultColumnExpressionKind.Computed;
            ApplyExpressionMetadata(column, expression);
        }

        private void ApplyExpressionMetadata(ProcedureResultColumn column, ScalarExpression? expression)
        {
            if (column == null || expression == null)
            {
                return;
            }

            switch (expression)
            {
                case ColumnReferenceExpression columnRef:
                    var previousKind = column.ExpressionKind;
                    PopulateColumnReference(column, columnRef);
                    column.ExpressionKind = previousKind;
                    break;
                case Literal literal:
                    var previousLiteralKind = column.ExpressionKind;
                    PopulateLiteral(column, literal);
                    column.ExpressionKind = previousLiteralKind;
                    break;
                case FunctionCall functionCall:
                    var previousFunctionKind = column.ExpressionKind;
                    PopulateFunctionCall(column, functionCall);
                    column.ExpressionKind = previousFunctionKind;
                    if (functionCall.Parameters != null)
                    {
                        foreach (var parameter in functionCall.Parameters)
                        {
                            ApplyExpressionMetadata(column, parameter);
                        }
                    }
                    break;
                case IIfCall iifCall:
                    var previousIifKind = column.ExpressionKind;
                    PopulateIIfCall(column, iifCall);
                    column.ExpressionKind = previousIifKind;
                    ApplyExpressionMetadata(column, iifCall.ThenExpression);
                    ApplyExpressionMetadata(column, iifCall.ElseExpression);
                    break;
                case CastCall castCall:
                    var previousCastKind = column.ExpressionKind;
                    PopulateCast(column, castCall);
                    column.ExpressionKind = previousCastKind;
                    ApplyExpressionMetadata(column, castCall.Parameter);
                    break;
                case ConvertCall convertCall:
                    var previousConvertKind = column.ExpressionKind;
                    PopulateConvertCall(column, convertCall);
                    column.ExpressionKind = previousConvertKind;
                    ApplyExpressionMetadata(column, convertCall.Parameter);
                    ApplyExpressionMetadata(column, convertCall.Style);
                    break;
                case TryCastCall tryCastCall:
                    var previousTryCastKind = column.ExpressionKind;
                    PopulateTryCastCall(column, tryCastCall);
                    column.ExpressionKind = previousTryCastKind;
                    ApplyExpressionMetadata(column, tryCastCall.Parameter);
                    break;
                case TryConvertCall tryConvertCall:
                    var previousTryConvertKind = column.ExpressionKind;
                    PopulateTryConvertCall(column, tryConvertCall);
                    column.ExpressionKind = previousTryConvertKind;
                    ApplyExpressionMetadata(column, tryConvertCall.Parameter);
                    ApplyExpressionMetadata(column, tryConvertCall.Style);
                    break;
                case ScalarSubquery nestedSubquery:
                    var previousSubqueryKind = column.ExpressionKind;
                    PopulateScalarSubquery(column, nestedSubquery);
                    column.ExpressionKind = previousSubqueryKind;
                    break;
                case VariableReference variableReference:
                    var previousVariableKind = column.ExpressionKind;
                    PopulateVariableReference(column, variableReference);
                    column.ExpressionKind = previousVariableKind;
                    break;
                case ParenthesisExpression parenthesisExpression:
                    ApplyExpressionMetadata(column, parenthesisExpression.Expression);
                    break;
                case UnaryExpression unaryExpression:
                    ApplyExpressionMetadata(column, unaryExpression.Expression);
                    break;
                case BinaryExpression binaryExpression:
                    ApplyExpressionMetadata(column, binaryExpression.FirstExpression);
                    ApplyExpressionMetadata(column, binaryExpression.SecondExpression);
                    ApplyBinaryExpressionMetadata(column, binaryExpression);
                    break;
                case CoalesceExpression coalesceExpression:
                    if (coalesceExpression.Expressions != null && coalesceExpression.Expressions.Count > 0)
                    {
                        var coalesceCandidates = new List<ColumnSourceCandidate>();
                        foreach (var expr in coalesceExpression.Expressions)
                        {
                            var candidate = ExtractColumnSource(expr);
                            if (candidate != null)
                            {
                                coalesceCandidates.Add(candidate);
                            }
                        }

                        if (coalesceCandidates.Count > 0)
                        {
                            AssignColumnSource(column, coalesceCandidates);
                        }

                        foreach (var expr in coalesceExpression.Expressions)
                        {
                            ApplyExpressionMetadata(column, expr);
                        }

                        if (string.IsNullOrWhiteSpace(column.SqlTypeName))
                        {
                            ApplyCoalesceLiteralFallback(column, coalesceExpression.Expressions);
                        }
                    }
                    break;
                case SimpleCaseExpression simpleCaseExpression:
                    ApplyExpressionMetadata(column, simpleCaseExpression.InputExpression);
                    foreach (var clause in simpleCaseExpression.WhenClauses)
                    {
                        ApplyExpressionMetadata(column, clause.WhenExpression);
                        ApplyExpressionMetadata(column, clause.ThenExpression);
                    }
                    ApplyExpressionMetadata(column, simpleCaseExpression.ElseExpression);
                    break;
                case SearchedCaseExpression searchedCaseExpression:
                    foreach (var clause in searchedCaseExpression.WhenClauses)
                    {
                        ApplyExpressionMetadata(column, clause.ThenExpression);
                    }
                    ApplyExpressionMetadata(column, searchedCaseExpression.ElseExpression);
                    break;
                case NullIfExpression nullIfExpression:
                    ApplyExpressionMetadata(column, nullIfExpression.FirstExpression);
                    ApplyExpressionMetadata(column, nullIfExpression.SecondExpression);
                    break;
            }
        }

        private void ApplyBinaryExpressionMetadata(ProcedureResultColumn column, BinaryExpression binaryExpression)
        {
            if (column == null || binaryExpression == null)
            {
                return;
            }

            var leftMetadata = ExtractBinaryOperandMetadata(binaryExpression.FirstExpression);
            var rightMetadata = ExtractBinaryOperandMetadata(binaryExpression.SecondExpression);
            var merged = MergeBinaryOperandMetadata(leftMetadata, rightMetadata);

            if (merged != null)
            {
                ApplyTypeMetadata(column, merged);
            }

            if (!column.IsNullable.HasValue)
            {
                var leftNullable = leftMetadata?.IsNullable ?? false;
                var rightNullable = rightMetadata?.IsNullable ?? false;
                if (leftMetadata != null || rightMetadata != null)
                {
                    column.IsNullable = leftNullable || rightNullable;
                }
            }
            else if (column.IsNullable == false)
            {
                var leftNullable = leftMetadata?.IsNullable ?? false;
                var rightNullable = rightMetadata?.IsNullable ?? false;
                if (leftNullable || rightNullable)
                {
                    column.IsNullable = true;
                }
            }
        }

        private ColumnSourceInfo? ExtractBinaryOperandMetadata(ScalarExpression? expression)
        {
            if (expression == null)
            {
                return null;
            }

            var candidate = ExtractColumnSource(expression);
            if (candidate?.Probe != null)
            {
                return CreateMetadataFromProbe(candidate.Probe);
            }

            var literalMetadata = InferLiteralColumnMetadata(expression);
            if (literalMetadata != null)
            {
                return literalMetadata;
            }

            return expression switch
            {
                IIfCall iifCall => MergeBinaryOperandMetadata(
                    ExtractBinaryOperandMetadata(iifCall.ThenExpression),
                    ExtractBinaryOperandMetadata(iifCall.ElseExpression)),
                ParenthesisExpression parenthesis => ExtractBinaryOperandMetadata(parenthesis.Expression),
                UnaryExpression unary => ExtractBinaryOperandMetadata(unary.Expression),
                _ => null
            };
        }

        private static ColumnSourceInfo? CreateMetadataFromProbe(ProcedureResultColumn? probe)
        {
            if (probe == null)
            {
                return null;
            }

            return new ColumnSourceInfo
            {
                Catalog = probe.SourceCatalog,
                Schema = probe.SourceSchema,
                Table = probe.SourceTable,
                Column = probe.SourceColumn,
                Reference = probe.Reference == null ? null : CloneReference(probe.Reference),
                ReturnsJson = probe.ReturnsJson,
                ReturnsJsonArray = probe.ReturnsJsonArray,
                ReturnsUnknownJson = probe.ReturnsUnknownJson,
                IsNestedJson = probe.IsNestedJson,
                SqlTypeName = probe.SqlTypeName,
                MaxLength = probe.MaxLength,
                IsNullable = probe.IsNullable,
                UserTypeSchema = probe.UserTypeSchemaName,
                UserTypeName = probe.UserTypeName,
                CastTargetType = probe.CastTargetType,
                CastTargetLength = probe.CastTargetLength,
                CastTargetPrecision = probe.CastTargetPrecision,
                CastTargetScale = probe.CastTargetScale
            };
        }

        private static ColumnSourceInfo? MergeBinaryOperandMetadata(ColumnSourceInfo? left, ColumnSourceInfo? right)
        {
            if (left == null && right == null)
            {
                return null;
            }

            if (left == null)
            {
                return CloneColumnSourceInfo(right!);
            }

            if (right == null)
            {
                return CloneColumnSourceInfo(left);
            }

            var leftType = NormalizeSqlTypeName(left.SqlTypeName ?? left.CastTargetType);
            var rightType = NormalizeSqlTypeName(right.SqlTypeName ?? right.CastTargetType);

            ColumnSourceInfo primary;
            ColumnSourceInfo secondary;

            if (string.IsNullOrWhiteSpace(leftType) && string.IsNullOrWhiteSpace(rightType))
            {
                primary = left;
                secondary = right;
            }
            else if (string.IsNullOrWhiteSpace(leftType))
            {
                primary = right;
                secondary = left;
            }
            else if (string.IsNullOrWhiteSpace(rightType))
            {
                primary = left;
                secondary = right;
            }
            else
            {
                var leftScore = GetTypePrecedence(leftType);
                var rightScore = GetTypePrecedence(rightType);
                primary = leftScore >= rightScore ? left : right;
                secondary = ReferenceEquals(primary, left) ? right : left;
            }

            var mergedBase = MergeColumnSourceInfo(primary, secondary);
            var nullable = mergedBase.IsNullable ?? ((left.IsNullable ?? false) || (right.IsNullable ?? false));

            return new ColumnSourceInfo
            {
                Catalog = mergedBase.Catalog,
                Schema = mergedBase.Schema,
                Table = mergedBase.Table,
                Column = mergedBase.Column,
                Reference = mergedBase.Reference,
                ReturnsJson = mergedBase.ReturnsJson,
                ReturnsJsonArray = mergedBase.ReturnsJsonArray,
                ReturnsUnknownJson = mergedBase.ReturnsUnknownJson,
                IsNestedJson = mergedBase.IsNestedJson,
                SqlTypeName = mergedBase.SqlTypeName,
                MaxLength = mergedBase.MaxLength,
                Precision = mergedBase.Precision,
                Scale = mergedBase.Scale,
                IsNullable = nullable,
                UserTypeSchema = mergedBase.UserTypeSchema,
                UserTypeName = mergedBase.UserTypeName,
                CastTargetType = mergedBase.CastTargetType,
                CastTargetLength = mergedBase.CastTargetLength,
                CastTargetPrecision = mergedBase.CastTargetPrecision,
                CastTargetScale = mergedBase.CastTargetScale
            };
        }

        private static string? NormalizeSqlTypeName(string? typeName)
        {
            if (string.IsNullOrWhiteSpace(typeName))
            {
                return null;
            }

            var trimmed = typeName.Trim();
            var dotIndex = trimmed.LastIndexOf('.');
            if (dotIndex >= 0)
            {
                trimmed = trimmed[(dotIndex + 1)..];
            }

            var parenIndex = trimmed.IndexOf('(');
            if (parenIndex >= 0)
            {
                trimmed = trimmed[..parenIndex];
            }

            return trimmed.ToLowerInvariant();
        }

        private static int GetTypePrecedence(string? normalizedType)
        {
            if (string.IsNullOrWhiteSpace(normalizedType))
            {
                return 0;
            }

            if (SqlTypePrecedence.TryGetValue(normalizedType, out var precedence))
            {
                return precedence;
            }

            return 0;
        }

        private void PopulateLiteral(ProcedureResultColumn column, Literal literal)
        {
            column.ExpressionKind = ProcedureResultColumnExpressionKind.Computed;

            switch (literal)
            {
                case IntegerLiteral integerLiteral:
                    column.HasIntegerLiteral = true;
                    column.IsNullable ??= false;

                    if (string.IsNullOrWhiteSpace(column.SqlTypeName) && long.TryParse(integerLiteral.Value, NumberStyles.Integer, CultureInfo.InvariantCulture, out var integerValue))
                    {
                        column.SqlTypeName = integerValue is >= int.MinValue and <= int.MaxValue ? "int" : "bigint";
                    }

                    break;
                case NumericLiteral:
                    column.HasDecimalLiteral = true;
                    column.IsNullable ??= false;
                    break;
                case RealLiteral:
                    column.HasDecimalLiteral = true;
                    column.IsNullable ??= false;
                    column.SqlTypeName ??= "float";
                    break;
                case MoneyLiteral:
                    column.HasDecimalLiteral = true;
                    column.IsNullable ??= false;
                    column.SqlTypeName ??= "money";
                    break;
                case BinaryLiteral binaryLiteral:
                    column.SqlTypeName ??= "varbinary";
                    var binaryValue = binaryLiteral.Value;
                    if (!string.IsNullOrWhiteSpace(binaryValue))
                    {
                        var span = binaryValue.AsSpan();
                        if (span.StartsWith("0x", StringComparison.OrdinalIgnoreCase))
                        {
                            span = span[2..];
                        }

                        if (span.Length > 0 && span.Length % 2 == 0)
                        {
                            column.MaxLength ??= span.Length / 2;
                        }
                    }

                    column.IsNullable ??= false;
                    break;
                case StringLiteral stringLiteral:
                    column.SqlTypeName ??= "nvarchar";
                    var length = stringLiteral.Value?.Length ?? 0;
                    var effectiveLength = length <= 0 ? 1 : length;
                    column.MaxLength ??= effectiveLength;
                    column.IsNullable ??= false;
                    break;
                case NullLiteral:
                    column.HasNullLiteral = true;
                    column.IsNullable = true;
                    var metadata = TryResolveScalarMetadata(column);
                    if (metadata != null)
                    {
                        ApplyScalarMetadata(column, metadata);
                    }

                    break;
                case DefaultLiteral:
                    column.ForcedNullable ??= true;
                    column.IsAmbiguous ??= true;
                    column.IsNullable ??= true;
                    var defaultMetadata = TryResolveScalarMetadata(column);
                    if (defaultMetadata != null)
                    {
                        ApplyScalarMetadata(column, defaultMetadata);
                    }

                    break;
                default:
                    column.IsAmbiguous ??= true;
                    if (_verboseParsing && string.IsNullOrWhiteSpace(column.RawExpression))
                    {
                        column.RawExpression = literal.Value ?? literal.GetType().Name;
                    }

                    break;
            }
        }

        private ColumnSourceInfo? TryResolveScalarMetadata(ProcedureResultColumn column)
        {
            if (column == null)
            {
                return null;
            }

            var candidates = new[]
            {
                column.Name,
                column.SourceColumn,
                column.SourceAlias
            };

            foreach (var candidate in candidates)
            {
                if (string.IsNullOrWhiteSpace(candidate))
                {
                    continue;
                }

                var normalized = NormalizeVariableName(candidate);
                if (_scalarVariableMetadata.TryGetValue(normalized, out var metadata) && metadata != null)
                {
                    return metadata;
                }
            }

            return null;
        }

        private static void ApplyScalarMetadata(ProcedureResultColumn column, ColumnSourceInfo metadata)
        {
            if (column == null || metadata == null)
            {
                return;
            }

            if (!string.IsNullOrWhiteSpace(metadata.SqlTypeName))
            {
                column.SqlTypeName ??= metadata.SqlTypeName;
            }

            if (metadata.MaxLength.HasValue)
            {
                column.MaxLength ??= metadata.MaxLength;
            }

            if (metadata.IsNullable.HasValue)
            {
                column.IsNullable ??= metadata.IsNullable;
            }

            if (!string.IsNullOrWhiteSpace(metadata.UserTypeSchema))
            {
                column.UserTypeSchemaName ??= metadata.UserTypeSchema;
            }

            if (!string.IsNullOrWhiteSpace(metadata.UserTypeName))
            {
                column.UserTypeName ??= metadata.UserTypeName;
            }

            if (metadata.ReturnsJson.HasValue)
            {
                column.ReturnsJson ??= metadata.ReturnsJson;
            }

            if (metadata.ReturnsJsonArray.HasValue)
            {
                column.ReturnsJsonArray ??= metadata.ReturnsJsonArray;
            }

            if (metadata.ReturnsUnknownJson.HasValue)
            {
                column.ReturnsUnknownJson ??= metadata.ReturnsUnknownJson;
            }

            if (metadata.IsNestedJson.HasValue)
            {
                column.IsNestedJson ??= metadata.IsNestedJson;
            }

            ApplyUserTypeResolution(column, metadata);
        }

        private void PopulateCast(ProcedureResultColumn column, CastCall castCall)
        {
            column.ExpressionKind = ProcedureResultColumnExpressionKind.Cast;
            ApplyTargetDataType(column, castCall?.DataType);

            var inner = BuildInnerColumn(column, castCall?.Parameter);
            if (inner != null)
            {
                column.SourceSchema = inner.SourceSchema;
                column.SourceTable = inner.SourceTable;
                column.SourceColumn = inner.SourceColumn;
                column.Reference = inner.Reference;
            }
        }

        private void PopulateConvertCall(ProcedureResultColumn column, ConvertCall convertCall)
        {
            column.ExpressionKind = ProcedureResultColumnExpressionKind.Cast;
            ApplyTargetDataType(column, convertCall?.DataType);

            var inner = BuildInnerColumn(column, convertCall?.Parameter);
            if (inner != null)
            {
                column.SourceSchema = inner.SourceSchema;
                column.SourceTable = inner.SourceTable;
                column.SourceColumn = inner.SourceColumn;
                column.Reference = inner.Reference;
            }
        }

        private void PopulateTryCastCall(ProcedureResultColumn column, TryCastCall tryCastCall)
        {
            column.ExpressionKind = ProcedureResultColumnExpressionKind.Cast;
            column.IsNullable = column.IsNullable ?? true;
            ApplyTargetDataType(column, tryCastCall?.DataType);

            var inner = BuildInnerColumn(column, tryCastCall?.Parameter);
            if (inner != null)
            {
                column.SourceSchema = inner.SourceSchema;
                column.SourceTable = inner.SourceTable;
                column.SourceColumn = inner.SourceColumn;
                column.Reference = inner.Reference;
            }
        }

        private void PopulateTryConvertCall(ProcedureResultColumn column, TryConvertCall tryConvertCall)
        {
            column.ExpressionKind = ProcedureResultColumnExpressionKind.Cast;
            column.IsNullable = column.IsNullable ?? true;
            ApplyTargetDataType(column, tryConvertCall?.DataType);

            var inner = BuildInnerColumn(column, tryConvertCall?.Parameter);
            if (inner != null)
            {
                column.SourceSchema = inner.SourceSchema;
                column.SourceTable = inner.SourceTable;
                column.SourceColumn = inner.SourceColumn;
                column.Reference = inner.Reference;
            }
        }

        private ProcedureResultColumn? BuildInnerColumn(ProcedureResultColumn column, ScalarExpression? expression)
        {
            if (expression == null)
            {
                return null;
            }

            return BuildColumnFromScalar(new SelectScalarExpression
            {
                Expression = expression,
                ColumnName = string.IsNullOrWhiteSpace(column.Name)
                    ? null
                    : new IdentifierOrValueExpression { Identifier = new Identifier { Value = column.Name } }
            });
        }

        private void ApplyTargetDataType(ProcedureResultColumn column, DataTypeReference? dataType)
        {
            if (column == null || dataType == null)
            {
                return;
            }

            switch (dataType)
            {
                case SqlDataTypeReference sqlDataType:
                    var metadata = BuildSqlDataTypeMetadata(sqlDataType);
                    if (!string.IsNullOrWhiteSpace(metadata.SqlTypeName))
                    {
                        column.CastTargetType ??= metadata.SqlTypeName;
                    }

                    if (metadata.MaxLength.HasValue)
                    {
                        column.CastTargetLength ??= metadata.MaxLength;
                    }

                    if (metadata.Precision.HasValue)
                    {
                        column.CastTargetPrecision ??= metadata.Precision;
                    }

                    if (metadata.Scale.HasValue)
                    {
                        column.CastTargetScale ??= metadata.Scale;
                    }
                    break;
                case ParameterizedDataTypeReference parameterized:
                    var parameterizedName = parameterized.Name?.BaseIdentifier?.Value;

                    if (parameterized is UserDataTypeReference userDataType)
                    {
                        var (userSchema, userName) = ExtractSchemaAndName(userDataType.Name);
                        if (!string.IsNullOrWhiteSpace(userSchema))
                        {
                            column.UserTypeSchemaName ??= userSchema;
                        }

                        if (!string.IsNullOrWhiteSpace(userName))
                        {
                            column.UserTypeName ??= userName;
                            column.CastTargetType ??= string.IsNullOrWhiteSpace(userSchema)
                                ? userName
                                : string.Concat(userSchema, ".", userName);
                        }

                        break;
                    }

                    if (!string.IsNullOrWhiteSpace(parameterizedName))
                    {
                        column.CastTargetType ??= parameterizedName;
                    }

                    if (parameterized.Parameters != null && parameterized.Parameters.Count > 0)
                    {
                        var first = TryParseInt(parameterized.Parameters[0]);
                        var second = parameterized.Parameters.Count > 1 ? TryParseInt(parameterized.Parameters[1]) : null;
                        var normalized = parameterizedName?.ToLowerInvariant();

                        if (normalized is "decimal" or "numeric")
                        {
                            if (first.HasValue)
                            {
                                column.CastTargetPrecision ??= first;
                            }

                            if (second.HasValue)
                            {
                                column.CastTargetScale ??= second;
                            }
                        }
                        else if (normalized is "datetime2" or "datetimeoffset" or "time")
                        {
                            if (first.HasValue)
                            {
                                column.CastTargetScale ??= first;
                            }
                        }
                        else if (first.HasValue)
                        {
                            column.CastTargetLength ??= first;
                        }
                    }
                    break;
                case XmlDataTypeReference:
                    column.CastTargetType ??= "xml";
                    break;
            }

            if (!string.IsNullOrWhiteSpace(column.CastTargetType) && string.IsNullOrWhiteSpace(column.SqlTypeName))
            {
                column.SqlTypeName = column.CastTargetType;
            }
        }

        private void PopulateFunctionCall(ProcedureResultColumn column, FunctionCall? functionCall)
        {
            if (column == null)
            {
                return;
            }

            if (functionCall == null)
            {
                return;
            }

            var activeFunctionCall = functionCall!;

            column.ExpressionKind = ProcedureResultColumnExpressionKind.FunctionCall;

            string? schema = null;
            var name = functionCall.FunctionName?.Value;
            var isIIfCall = IsIIfCall(functionCall);
            if (isIIfCall && string.IsNullOrWhiteSpace(name))
            {
                name = "IIF";
            }

            var isBuiltIn = IsBuiltInFunction(functionCall);

            // Enhanced JSON function analysis for JSON_QUERY
            if (string.Equals(name, "JSON_QUERY", StringComparison.OrdinalIgnoreCase))
            {
                var enhancedInfo = ExtractEnhancedJsonFunctionInfo(functionCall, _defaultSchema);
                if (enhancedInfo.HasValue)
                {
                    schema = enhancedInfo.Value.Schema;
                    name = enhancedInfo.Value.FunctionName;
                    isBuiltIn = false;
                    column.ReturnsJson = true;
                    column.ReturnsUnknownJson = false;
                }

                column.ReturnsJson ??= true;
                column.SqlTypeName ??= "nvarchar(max)";
                column.IsNullable ??= true;

                TryInferJsonArrayMetadata(column, functionCall);
                ApplyJsonQueryParameterMetadata(column, functionCall?.Parameters);
            }

            if (activeFunctionCall.CallTarget is MultiPartIdentifierCallTarget fnTarget)
            {
                var multiPart = fnTarget.MultiPartIdentifier;
                if (multiPart?.Identifiers is { Count: > 0 } identifiers)
                {
                    var qualifierParts = identifiers
                        .Select(id => id?.Value)
                        .Where(value => !string.IsNullOrWhiteSpace(value))
                        .ToArray();

                    if (qualifierParts.Length > 0)
                    {
                        schema = string.Join('.', qualifierParts);
                    }
                }
            }

            if (!isBuiltIn && schema == null && !string.IsNullOrWhiteSpace(_defaultSchema))
            {
                schema = _defaultSchema;
            }

            var normalizedName = name?.ToUpperInvariant();

            if (!string.IsNullOrWhiteSpace(name) && !isBuiltIn)
            {
                column.Reference = new ProcedureReferenceModel
                {
                    Kind = ProcedureReferenceKind.Function,
                    Schema = schema,
                    Name = name
                };
            }

            if (activeFunctionCall.CallTarget is MultiPartIdentifierCallTarget target && target.MultiPartIdentifier != null)
            {
                var identifiers = target.MultiPartIdentifier.Identifiers;
                if (identifiers.Count >= 2)
                {
                    column.SourceSchema = identifiers[^2]?.Value;
                }

                if (identifiers.Count >= 1)
                {
                    column.SourceTable = identifiers[^1]?.Value;
                }
            }

            if (isBuiltIn)
            {
                TryPopulateSourceFromFunctionArguments(column, activeFunctionCall);
                ApplyBuiltInFunctionFallback(column, normalizedName, activeFunctionCall);
            }
        }

        private void ApplyBuiltInFunctionFallback(ProcedureResultColumn column, string? normalizedName, FunctionCall functionCall)
        {
            if (column == null || string.IsNullOrWhiteSpace(normalizedName))
            {
                return;
            }

            switch (normalizedName)
            {
                case "COUNT":
                    column.SqlTypeName ??= "int";
                    column.IsNullable ??= false;
                    break;
                case "COUNT_BIG":
                    column.SqlTypeName ??= "bigint";
                    column.IsNullable ??= false;
                    break;
                case "ROW_NUMBER":
                    column.SqlTypeName ??= "bigint";
                    column.IsNullable ??= false;
                    break;
                case "ISNULL":
                    ApplyNullCoalesceFunctionFallback(column, functionCall, treatAsIsNull: true);
                    break;
                case "COALESCE":
                    ApplyNullCoalesceFunctionFallback(column, functionCall, treatAsIsNull: false);
                    break;
                case "SUM":
                    ApplySumFallback(column, functionCall);
                    break;
                case "AVG":
                    ApplyAverageFallback(column, functionCall);
                    break;
                case "MIN":
                case "MAX":
                    ApplyMinMaxFallback(column, functionCall);
                    break;
                case "CONCAT":
                    ApplyConcatFallback(column);
                    break;
                case "STRING_AGG":
                    ApplyStringAggFallback(column, functionCall);
                    break;
                case "DB_NAME":
                    column.SqlTypeName = "nvarchar";
                    column.MaxLength = column.MaxLength ?? 128;
                    column.CastTargetType = null;
                    column.CastTargetLength = null;
                    column.CastTargetPrecision = null;
                    column.CastTargetScale = null;
                    column.SourceSchema = null;
                    column.SourceTable = null;
                    column.SourceColumn = null;
                    column.Reference = null;
                    column.UserTypeName = null;
                    column.UserTypeSchemaName = null;
                    column.IsNullable = true;
                    break;
            }
        }

        private void ApplySumFallback(ProcedureResultColumn column, FunctionCall functionCall)
        {
            if (column == null)
            {
                return;
            }

            if (HasAggregateMetadata(column))
            {
                column.IsNullable = true;
                return;
            }

            var operandMetadata = ResolveAggregateOperandMetadata(functionCall);
            if (operandMetadata != null)
            {
                var details = ExtractAggregateOperandDetails(operandMetadata);
                var inference = AggregateTypeRules.InferSum(details.BaseType, details.FormattedType, details.Precision, details.Scale, details.Length);
                if (inference.HasValue)
                {
                    ApplyAggregateInference(column, inference, operandMetadata, propagateUserType: false, useOperandNullability: false, copyLengthToMaxLength: false);
                    column.IsNullable = true;
                    return;
                }
            }

            if (column.HasDecimalLiteral)
            {
                ApplyDecimalFallback(column, "decimal", 38, 10);
            }
            else if (column.HasIntegerLiteral)
            {
                ApplyIntegralFallback(column, "int");
            }
            else
            {
                ApplyDecimalFallback(column, "decimal", 38, 10);
            }

            column.IsNullable = true;
        }

        private void ApplyAverageFallback(ProcedureResultColumn column, FunctionCall functionCall)
        {
            if (column == null)
            {
                return;
            }

            if (HasAggregateMetadata(column))
            {
                column.IsNullable = true;
                return;
            }

            var operandMetadata = ResolveAggregateOperandMetadata(functionCall);
            if (operandMetadata != null)
            {
                var details = ExtractAggregateOperandDetails(operandMetadata);
                var inference = AggregateTypeRules.InferAverage(details.BaseType, details.FormattedType, details.Precision, details.Scale, details.Length);
                if (inference.HasValue)
                {
                    ApplyAggregateInference(column, inference, operandMetadata, propagateUserType: false, useOperandNullability: false, copyLengthToMaxLength: false);
                    column.IsNullable = true;
                    return;
                }
            }

            ApplyDecimalFallback(column, "decimal", 38, 6);
            column.IsNullable = true;
        }

        private void ApplyMinMaxFallback(ProcedureResultColumn column, FunctionCall functionCall)
        {
            if (column == null)
            {
                return;
            }

            if (HasAggregateMetadata(column))
            {
                return;
            }

            var operandMetadata = ResolveAggregateOperandMetadata(functionCall);
            if (operandMetadata != null)
            {
                var details = ExtractAggregateOperandDetails(operandMetadata);
                var inference = AggregateTypeRules.InferMinMax(details.BaseType, details.FormattedType, details.Precision, details.Scale, details.Length);
                if (inference.HasValue)
                {
                    ApplyAggregateInference(column, inference, operandMetadata, propagateUserType: true, useOperandNullability: true, copyLengthToMaxLength: true);
                    return;
                }
            }

            if (column.HasIntegerLiteral && !column.HasDecimalLiteral)
            {
                ApplyIntegralFallback(column, "int");
                return;
            }

            if (column.HasDecimalLiteral)
            {
                ApplyDecimalFallback(column, "decimal", 38, 6);
            }
        }

        private static void ApplyIntegralFallback(ProcedureResultColumn column, string sqlType)
        {
            column.SqlTypeName = sqlType;
            column.CastTargetType ??= sqlType;
            column.CastTargetPrecision = null;
            column.CastTargetScale = null;
            column.CastTargetLength = null;
        }

        private static void ApplyDecimalFallback(ProcedureResultColumn column, string baseType, int precision, int scale)
        {
            var formatted = string.Concat(baseType, "(", precision.ToString(CultureInfo.InvariantCulture), ",", scale.ToString(CultureInfo.InvariantCulture), ")");
            column.SqlTypeName = formatted;
            column.CastTargetType ??= baseType;
            column.CastTargetPrecision ??= precision;
            column.CastTargetScale ??= scale;
            column.CastTargetLength = null;
        }

        private ColumnSourceInfo? ResolveAggregateOperandMetadata(FunctionCall functionCall)
        {
            if (functionCall?.Parameters == null || functionCall.Parameters.Count == 0)
            {
                return null;
            }

            ColumnSourceInfo? operandMetadata = null;

            foreach (var parameter in functionCall.Parameters)
            {
                var candidateMetadata = ExtractBinaryOperandMetadata(parameter);
                if (candidateMetadata == null)
                {
                    continue;
                }

                operandMetadata = operandMetadata == null
                    ? candidateMetadata
                    : MergeColumnSourceInfo(candidateMetadata, operandMetadata);

                if (!string.IsNullOrWhiteSpace(operandMetadata.SqlTypeName) || !string.IsNullOrWhiteSpace(operandMetadata.CastTargetType))
                {
                    break;
                }
            }

            return operandMetadata;
        }

        private static AggregateOperandDetails ExtractAggregateOperandDetails(ColumnSourceInfo operand)
        {
            var baseType = NormalizeSqlTypeName(operand.CastTargetType ?? operand.SqlTypeName);
            var precision = operand.CastTargetPrecision ?? operand.Precision;
            var scale = operand.CastTargetScale ?? operand.Scale;
            var length = operand.CastTargetLength ?? operand.MaxLength;

            var formatted = operand.SqlTypeName;
            if (string.IsNullOrWhiteSpace(formatted))
            {
                if (!string.IsNullOrWhiteSpace(baseType))
                {
                    if ((baseType == "decimal" || baseType == "numeric") && precision.HasValue)
                    {
                        var effectiveScale = scale ?? 0;
                        formatted = string.Concat(baseType, "(", precision.Value.ToString(CultureInfo.InvariantCulture), ",", effectiveScale.ToString(CultureInfo.InvariantCulture), ")");
                    }
                    else if (baseType == "float" && length.HasValue && length.Value > 0)
                    {
                        formatted = string.Concat("float(", length.Value.ToString(CultureInfo.InvariantCulture), ")");
                    }
                    else
                    {
                        formatted = baseType;
                    }
                }
            }

            return new AggregateOperandDetails(
                baseType,
                formatted,
                precision,
                scale,
                length,
                operand.IsNullable,
                operand.UserTypeSchema,
                operand.UserTypeName);
        }

        private void ApplyAggregateInference(
            ProcedureResultColumn column,
            AggregateTypeRules.AggregateSqlType inference,
            ColumnSourceInfo? operand,
            bool propagateUserType,
            bool useOperandNullability,
            bool copyLengthToMaxLength)
        {
            if (!inference.HasValue)
            {
                return;
            }

            if (!string.IsNullOrWhiteSpace(inference.FormattedType))
            {
                column.SqlTypeName = inference.FormattedType;
            }
            else if (!string.IsNullOrWhiteSpace(inference.BaseType))
            {
                column.SqlTypeName = inference.BaseType;
            }

            if (!string.IsNullOrWhiteSpace(inference.BaseType))
            {
                column.CastTargetType = inference.BaseType;
            }

            if (inference.Precision.HasValue)
            {
                column.CastTargetPrecision = inference.Precision;
            }

            if (inference.Scale.HasValue)
            {
                column.CastTargetScale = inference.Scale;
            }

            if (inference.Length.HasValue)
            {
                column.CastTargetLength = inference.Length;
            }
            else if (copyLengthToMaxLength && operand != null)
            {
                var operandLength = operand.CastTargetLength ?? operand.MaxLength;
                if (operandLength.HasValue)
                {
                    column.CastTargetLength ??= operandLength;
                }
            }

            if (copyLengthToMaxLength)
            {
                var candidateLength = inference.Length ?? operand?.CastTargetLength ?? operand?.MaxLength;
                if (candidateLength.HasValue)
                {
                    column.MaxLength = candidateLength;
                }
            }

            if (propagateUserType && operand != null)
            {
                if (!string.IsNullOrWhiteSpace(operand.UserTypeSchema))
                {
                    column.UserTypeSchemaName ??= operand.UserTypeSchema;
                }

                if (!string.IsNullOrWhiteSpace(operand.UserTypeName))
                {
                    column.UserTypeName ??= operand.UserTypeName;
                }
            }

            if (useOperandNullability && operand?.IsNullable is bool operandNullable)
            {
                column.IsNullable = operandNullable;
            }
            else if (inference.ForceNullable)
            {
                column.IsNullable = true;
            }
        }

        private static bool HasAggregateMetadata(ProcedureResultColumn column)
        {
            return !string.IsNullOrWhiteSpace(column.SqlTypeName)
                || !string.IsNullOrWhiteSpace(column.CastTargetType)
                || (!string.IsNullOrWhiteSpace(column.UserTypeSchemaName) && !string.IsNullOrWhiteSpace(column.UserTypeName));
        }

        private readonly record struct AggregateOperandDetails(
            string? BaseType,
            string? FormattedType,
            int? Precision,
            int? Scale,
            int? Length,
            bool? IsNullable,
            string? UserTypeSchema,
            string? UserTypeName);

        private void ApplyConcatFallback(ProcedureResultColumn column)
        {
            if (column == null)
            {
                return;
            }

            column.SqlTypeName = "nvarchar(max)";
            column.MaxLength = null;
            column.CastTargetType = null;
            column.CastTargetLength = null;
            column.CastTargetPrecision = null;
            column.CastTargetScale = null;
            column.UserTypeSchemaName = null;
            column.UserTypeName = null;
            column.Reference = null;
            column.SourceSchema = null;
            column.SourceTable = null;
            column.SourceColumn = null;
            column.IsNullable ??= false;
        }

        private void ApplyStringAggFallback(ProcedureResultColumn column, FunctionCall functionCall)
        {
            if (column == null)
            {
                return;
            }

            ColumnSourceInfo? valueMetadata = null;

            if (functionCall?.Parameters is { Count: > 0 })
            {
                valueMetadata = ExtractBinaryOperandMetadata(functionCall.Parameters[0]);
            }

            column.SqlTypeName = "nvarchar(max)";
            column.MaxLength = null;
            column.CastTargetType = null;
            column.CastTargetLength = null;
            column.CastTargetPrecision = null;
            column.CastTargetScale = null;
            column.IsNullable = true;

            column.Reference = null;
            column.SourceSchema = null;
            column.SourceTable = null;
            column.SourceColumn = null;
            column.UserTypeSchemaName = null;
            column.UserTypeName = null;

            if (valueMetadata?.ReturnsJson.HasValue == true)
            {
                column.ReturnsJson ??= valueMetadata.ReturnsJson;
            }

            if (valueMetadata?.ReturnsJsonArray.HasValue == true)
            {
                column.ReturnsJsonArray ??= valueMetadata.ReturnsJsonArray;
            }

            ApplyUserTypeResolution(column);
        }

        private void TryInferJsonArrayMetadata(ProcedureResultColumn column, FunctionCall jsonQueryCall)
        {
            if (column == null || jsonQueryCall?.Parameters == null || jsonQueryCall.Parameters.Count == 0)
            {
                return;
            }

            if (jsonQueryCall.Parameters[0] is not FunctionCall concat || concat.Parameters == null || concat.Parameters.Count < 3)
            {
                return;
            }

            if (!IsBracketLiteral(concat.Parameters[0], '[') || !IsBracketLiteral(concat.Parameters[^1], ']'))
            {
                return;
            }

            ColumnSourceInfo? itemMetadata = null;
            for (var i = 1; i < concat.Parameters.Count - 1; i++)
            {
                itemMetadata = ExtractBinaryOperandMetadata(concat.Parameters[i]);
                if (itemMetadata != null)
                {
                    break;
                }
            }

            if (itemMetadata == null)
            {
                return;
            }

            column.ReturnsJson = true;
            column.ReturnsJsonArray = true;
            column.ReturnsUnknownJson = false;
            column.IsNestedJson ??= true;

            ProcedureResultColumn itemColumn;
            if (column.Columns.Count == 1)
            {
                itemColumn = column.Columns[0];
                itemColumn.Columns.Clear();
            }
            else
            {
                itemColumn = new ProcedureResultColumn();
                column.Columns.Clear();
                column.Columns.Add(itemColumn);
            }

            itemColumn.ReturnsJson = false;
            itemColumn.ReturnsJsonArray = false;
            itemColumn.ReturnsUnknownJson = false;
            itemColumn.IsNestedJson = false;
            itemColumn.SqlTypeName = itemMetadata.SqlTypeName ?? itemMetadata.CastTargetType ?? "nvarchar(max)";
            itemColumn.CastTargetType = null;
            itemColumn.CastTargetLength = null;
            itemColumn.CastTargetPrecision = null;
            itemColumn.CastTargetScale = null;
            itemColumn.MaxLength = itemMetadata.MaxLength;
            itemColumn.IsNullable = itemMetadata.IsNullable ?? true;
            itemColumn.UserTypeSchemaName = itemMetadata.UserTypeSchema;
            itemColumn.UserTypeName = itemMetadata.UserTypeName;
            itemColumn.SourceSchema = null;
            itemColumn.SourceTable = null;
            itemColumn.SourceColumn = null;
            itemColumn.Reference = null;
            itemColumn.Name = "value";

            ApplyUserTypeResolution(itemColumn);

            AssignJsonElementMetadata(column, itemColumn, itemMetadata);

            itemColumn.IsAmbiguous = null;
        }

        private void ApplyJsonQueryParameterMetadata(ProcedureResultColumn column, IList<ScalarExpression>? parameters)
        {
            if (parameters == null || parameters.Count == 0)
            {
                return;
            }

            foreach (var parameter in parameters)
            {
                ApplyJsonQueryParameterMetadata(column, parameter);
            }
        }

        private void ApplyJsonQueryParameterMetadata(ProcedureResultColumn column, ScalarExpression? parameter)
        {
            if (column == null || parameter == null)
            {
                return;
            }

            if (parameter is ParenthesisExpression parenthesis)
            {
                ApplyJsonQueryParameterMetadata(column, parenthesis.Expression);
                return;
            }

            if (parameter is ScalarSubquery scalarSubquery)
            {
                var previousKind = column.ExpressionKind;
                try
                {
                    PopulateScalarSubquery(column, scalarSubquery);
                }
                finally
                {
                    column.ExpressionKind = previousKind;
                }

                if (column.Columns.Count > 0)
                {
                    column.ReturnsUnknownJson = false;
                }

                return;
            }
        }

        private static void AssignJsonElementMetadata(ProcedureResultColumn container, ProcedureResultColumn elementColumn, ColumnSourceInfo itemMetadata)
        {
            if (container == null || elementColumn == null || itemMetadata == null)
            {
                return;
            }

            if (elementColumn.Columns.Count > 0)
            {
                return;
            }

            var sqlType = elementColumn.SqlTypeName;
            if (string.IsNullOrWhiteSpace(sqlType))
            {
                sqlType = itemMetadata.SqlTypeName ?? itemMetadata.CastTargetType;
            }

            if (string.IsNullOrWhiteSpace(sqlType) && !string.IsNullOrWhiteSpace(itemMetadata.UserTypeName))
            {
                sqlType = BuildTypeRef(itemMetadata.Catalog, itemMetadata.UserTypeSchema, itemMetadata.UserTypeName);
            }

            if (string.IsNullOrWhiteSpace(sqlType))
            {
                return;
            }

            container.JsonElementSqlType ??= sqlType;

            var elementNullable = elementColumn.IsNullable ?? itemMetadata.IsNullable ?? true;
            container.JsonElementClrType ??= SqlClrTypeMapper.Map(sqlType, elementNullable);
        }

        private static bool IsBracketLiteral(ScalarExpression? expression, char expected)
        {
            if (expression is StringLiteral stringLiteral && !string.IsNullOrWhiteSpace(stringLiteral.Value))
            {
                var trimmed = stringLiteral.Value.Trim();
                return trimmed.Length == 1 && trimmed[0] == expected;
            }

            if (expression is Literal literal && !string.IsNullOrWhiteSpace(literal.Value))
            {
                var trimmed = literal.Value.Trim();
                return trimmed.Length == 1 && trimmed[0] == expected;
            }

            return false;
        }

        private void ApplyNullCoalesceFunctionFallback(ProcedureResultColumn column, FunctionCall functionCall, bool treatAsIsNull)
        {
            if (column == null || functionCall?.Parameters == null || functionCall.Parameters.Count == 0)
            {
                return;
            }

            ColumnSourceInfo? aggregated = null;

            foreach (var parameter in functionCall.Parameters)
            {
                var metadata = ExtractExpressionMetadata(parameter);
                if (metadata == null)
                {
                    continue;
                }

                aggregated = aggregated == null ? metadata : MergeColumnSourceInfo(aggregated, metadata);
            }

            if (aggregated == null)
            {
                return;
            }

            ApplyTypeMetadata(column, aggregated);

            if (aggregated.IsNullable.HasValue)
            {
                column.IsNullable ??= aggregated.IsNullable;
            }
            else if (treatAsIsNull && !column.IsNullable.HasValue)
            {
                column.IsNullable = false;
            }
        }

        private ColumnSourceInfo? ExtractExpressionMetadata(ScalarExpression? expression)
        {
            if (expression == null)
            {
                return null;
            }

            var probe = new ProcedureResultColumn();
            ApplyExpressionMetadata(probe, expression);

            var metadata = CreateMetadataFromProbe(probe);
            var hasTypeMetadata = HasResolvedTypeMetadata(metadata);

            if (!hasTypeMetadata)
            {
                var literalMetadata = InferLiteralColumnMetadata(expression);
                if (literalMetadata != null)
                {
                    metadata = metadata == null ? literalMetadata : MergeColumnSourceInfo(metadata, literalMetadata);
                    hasTypeMetadata = HasResolvedTypeMetadata(metadata);
                }
            }

            return hasTypeMetadata ? metadata : null;
        }

        private static bool HasResolvedTypeMetadata(ColumnSourceInfo? metadata)
        {
            if (metadata == null)
            {
                return false;
            }

            return !string.IsNullOrWhiteSpace(metadata.SqlTypeName) ||
                   !string.IsNullOrWhiteSpace(metadata.CastTargetType) ||
                   !string.IsNullOrWhiteSpace(metadata.UserTypeName) ||
                   !string.IsNullOrWhiteSpace(metadata.UserTypeSchema) ||
                   metadata.MaxLength.HasValue ||
                   metadata.Precision.HasValue ||
                   metadata.Scale.HasValue;
        }

        private void PopulateIIfCall(ProcedureResultColumn column, IIfCall iifCall)
        {
            column.ExpressionKind = ProcedureResultColumnExpressionKind.FunctionCall;

            var candidates = new List<ColumnSourceCandidate>();
            var thenSource = ExtractColumnSource(iifCall.ThenExpression);
            if (thenSource != null)
            {
                candidates.Add(thenSource);
            }

            var elseSource = ExtractColumnSource(iifCall.ElseExpression);
            if (elseSource != null)
            {
                candidates.Add(elseSource);
            }

            AssignColumnSource(column, candidates);

            // Propagate branch metadata (casts, literals, column refs) before falling back to literal inference.
            ApplyExpressionMetadata(column, iifCall.ThenExpression);
            ApplyExpressionMetadata(column, iifCall.ElseExpression);

            if (string.IsNullOrWhiteSpace(column.SqlTypeName))
            {
                ApplyIIfLiteralFallback(column, iifCall);
            }
        }

        private static void ApplyIIfLiteralFallback(ProcedureResultColumn column, IIfCall iifCall)
        {
            if (column == null || iifCall == null)
            {
                return;
            }

            var thenMetadata = InferLiteralColumnMetadata(iifCall.ThenExpression);
            var elseMetadata = InferLiteralColumnMetadata(iifCall.ElseExpression);

            if (thenMetadata == null && elseMetadata == null)
            {
                return;
            }

            var effective = MergeLiteralMetadata(thenMetadata, elseMetadata);
            if (effective == null)
            {
                return;
            }

            ApplyTypeMetadata(column, effective);

            if (effective.IsNullable.HasValue)
            {
                column.IsNullable ??= effective.IsNullable;
            }
        }

        private static void ApplyCoalesceLiteralFallback(ProcedureResultColumn column, IEnumerable<ScalarExpression>? expressions)
        {
            if (column == null || expressions == null)
            {
                return;
            }

            ColumnSourceInfo? merged = null;
            foreach (var expression in expressions)
            {
                var literalMetadata = InferLiteralColumnMetadata(expression);
                if (literalMetadata == null)
                {
                    continue;
                }

                merged = merged == null ? literalMetadata : MergeLiteralMetadata(merged, literalMetadata);
            }

            if (merged == null)
            {
                return;
            }

            ApplyTypeMetadata(column, merged);

            if (merged.IsNullable.HasValue)
            {
                column.IsNullable ??= merged.IsNullable;
            }
        }

        private static ColumnSourceInfo? InferLiteralColumnMetadata(ScalarExpression? expression)
        {
            switch (expression)
            {
                case IntegerLiteral:
                    return new ColumnSourceInfo
                    {
                        SqlTypeName = "int",
                        IsNullable = false
                    };
                case NumericLiteral numeric:
                    var value = numeric.Value ?? string.Empty;
                    var hasFraction = value.Contains('.', StringComparison.Ordinal) ||
                                      value.Contains('e', StringComparison.OrdinalIgnoreCase);
                    return new ColumnSourceInfo
                    {
                        SqlTypeName = hasFraction ? "decimal" : "int",
                        CastTargetPrecision = hasFraction ? 38 : (int?)null,
                        CastTargetScale = hasFraction ? 10 : (int?)null,
                        IsNullable = false
                    };
                case StringLiteral str:
                    var length = str.Value?.Length ?? 0;
                    return new ColumnSourceInfo
                    {
                        SqlTypeName = "nvarchar",
                        MaxLength = length <= 0 ? 1 : length,
                        IsNullable = false
                    };
                case NullLiteral:
                    return new ColumnSourceInfo
                    {
                        IsNullable = true
                    };
                case ParenthesisExpression parenthesis:
                    return InferLiteralColumnMetadata(parenthesis.Expression);
                default:
                    return null;
            }
        }

        private static ColumnSourceInfo? MergeLiteralMetadata(ColumnSourceInfo? first, ColumnSourceInfo? second)
        {
            if (first == null && second == null)
            {
                return null;
            }

            if (first == null)
            {
                return second;
            }

            if (second == null)
            {
                return first;
            }

            var primary = !string.IsNullOrWhiteSpace(first.SqlTypeName) ? first : second;
            var secondary = ReferenceEquals(primary, first) ? second : first;

            var isNullable = (first.IsNullable == true) || (second.IsNullable == true)
                ? true
                : primary.IsNullable;

            return new ColumnSourceInfo
            {
                Catalog = primary.Catalog ?? secondary.Catalog,
                Schema = primary.Schema ?? secondary.Schema,
                Table = primary.Table ?? secondary.Table,
                Column = primary.Column ?? secondary.Column,
                Reference = primary.Reference ?? secondary.Reference,
                ReturnsJson = primary.ReturnsJson ?? secondary.ReturnsJson,
                ReturnsJsonArray = primary.ReturnsJsonArray ?? secondary.ReturnsJsonArray,
                ReturnsUnknownJson = primary.ReturnsUnknownJson ?? secondary.ReturnsUnknownJson,
                IsNestedJson = primary.IsNestedJson ?? secondary.IsNestedJson,
                SqlTypeName = primary.SqlTypeName,
                MaxLength = primary.MaxLength ?? secondary.MaxLength,
                CastTargetType = primary.CastTargetType ?? secondary.CastTargetType,
                CastTargetLength = primary.CastTargetLength ?? secondary.CastTargetLength,
                CastTargetPrecision = primary.CastTargetPrecision ?? secondary.CastTargetPrecision,
                CastTargetScale = primary.CastTargetScale ?? secondary.CastTargetScale,
                IsNullable = isNullable
            };
        }

        private void PopulateColumnReference(ProcedureResultColumn column, ColumnReferenceExpression columnRef)
        {
            column.ExpressionKind = ProcedureResultColumnExpressionKind.ColumnRef;
            if (columnRef?.MultiPartIdentifier == null || columnRef.MultiPartIdentifier.Count == 0)
            {
                return;
            }

            var identifiers = columnRef.MultiPartIdentifier.Identifiers;
            column.SourceColumn = identifiers[^1].Value;
            if (identifiers.Count >= 2 && string.IsNullOrWhiteSpace(column.SourceAlias))
            {
                column.SourceAlias = identifiers[^2].Value;
            }

            if (identifiers.Count >= 2)
            {
                var tableOrAlias = identifiers[^2].Value;
                var resolved = ResolveTableAlias(tableOrAlias);
                if (resolved != null)
                {
                    column.SourceTable = resolved.Name;
                    column.SourceCatalog = resolved.Catalog;
                    if (!string.IsNullOrWhiteSpace(resolved.Schema))
                    {
                        column.SourceSchema = resolved.Schema;
                    }
                    else
                    {
                        column.SourceSchema = null;
                    }

                    if (string.IsNullOrWhiteSpace(column.SourceCatalog) && !resolved.IsCte && !string.IsNullOrWhiteSpace(_defaultCatalog))
                    {
                        column.SourceCatalog = _defaultCatalog;
                    }

                    var schemaUpdated = false;
                    var tableUpdated = false;
                    var columnUpdated = false;
                    var referenceUpdated = false;
                    var jsonMetadataUpdated = false;
                    var typeMetadataUpdated = false;

                    if (resolved.Columns != null && !string.IsNullOrWhiteSpace(column.SourceColumn) && resolved.Columns.TryGetValue(column.SourceColumn, out var sourceInfo))
                    {
                        if (IsAliasDebugEnabled())
                        {
                            var hasReference = sourceInfo?.Reference != null;
                            Console.WriteLine(
                                $"[alias-column-source] alias={tableOrAlias ?? "<null>"} lookup={column.SourceColumn ?? "<null>"} hasReference={hasReference}");
                        }

                        if (sourceInfo != null)
                        {
                            if (!string.IsNullOrWhiteSpace(sourceInfo.Schema))
                            {
                                column.SourceSchema = sourceInfo.Schema;
                                schemaUpdated = true;
                            }

                            if (!string.IsNullOrWhiteSpace(sourceInfo.Catalog))
                            {
                                column.SourceCatalog = sourceInfo.Catalog;
                            }
                            else if (!string.IsNullOrWhiteSpace(resolved.Catalog))
                            {
                                column.SourceCatalog ??= resolved.Catalog;
                            }

                            if (!string.IsNullOrWhiteSpace(sourceInfo.Table))
                            {
                                column.SourceTable = sourceInfo.Table;
                                tableUpdated = true;
                            }

                            if (!string.IsNullOrWhiteSpace(sourceInfo.Column))
                            {
                                column.SourceColumn = sourceInfo.Column;
                                columnUpdated = true;
                            }

                            if (sourceInfo.Reference != null && column.Reference == null)
                            {
                                column.Reference = CloneReference(sourceInfo.Reference);
                                referenceUpdated = true;
                            }

                            if (sourceInfo.ReturnsJson.HasValue)
                            {
                                column.ReturnsJson ??= sourceInfo.ReturnsJson;
                                jsonMetadataUpdated = true;
                            }

                            if (sourceInfo.ReturnsJsonArray.HasValue)
                            {
                                column.ReturnsJsonArray ??= sourceInfo.ReturnsJsonArray;
                                jsonMetadataUpdated = true;
                            }

                            if (sourceInfo.ReturnsUnknownJson.HasValue)
                            {
                                column.ReturnsUnknownJson ??= sourceInfo.ReturnsUnknownJson;
                                jsonMetadataUpdated = true;
                            }

                            if (sourceInfo.IsNestedJson.HasValue)
                            {
                                column.IsNestedJson ??= sourceInfo.IsNestedJson;
                                jsonMetadataUpdated = true;
                            }

                            if (!string.IsNullOrWhiteSpace(sourceInfo.SqlTypeName))
                            {
                                var before = column.SqlTypeName;
                                column.SqlTypeName ??= sourceInfo.SqlTypeName;
                                if (!string.Equals(before, column.SqlTypeName, StringComparison.OrdinalIgnoreCase))
                                {
                                    typeMetadataUpdated = true;
                                }
                            }

                            if (!string.IsNullOrWhiteSpace(sourceInfo.CastTargetType))
                            {
                                var before = column.CastTargetType;
                                column.CastTargetType ??= sourceInfo.CastTargetType;
                                if (!string.Equals(before, column.CastTargetType, StringComparison.OrdinalIgnoreCase))
                                {
                                    typeMetadataUpdated = true;
                                }
                            }

                            if (sourceInfo.CastTargetLength.HasValue)
                            {
                                var before = column.CastTargetLength;
                                column.CastTargetLength ??= sourceInfo.CastTargetLength;
                                if (before != column.CastTargetLength)
                                {
                                    typeMetadataUpdated = true;
                                }
                            }

                            if (sourceInfo.CastTargetPrecision.HasValue)
                            {
                                var before = column.CastTargetPrecision;
                                column.CastTargetPrecision ??= sourceInfo.CastTargetPrecision;
                                if (before != column.CastTargetPrecision)
                                {
                                    typeMetadataUpdated = true;
                                }
                            }

                            if (sourceInfo.CastTargetScale.HasValue)
                            {
                                var before = column.CastTargetScale;
                                column.CastTargetScale ??= sourceInfo.CastTargetScale;
                                if (before != column.CastTargetScale)
                                {
                                    typeMetadataUpdated = true;
                                }
                            }

                            if (sourceInfo.MaxLength.HasValue)
                            {
                                var before = column.MaxLength;
                                column.MaxLength ??= sourceInfo.MaxLength;
                                if (before != column.MaxLength)
                                {
                                    typeMetadataUpdated = true;
                                }
                            }

                            if (sourceInfo.IsNullable.HasValue)
                            {
                                var before = column.IsNullable;
                                column.IsNullable ??= sourceInfo.IsNullable;
                                if (before != column.IsNullable)
                                {
                                    typeMetadataUpdated = true;
                                }
                            }

                            if (!string.IsNullOrWhiteSpace(sourceInfo.UserTypeSchema))
                            {
                                var before = column.UserTypeSchemaName;
                                column.UserTypeSchemaName ??= sourceInfo.UserTypeSchema;
                                if (!string.Equals(before, column.UserTypeSchemaName, StringComparison.OrdinalIgnoreCase))
                                {
                                    typeMetadataUpdated = true;
                                }
                            }

                            if (!string.IsNullOrWhiteSpace(sourceInfo.UserTypeName))
                            {
                                var before = column.UserTypeName;
                                column.UserTypeName ??= sourceInfo.UserTypeName;
                                if (!string.Equals(before, column.UserTypeName, StringComparison.OrdinalIgnoreCase))
                                {
                                    typeMetadataUpdated = true;
                                }
                            }

                            ApplyUserTypeResolution(column, sourceInfo);
                        }

                        if (resolved.IsFunction && column.Reference == null && !string.IsNullOrWhiteSpace(resolved.Schema) && !string.IsNullOrWhiteSpace(resolved.Name))
                        {
                            column.Reference = new ProcedureReferenceModel
                            {
                                Kind = ProcedureReferenceKind.Function,
                                Schema = resolved.Schema,
                                Name = resolved.Name
                            };
                        }
                    }

                    if (IsAliasDebugEnabled())
                    {
                        var referenceKind = column.Reference?.Kind.ToString() ?? "<null>";
                        var referenceName = column.Reference?.Name ?? "<null>";
                        var referenceSchema = column.Reference?.Schema ?? "<null>";
                        Console.WriteLine(
                            $"[alias-column] alias={tableOrAlias ?? "<null>"} sourceTable={column.SourceTable ?? "<null>"} sourceColumn={column.SourceColumn ?? "<null>"} refKind={referenceKind} refSchema={referenceSchema} refName={referenceName}");
                    }

                    var aliasResolutionApplied = schemaUpdated || tableUpdated || columnUpdated || referenceUpdated || jsonMetadataUpdated || typeMetadataUpdated;

                    if (!aliasResolutionApplied && string.IsNullOrWhiteSpace(resolved.Schema))
                    {
                        column.SourceSchema = null;
                        column.SourceTable = null;
                    }
                    else if (string.IsNullOrWhiteSpace(resolved.Schema))
                    {
                        if (resolved.IsCte)
                        {
                            if (!tableUpdated || string.IsNullOrWhiteSpace(column.SourceTable) || string.Equals(column.SourceTable, resolved.Name, StringComparison.OrdinalIgnoreCase))
                            {
                                if (!tableUpdated)
                                {
                                    column.SourceSchema = null;
                                }

                                if (!tableUpdated || string.Equals(column.SourceTable, resolved.Name, StringComparison.OrdinalIgnoreCase))
                                {
                                    column.SourceTable = null;
                                }
                            }
                        }
                        else if (!tableUpdated && string.IsNullOrWhiteSpace(column.SourceTable))
                        {
                            column.SourceSchema = null;
                        }
                    }

                    if (!string.IsNullOrWhiteSpace(column.SourceTable) && _cteColumnCache.ContainsKey(column.SourceTable))
                    {
                        column.SourceSchema = null;
                        column.SourceTable = null;
                    }

                    if (resolved.ForceNullableColumns)
                    {
                        column.IsNullable = true;
                        column.ForcedNullable ??= true;
                    }
                }
                else
                {
                    column.SourceTable = tableOrAlias;
                    if (identifiers.Count >= 3)
                    {
                        column.SourceSchema = identifiers[^3].Value;
                    }
                    else if (!string.IsNullOrWhiteSpace(_defaultSchema))
                    {
                        column.SourceSchema ??= _defaultSchema;
                    }

                    if (identifiers.Count >= 4)
                    {
                        column.SourceCatalog = identifiers[^4].Value;
                    }
                    else if (!string.IsNullOrWhiteSpace(_defaultCatalog))
                    {
                        column.SourceCatalog ??= _defaultCatalog;
                    }
                }
            }
            else
            {
                if (TryApplyColumnMetadataFromScopes(column, column.SourceColumn))
                {
                    return;
                }

                var implicitSource = ResolveImplicitTableForUnqualifiedColumn();
                if (implicitSource.HasValue)
                {
                    column.SourceTable ??= implicitSource.Value.Name;
                    column.SourceSchema ??= implicitSource.Value.Schema ?? _defaultSchema;
                    column.SourceCatalog ??= implicitSource.Value.Catalog ?? _defaultCatalog;
                }
                else if (!string.IsNullOrWhiteSpace(_defaultSchema))
                {
                    column.SourceSchema ??= _defaultSchema;
                }
            }
        }

        private (string? Schema, string FunctionName)? ExtractEnhancedJsonFunctionInfo(FunctionCall functionCall, string? defaultSchema)
        {
            try
            {
                // Use the SubqueryFunctionAnalyzer to extract the inner function from JSON_QUERY
                var subqueryAnalyzer = new SubqueryFunctionAnalyzer();
                functionCall.Accept(subqueryAnalyzer);

                var extractedFunction = subqueryAnalyzer.ExtractedFunction;
                if (extractedFunction != null && !string.IsNullOrEmpty(extractedFunction.FunctionName))
                {
                    if (BuiltInFunctionCatalog.Contains(extractedFunction.FunctionName))
                    {
                        return null;
                    }

                    var schema = extractedFunction.SchemaName;
                    if (string.IsNullOrWhiteSpace(schema))
                    {
                        schema = defaultSchema;
                    }
                    return (schema, extractedFunction.FunctionName);
                }
            }
            catch
            {
                // Fallback to original behavior if analysis fails
            }

            return null;
        }

        private static void EnsureUniqueColumnName(ProcedureResultColumn column, HashSet<string> usedNames)
        {
            if (column == null)
            {
                return;
            }

            var candidate = NormalizeColumnNameCandidate(column);
            if (string.IsNullOrWhiteSpace(candidate))
            {
                candidate = BuildFallbackColumnName(column, usedNames.Count);
            }

            candidate = ResolveColumnNameCollision(candidate, column, usedNames);
            column.Name = candidate;
            usedNames.Add(candidate);
        }

        private static string NormalizeColumnNameCandidate(ProcedureResultColumn column)
        {
            if (!string.IsNullOrWhiteSpace(column.Name))
            {
                return column.Name.Trim();
            }

            if (!string.IsNullOrWhiteSpace(column.SourceColumn))
            {
                return column.SourceColumn.Trim();
            }

            if (!string.IsNullOrWhiteSpace(column.SourceAlias))
            {
                return column.SourceAlias.Trim();
            }

            if (!string.IsNullOrWhiteSpace(column.Reference?.Name))
            {
                return column.Reference.Name.Trim();
            }

            return string.Empty;
        }

        private static string BuildFallbackColumnName(ProcedureResultColumn column, int index)
        {
            if (!string.IsNullOrWhiteSpace(column.SourceTable) && !string.IsNullOrWhiteSpace(column.SourceColumn))
            {
                return string.Concat(column.SourceTable.Trim(), "_", column.SourceColumn.Trim());
            }

            if (!string.IsNullOrWhiteSpace(column.SourceColumn))
            {
                return column.SourceColumn.Trim();
            }

            return string.Concat("Column", index + 1);
        }

        private static string ResolveColumnNameCollision(string candidate, ProcedureResultColumn column, HashSet<string> usedNames)
        {
            var normalized = candidate.Trim();
            if (normalized.Length == 0)
            {
                normalized = "Column";
            }

            if (!usedNames.Contains(normalized))
            {
                return normalized;
            }

            var alternatives = new List<string>();
            if (!string.IsNullOrWhiteSpace(column.SourceTable) && !string.IsNullOrWhiteSpace(column.SourceColumn))
            {
                alternatives.Add(string.Concat(column.SourceTable.Trim(), "_", column.SourceColumn.Trim()));
            }

            if (!string.IsNullOrWhiteSpace(column.SourceAlias) && !string.IsNullOrWhiteSpace(column.SourceColumn))
            {
                alternatives.Add(string.Concat(column.SourceAlias.Trim(), "_", column.SourceColumn.Trim()));
            }

            if (!string.IsNullOrWhiteSpace(column.Reference?.Name) && !string.IsNullOrWhiteSpace(column.SourceColumn))
            {
                alternatives.Add(string.Concat(column.Reference.Name.Trim(), "_", column.SourceColumn.Trim()));
            }

            foreach (var alt in alternatives)
            {
                var altNormalized = alt.Trim();
                if (altNormalized.Length == 0)
                {
                    continue;
                }

                if (!usedNames.Contains(altNormalized))
                {
                    return altNormalized;
                }
            }

            var baseName = normalized;
            var suffix = 2;
            string attempt;
            do
            {
                attempt = string.Concat(baseName, "_", suffix);
                suffix++;
            }
            while (usedNames.Contains(attempt));

            return attempt;
        }

        private void TryPopulateSourceFromFunctionArguments(ProcedureResultColumn column, FunctionCall functionCall)
        {
            if (column == null || functionCall?.Parameters == null || functionCall.Parameters.Count == 0)
            {
                return;
            }

            var functionName = functionCall.FunctionName?.Value;
            var metadataOnly = BuiltInFunctionCatalog.IsMetadataPropagationOnly(functionName);

            var candidates = new List<ColumnSourceCandidate>();

            foreach (var parameter in functionCall.Parameters)
            {
                var source = ExtractColumnSource(parameter);
                if (source == null)
                {
                    continue;
                }

                candidates.Add(source);
            }

            if (metadataOnly)
            {
                foreach (var candidate in candidates)
                {
                    if (candidate?.Probe != null)
                    {
                        ApplyProbeMetadata(column, candidate.Probe);
                    }
                }

                return;
            }

            AssignColumnSource(column, candidates);
        }

        private ColumnSourceCandidate? ExtractColumnSource(ScalarExpression? expression)
        {
            switch (expression)
            {
                case null:
                    return null;
                case ColumnReferenceExpression columnRef:
                    var probe = new ProcedureResultColumn();
                    PopulateColumnReference(probe, columnRef);
                    if (string.IsNullOrWhiteSpace(probe.SourceColumn))
                    {
                        return null;
                    }

                    return new ColumnSourceCandidate
                    {
                        Catalog = probe.SourceCatalog,
                        Schema = probe.SourceSchema,
                        Table = probe.SourceTable,
                        Column = probe.SourceColumn!,
                        Probe = probe
                    };
                case CastCall castCall when castCall.Parameter != null:
                    return ExtractColumnSource(castCall.Parameter);
                case ScalarSubquery subquery when subquery.QueryExpression is QuerySpecification nestedQuery:
                    if (nestedQuery.SelectElements != null)
                    {
                        foreach (var element in nestedQuery.SelectElements)
                        {
                            if (element is SelectScalarExpression nestedScalar && nestedScalar.Expression != null)
                            {
                                var candidate = ExtractColumnSource(nestedScalar.Expression);
                                if (candidate != null)
                                {
                                    return candidate;
                                }
                            }
                        }
                    }

                    break;
                case FunctionCall nestedFunction when IsPassThroughFunction(nestedFunction):
                    foreach (var argument in nestedFunction.Parameters)
                    {
                        var candidate = ExtractColumnSource(argument);
                        if (candidate != null)
                        {
                            return candidate;
                        }
                    }

                    break;
                case ParenthesisExpression parenthesis when parenthesis.Expression != null:
                    return ExtractColumnSource(parenthesis.Expression);
                case IIfCall iifCall:
                    var thenCandidate = ExtractColumnSource(iifCall.ThenExpression);
                    var elseCandidate = ExtractColumnSource(iifCall.ElseExpression);

                    if (thenCandidate == null && elseCandidate == null)
                    {
                        return null;
                    }

                    if (thenCandidate == null || elseCandidate == null)
                    {
                        return thenCandidate ?? elseCandidate;
                    }

                    if (AreSameColumnSource(thenCandidate, elseCandidate))
                    {
                        return CombineCandidates(thenCandidate, elseCandidate);
                    }

                    var thenScore = ComputeCandidateScore(thenCandidate);
                    var elseScore = ComputeCandidateScore(elseCandidate);
                    var preferred = thenScore >= elseScore ? thenCandidate : elseCandidate;
                    var fallback = ReferenceEquals(preferred, thenCandidate) ? elseCandidate : thenCandidate;

                    LogVerbose($"[iif-source-mismatch] then={DescribeCandidate(thenCandidate)} (score={thenScore}) else={DescribeCandidate(elseCandidate)} (score={elseScore}) selected={DescribeCandidate(preferred)}");

                    return CombineCandidates(preferred, fallback);
            }

            return null;
        }

        private static bool AreSameColumnSource(ColumnSourceCandidate first, ColumnSourceCandidate second)
        {
            if (first == null || second == null)
            {
                return false;
            }

            if (string.IsNullOrWhiteSpace(first.Column) || string.IsNullOrWhiteSpace(second.Column))
            {
                return false;
            }

            if (!string.Equals(first.Column, second.Column, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            if (!string.IsNullOrWhiteSpace(first.Table) && !string.IsNullOrWhiteSpace(second.Table) &&
                !string.Equals(first.Table, second.Table, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            if (!string.IsNullOrWhiteSpace(first.Schema) && !string.IsNullOrWhiteSpace(second.Schema) &&
                !string.Equals(first.Schema, second.Schema, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            if (!string.IsNullOrWhiteSpace(first.Catalog) && !string.IsNullOrWhiteSpace(second.Catalog) &&
                !string.Equals(first.Catalog, second.Catalog, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            return true;
        }

        private static ColumnSourceCandidate CombineCandidates(ColumnSourceCandidate primary, ColumnSourceCandidate secondary)
        {
            var columnName = !string.IsNullOrWhiteSpace(primary.Column)
                ? primary.Column
                : secondary.Column;

            var mergedProbe = MergeProbes(primary.Probe, secondary.Probe);

            return new ColumnSourceCandidate
            {
                Catalog = string.IsNullOrWhiteSpace(primary.Catalog) ? secondary.Catalog : primary.Catalog,
                Schema = string.IsNullOrWhiteSpace(primary.Schema) ? secondary.Schema : primary.Schema,
                Table = string.IsNullOrWhiteSpace(primary.Table) ? secondary.Table : primary.Table,
                Column = columnName ?? string.Empty,
                Probe = mergedProbe
            };
        }

        private static ProcedureResultColumn? MergeProbes(ProcedureResultColumn? primary, ProcedureResultColumn? secondary)
        {
            if (primary == null && secondary == null)
            {
                return null;
            }

            if (primary == null)
            {
                return CloneProbeColumn(secondary!);
            }

            var merged = CloneProbeColumn(primary);

            if (secondary != null)
            {
                if (string.IsNullOrWhiteSpace(merged.SourceSchema))
                {
                    merged.SourceSchema = secondary.SourceSchema;
                }

                if (string.IsNullOrWhiteSpace(merged.SourceCatalog))
                {
                    merged.SourceCatalog = secondary.SourceCatalog;
                }

                if (string.IsNullOrWhiteSpace(merged.SourceTable))
                {
                    merged.SourceTable = secondary.SourceTable;
                }

                if (string.IsNullOrWhiteSpace(merged.SourceColumn))
                {
                    merged.SourceColumn = secondary.SourceColumn;
                }

                if (merged.Reference == null && secondary.Reference != null)
                {
                    merged.Reference = CloneReference(secondary.Reference);
                }

                ApplyProbeMetadata(merged, secondary);
            }

            return merged;
        }

        private static ProcedureResultColumn CloneProbeColumn(ProcedureResultColumn source)
        {
            return new ProcedureResultColumn
            {
                Name = source.Name,
                Alias = source.Alias,
                SourceCatalog = source.SourceCatalog,
                SourceSchema = source.SourceSchema,
                SourceTable = source.SourceTable,
                SourceColumn = source.SourceColumn,
                SqlTypeName = source.SqlTypeName,
                CastTargetType = source.CastTargetType,
                CastTargetLength = source.CastTargetLength,
                CastTargetPrecision = source.CastTargetPrecision,
                CastTargetScale = source.CastTargetScale,
                MaxLength = source.MaxLength,
                IsNullable = source.IsNullable,
                UserTypeCatalogName = source.UserTypeCatalogName,
                UserTypeSchemaName = source.UserTypeSchemaName,
                UserTypeName = source.UserTypeName,
                ReturnsJson = source.ReturnsJson,
                ReturnsJsonArray = source.ReturnsJsonArray,
                ReturnsUnknownJson = source.ReturnsUnknownJson,
                IsNestedJson = source.IsNestedJson,
                Reference = source.Reference == null ? null : CloneReference(source.Reference)
            };
        }

        private static int ComputeCandidateScore(ColumnSourceCandidate candidate)
        {
            if (candidate == null)
            {
                return 0;
            }

            var score = 0;

            if (!string.IsNullOrWhiteSpace(candidate.Schema))
            {
                score += 4;
            }

            if (!string.IsNullOrWhiteSpace(candidate.Table))
            {
                score += 4;
            }

            if (!string.IsNullOrWhiteSpace(candidate.Column))
            {
                score += 10;
            }

            if (candidate.Probe != null)
            {
                score += ComputeProbeScore(candidate.Probe);
            }

            return score;
        }

        private static int ComputeProbeScore(ProcedureResultColumn probe)
        {
            var score = 0;

            if (!string.IsNullOrWhiteSpace(probe.CastTargetType))
            {
                score += 20;
            }

            if (!string.IsNullOrWhiteSpace(probe.SqlTypeName))
            {
                score += 15;
            }

            if (probe.CastTargetPrecision.HasValue)
            {
                score += 6;
            }

            if (probe.CastTargetScale.HasValue)
            {
                score += 6;
            }

            if (probe.CastTargetLength.HasValue)
            {
                score += 8 + NormalizeLengthScore(probe.CastTargetLength.Value);
            }

            if (probe.MaxLength.HasValue)
            {
                score += 5 + NormalizeLengthScore(probe.MaxLength.Value);
            }

            if (!string.IsNullOrWhiteSpace(probe.UserTypeName))
            {
                score += 10;
            }

            if (!string.IsNullOrWhiteSpace(probe.UserTypeSchemaName))
            {
                score += 4;
            }

            return score;
        }

        private static int NormalizeLengthScore(int value)
        {
            if (value <= 0)
            {
                return 4;
            }

            var bounded = Math.Min(value, 8000);
            return bounded / 256;
        }

        private static string DescribeCandidate(ColumnSourceCandidate candidate)
        {
            if (candidate == null)
            {
                return "<null>";
            }

            var parts = new List<string>();

            if (!string.IsNullOrWhiteSpace(candidate.Schema))
            {
                parts.Add(candidate.Schema!);
            }

            if (!string.IsNullOrWhiteSpace(candidate.Table))
            {
                parts.Add(candidate.Table!);
            }

            if (!string.IsNullOrWhiteSpace(candidate.Column))
            {
                parts.Add(candidate.Column);
            }

            var location = parts.Count > 0 ? string.Join('.', parts) : "<unknown>";

            if (candidate.Probe == null)
            {
                return location;
            }

            var descriptors = new List<string>();
            var probe = candidate.Probe;

            if (!string.IsNullOrWhiteSpace(probe.CastTargetType))
            {
                descriptors.Add($"cast={probe.CastTargetType}");

                var castDetails = BuildTypeDetailDescriptor(probe.CastTargetLength, probe.CastTargetPrecision, probe.CastTargetScale);
                if (!string.IsNullOrEmpty(castDetails))
                {
                    descriptors.Add(castDetails);
                }
            }
            else if (!string.IsNullOrWhiteSpace(probe.SqlTypeName))
            {
                descriptors.Add($"type={probe.SqlTypeName}");

                if (probe.MaxLength.HasValue)
                {
                    descriptors.Add($"maxLength={FormatLength(probe.MaxLength.Value)}");
                }
            }

            if (!string.IsNullOrWhiteSpace(probe.UserTypeName))
            {
                var qualified = string.IsNullOrWhiteSpace(probe.UserTypeSchemaName)
                    ? probe.UserTypeName
                    : string.Concat(probe.UserTypeSchemaName, ".", probe.UserTypeName);

                descriptors.Add($"udt={qualified}");
            }

            if (descriptors.Count == 0)
            {
                return location;
            }

            return string.Concat(location, " [", string.Join(", ", descriptors), "]");
        }

        private static string? BuildTypeDetailDescriptor(int? length, int? precision, int? scale)
        {
            if (precision.HasValue || scale.HasValue)
            {
                return string.Concat(
                    "precision=",
                    precision?.ToString(CultureInfo.InvariantCulture) ?? "?",
                    ", scale=",
                    scale?.ToString(CultureInfo.InvariantCulture) ?? "?");
            }

            if (length.HasValue)
            {
                return string.Concat("length=", FormatLength(length.Value));
            }

            return null;
        }

        private static string FormatLength(int value)
        {
            return value <= 0 ? "max" : value.ToString(CultureInfo.InvariantCulture);
        }

        private void AssignColumnSource(ProcedureResultColumn column, IReadOnlyList<ColumnSourceCandidate> candidates)
        {
            if (column == null || candidates == null || candidates.Count == 0)
            {
                return;
            }

            string? selectedSchema = column.SourceSchema;
            string? selectedTable = column.SourceTable;
            string? selectedColumn = column.SourceColumn;
            var distinctSources = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            ColumnSourceCandidate? selectedCandidate = null;

            foreach (var candidate in candidates)
            {
                var schema = candidate.Schema;
                var table = candidate.Table;
                var col = candidate.Column;
                if (string.IsNullOrWhiteSpace(col))
                {
                    continue;
                }

                var key = string.Concat(schema ?? string.Empty, "|", table ?? string.Empty, "|", col);
                distinctSources.Add(key);

                if (string.IsNullOrWhiteSpace(selectedColumn))
                {
                    selectedSchema = schema;
                    selectedTable = table;
                    selectedColumn = col;
                    selectedCandidate ??= candidate;
                }

                if (selectedCandidate == null && !string.IsNullOrWhiteSpace(column.SourceColumn) && string.Equals(column.SourceColumn, col, StringComparison.OrdinalIgnoreCase))
                {
                    selectedCandidate = candidate;
                }
            }

            if (!string.IsNullOrWhiteSpace(selectedColumn))
            {
                column.SourceSchema ??= selectedSchema;
                column.SourceTable ??= selectedTable;
                column.SourceColumn ??= selectedColumn;
            }

            if (distinctSources.Count > 1)
            {
                column.IsAmbiguous = true;
            }

            if (selectedCandidate == null && candidates.Count == 1)
            {
                selectedCandidate = candidates[0];
            }

            if (selectedCandidate?.Probe != null)
            {
                ApplyProbeMetadata(column, selectedCandidate.Probe);
            }
        }

        private static void ApplyProbeMetadata(ProcedureResultColumn column, ProcedureResultColumn probe)
        {
            if (column == null || probe == null)
            {
                return;
            }

            if (!string.IsNullOrWhiteSpace(probe.SqlTypeName))
            {
                column.SqlTypeName ??= probe.SqlTypeName;
            }

            if (!string.IsNullOrWhiteSpace(probe.CastTargetType))
            {
                column.CastTargetType ??= probe.CastTargetType;
            }

            if (probe.CastTargetLength.HasValue)
            {
                column.CastTargetLength ??= probe.CastTargetLength;
            }

            if (probe.CastTargetPrecision.HasValue)
            {
                column.CastTargetPrecision ??= probe.CastTargetPrecision;
            }

            if (probe.CastTargetScale.HasValue)
            {
                column.CastTargetScale ??= probe.CastTargetScale;
            }

            if (probe.MaxLength.HasValue)
            {
                column.MaxLength ??= probe.MaxLength;
            }

            if (probe.IsNullable.HasValue)
            {
                column.IsNullable ??= probe.IsNullable;
            }

            if (!string.IsNullOrWhiteSpace(probe.UserTypeSchemaName))
            {
                column.UserTypeSchemaName ??= probe.UserTypeSchemaName;
            }

            if (!string.IsNullOrWhiteSpace(probe.UserTypeName))
            {
                column.UserTypeName ??= probe.UserTypeName;
            }

            if (column.Reference == null && probe.Reference != null)
            {
                column.Reference = CloneReference(probe.Reference);
            }

            ApplyUserTypeResolution(column);
        }

        private static bool IsIIfCall(FunctionCall? functionCall)
        {
            if (functionCall == null)
            {
                return false;
            }

            return string.Equals(functionCall.GetType().Name, "IIfCall", StringComparison.Ordinal);
        }

        private static bool IsPassThroughFunction(FunctionCall functionCall)
        {
            var name = functionCall?.FunctionName?.Value;
            if (string.IsNullOrWhiteSpace(name))
            {
                return false;
            }

            return name.Equals("ISNULL", StringComparison.OrdinalIgnoreCase) ||
                   name.Equals("COALESCE", StringComparison.OrdinalIgnoreCase) ||
                   name.Equals("NULLIF", StringComparison.OrdinalIgnoreCase) ||
                   BuiltInFunctionCatalog.IsMetadataPropagationOnly(name);
        }

        private static bool IsBuiltInFunction(FunctionCall? functionCall)
        {
            if (functionCall == null)
            {
                return false;
            }

            if (IsIIfCall(functionCall))
            {
                return true;
            }

            var name = functionCall.FunctionName?.Value;
            if (string.IsNullOrWhiteSpace(name))
            {
                return false;
            }

            return BuiltInFunctionCatalog.Contains(name);
        }

        private Dictionary<string, TableAliasInfo> BuildAliasScope(FromClause fromClause)
        {
            var scope = new Dictionary<string, TableAliasInfo>(StringComparer.OrdinalIgnoreCase);
            if (fromClause == null)
            {
                return scope;
            }

            foreach (var reference in fromClause.TableReferences ?? new List<TableReference>())
            {
                CollectTableAliases(reference, scope, forceNullableColumns: false);
            }

            return scope;
        }

        private Dictionary<string, TableAliasInfo> BuildMergeOutputScope(MergeSpecification merge)
        {
            var scope = new Dictionary<string, TableAliasInfo>(StringComparer.OrdinalIgnoreCase);

            var targetAlias = CreateMergeTargetAliasInfo(merge?.Target);
            if (targetAlias.HasValue)
            {
                var info = targetAlias.Value.Info;
                var aliasName = targetAlias.Value.Alias;

                if (!string.IsNullOrWhiteSpace(info.Name))
                {
                    RegisterAlias(scope, info.Name, info);
                }

                if (!string.IsNullOrWhiteSpace(aliasName))
                {
                    RegisterAlias(scope, aliasName!, info);
                }

                if (info.Columns != null && info.Columns.Count > 0)
                {
                    RegisterAlias(scope, "inserted", info);
                    RegisterAlias(scope, "deleted", info);
                }
            }

            if (merge?.TableReference != null)
            {
                CollectTableAliases(merge.TableReference, scope, forceNullableColumns: false);
            }

            return scope;
        }

        private (TableAliasInfo Info, string? Alias)? CreateMergeTargetAliasInfo(TableReference? target)
        {
            switch (target)
            {
                case NamedTableReference named when named.SchemaObject != null:
                    var (catalog, schema, name) = ExtractCatalogSchemaAndName(named.SchemaObject);
                    if (string.IsNullOrWhiteSpace(name))
                    {
                        return null;
                    }

                    var columns = ResolveTableColumns(schema, name, catalog);
                    var info = new TableAliasInfo
                    {
                        Catalog = catalog,
                        Schema = schema,
                        Name = name,
                        Columns = CloneAliasColumns(columns, forceNullableColumns: false),
                        IsCte = false,
                        ForceNullableColumns = false
                    };

                    return (info, named.Alias?.Value);
                case VariableTableReference variable:
                    var variableName = NormalizeVariableName(variable.Variable?.Name);
                    if (!string.IsNullOrWhiteSpace(variableName) && _tableTypeBindings.TryGetValue(variableName, out var binding) && binding != null)
                    {
                        var bindingInfo = new TableAliasInfo
                        {
                            Catalog = binding.Catalog ?? _defaultCatalog,
                            Schema = binding.Schema,
                            Name = binding.Name,
                            Columns = CloneAliasColumns(binding.Columns, forceNullableColumns: false),
                            IsCte = false,
                            ForceNullableColumns = false
                        };

                        var aliasName = variable.Alias?.Value;
                        return (bindingInfo, aliasName);
                    }

                    break;
            }

            return null;
        }

        private ProcedureResultSet BuildMergeOutputResultSet(IReadOnlyList<SelectScalarExpression> selectColumns, Dictionary<string, TableAliasInfo> scope)
        {
            var resultSet = new ProcedureResultSet();
            var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            var jsonBindings = new Dictionary<string, JsonPathBinding>(StringComparer.OrdinalIgnoreCase);
            var aliasHints = new Dictionary<string, TableAliasInfo>(StringComparer.OrdinalIgnoreCase);

            foreach (var scalar in selectColumns)
            {
                if (scalar == null)
                {
                    continue;
                }

                var column = BuildColumnFromScalar(scalar);
                if (column == null)
                {
                    continue;
                }

                NormalizeColumnSource(column, scope);
                RegisterAliasHint(column, scope, aliasHints);
                ApplyAliasColumnFallback(column, scope, aliasHints);
                ApplyJsonPathMetadata(column, jsonBindings);
                RegisterJsonPathBinding(column, jsonBindings, scope);
                ApplyMergePseudoTableMetadata(column, scope);
                ApplyMergeActionMetadata(column, scalar.Expression);
                EnsureUniqueColumnName(column, usedNames);
                resultSet.Columns.Add(column);
            }

            ApplyNullLiteralFallback(resultSet);
            return resultSet;
        }

        private static void ApplyMergePseudoTableMetadata(ProcedureResultColumn column, Dictionary<string, TableAliasInfo> scope)
        {
            if (column == null || scope == null || scope.Count == 0)
            {
                return;
            }

            if (string.IsNullOrWhiteSpace(column.SourceAlias) || string.IsNullOrWhiteSpace(column.SourceColumn))
            {
                return;
            }

            var aliasKey = column.SourceAlias;
            if (!string.Equals(aliasKey, "inserted", StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(aliasKey, "deleted", StringComparison.OrdinalIgnoreCase))
            {
                return;
            }

            if (!scope.TryGetValue(aliasKey, out var aliasInfo) || aliasInfo?.Columns == null)
            {
                return;
            }

            if (!aliasInfo.Columns.TryGetValue(column.SourceColumn, out var sourceInfo) || sourceInfo == null)
            {
                return;
            }

            if (!string.IsNullOrWhiteSpace(sourceInfo.Schema))
            {
                column.SourceSchema ??= sourceInfo.Schema;
            }
            else if (!string.IsNullOrWhiteSpace(aliasInfo.Schema))
            {
                column.SourceSchema ??= aliasInfo.Schema;
            }

            if (!string.IsNullOrWhiteSpace(sourceInfo.Catalog))
            {
                column.SourceCatalog ??= sourceInfo.Catalog;
            }
            else if (!string.IsNullOrWhiteSpace(aliasInfo.Catalog))
            {
                column.SourceCatalog ??= aliasInfo.Catalog;
            }

            if (!string.IsNullOrWhiteSpace(sourceInfo.Table))
            {
                column.SourceTable ??= sourceInfo.Table;
            }
            else if (!string.IsNullOrWhiteSpace(aliasInfo.Name))
            {
                column.SourceTable ??= aliasInfo.Name;
            }

            if (!string.IsNullOrWhiteSpace(sourceInfo.Column))
            {
                column.SourceColumn = sourceInfo.Column;
            }

            if (column.Reference == null && sourceInfo.Reference != null)
            {
                column.Reference = CloneReference(sourceInfo.Reference);
            }

            if (!string.IsNullOrWhiteSpace(sourceInfo.SqlTypeName))
            {
                column.SqlTypeName ??= sourceInfo.SqlTypeName;
            }

            if (!string.IsNullOrWhiteSpace(sourceInfo.CastTargetType))
            {
                column.CastTargetType ??= sourceInfo.CastTargetType;
            }

            if (sourceInfo.CastTargetLength.HasValue)
            {
                column.CastTargetLength ??= sourceInfo.CastTargetLength;
            }

            if (sourceInfo.CastTargetPrecision.HasValue)
            {
                column.CastTargetPrecision ??= sourceInfo.CastTargetPrecision;
            }

            if (sourceInfo.CastTargetScale.HasValue)
            {
                column.CastTargetScale ??= sourceInfo.CastTargetScale;
            }

            if (sourceInfo.MaxLength.HasValue)
            {
                column.MaxLength ??= sourceInfo.MaxLength;
            }

            if (sourceInfo.IsNullable.HasValue)
            {
                column.IsNullable ??= sourceInfo.IsNullable;
            }

            if (!string.IsNullOrWhiteSpace(sourceInfo.UserTypeSchema))
            {
                column.UserTypeSchemaName ??= sourceInfo.UserTypeSchema;
            }

            if (!string.IsNullOrWhiteSpace(sourceInfo.UserTypeName))
            {
                column.UserTypeName ??= sourceInfo.UserTypeName;
            }
        }

        private static void ApplyMergeActionMetadata(ProcedureResultColumn column, ScalarExpression? sourceExpression)
        {
            if (column == null)
            {
                return;
            }

            var raw = column.RawExpression?.Trim();
            var source = column.SourceColumn?.Trim();

            var containsToken = false;

            if (!string.IsNullOrWhiteSpace(raw) && string.Equals(raw, "$action", StringComparison.OrdinalIgnoreCase))
            {
                containsToken = true;
            }
            else if (!string.IsNullOrWhiteSpace(source) && string.Equals(source, "$action", StringComparison.OrdinalIgnoreCase))
            {
                containsToken = true;
            }
            else if (!string.IsNullOrWhiteSpace(raw) && raw.IndexOf("$action", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                containsToken = true;
            }
            else if (sourceExpression?.ScriptTokenStream != null)
            {
                var tokens = sourceExpression.ScriptTokenStream;
                var start = Math.Max(sourceExpression.FirstTokenIndex, 0);
                var end = Math.Min(sourceExpression.LastTokenIndex, tokens.Count - 1);

                for (var index = start; index <= end; index++)
                {
                    var tokenText = tokens[index]?.Text;
                    if (!string.IsNullOrWhiteSpace(tokenText) && tokenText.IndexOf("$action", StringComparison.OrdinalIgnoreCase) >= 0)
                    {
                        containsToken = true;
                        break;
                    }
                }
            }

            if (!containsToken)
            {
                return;
            }

            if (string.IsNullOrWhiteSpace(column.SourceColumn))
            {
                column.SourceColumn = "$action";
            }

            column.SqlTypeName ??= "nvarchar";
            column.MaxLength ??= 10;
            column.IsNullable ??= false;
        }

        private void CollectTableAliases(TableReference? reference, Dictionary<string, TableAliasInfo> scope, bool forceNullableColumns)
        {
            if (reference == null)
            {
                return;
            }

            if (IsAliasDebugEnabled())
            {
                Console.WriteLine($"[table-ref] type={reference.GetType().FullName}");
            }

            if (TryHandleApplyReference(reference, scope, forceNullableColumns))
            {
                return;
            }

            if (IsAliasDebugEnabled())
            {
                var aliasLabel = reference is TableReferenceWithAlias withAlias && withAlias.Alias != null
                    ? withAlias.Alias.Value
                    : "<null>";
                Console.WriteLine($"[alias-reference] type={reference.GetType().Name} alias={aliasLabel}");
            }

            switch (reference)
            {
                case NamedTableReference named:
                    var (catalog, schema, name) = ExtractCatalogSchemaAndName(named.SchemaObject);
                    if (!string.IsNullOrWhiteSpace(name))
                    {
                        var cteColumns = ResolveCteColumns(name);
                        var info = new TableAliasInfo
                        {
                            Catalog = cteColumns != null ? null : catalog,
                            Schema = cteColumns != null ? null : schema,
                            Name = name,
                            Columns = CloneAliasColumns(cteColumns, forceNullableColumns),
                            IsCte = cteColumns != null,
                            ForceNullableColumns = forceNullableColumns
                        };

                        if (cteColumns == null)
                        {
                            var resolvedColumns = ResolveTableColumns(info.Schema, info.Name, info.Catalog);
                            if (resolvedColumns != null && resolvedColumns.Count > 0)
                            {
                                info.Columns = CloneAliasColumns(resolvedColumns, forceNullableColumns);
                            }
                        }

                        if (IsAliasDebugEnabled())
                        {
                            Console.WriteLine($"[alias-register] name={name} schema={schema} catalog={catalog} alias={named.Alias?.Value}");
                        }

                        RegisterAlias(scope, name, info);

                        if (!string.IsNullOrWhiteSpace(named.Alias?.Value))
                        {
                            RegisterAlias(scope, named.Alias.Value, info);
                        }
                    }
                    break;
                case SchemaObjectFunctionTableReference functionRef:
                    var (fnCatalog, fnSchema, fnName) = ExtractCatalogSchemaAndName(functionRef.SchemaObject);
                    if (!string.IsNullOrWhiteSpace(fnName))
                    {
                        var info = new TableAliasInfo
                        {
                            Catalog = fnCatalog,
                            Schema = fnSchema ?? _defaultSchema,
                            Name = fnName,
                            ForceNullableColumns = forceNullableColumns,
                            IsFunction = true
                        };

                        var resolvedColumns = ResolveFunctionColumns(info.Schema, info.Name);
                        if (resolvedColumns != null && resolvedColumns.Count > 0)
                        {
                            info.Columns = CloneAliasColumns(resolvedColumns, forceNullableColumns);
                        }

                        RegisterAlias(scope, fnName, info);

                        if (!string.IsNullOrWhiteSpace(functionRef.Alias?.Value))
                        {
                            RegisterAlias(scope, functionRef.Alias.Value, info);
                        }
                    }
                    break;
                case QualifiedJoin join:
                    var leftNullable = forceNullableColumns || join.QualifiedJoinType is QualifiedJoinType.RightOuter or QualifiedJoinType.FullOuter;
                    var rightNullable = forceNullableColumns || join.QualifiedJoinType is QualifiedJoinType.LeftOuter or QualifiedJoinType.FullOuter;
                    CollectTableAliases(join.FirstTableReference, scope, leftNullable);
                    CollectTableAliases(join.SecondTableReference, scope, rightNullable);
                    break;
                case UnqualifiedJoin unqualified:
                    CollectTableAliases(unqualified.FirstTableReference, scope, forceNullableColumns);
                    CollectTableAliases(unqualified.SecondTableReference, scope, forceNullableColumns);
                    break;
                case VariableTableReference variable:
                    var variableKey = NormalizeVariableName(variable?.Variable?.Name);
                    if (!string.IsNullOrWhiteSpace(variableKey) && _tableTypeBindings.TryGetValue(variableKey, out var binding))
                    {
                        var info = new TableAliasInfo
                        {
                            Catalog = binding.Catalog ?? _defaultCatalog,
                            Schema = binding.Schema,
                            Name = binding.Name,
                            Columns = CloneAliasColumns(binding.Columns, forceNullableColumns),
                            IsCte = false,
                            ForceNullableColumns = forceNullableColumns
                        };

                        var aliasName = variable?.Alias?.Value;
                        if (!string.IsNullOrWhiteSpace(aliasName))
                        {
                            RegisterAlias(scope, aliasName!, info);
                        }

                        RegisterAlias(scope, variableKey, info);
                    }
                    else if (!string.IsNullOrWhiteSpace(variableKey))
                    {
                        var fallback = new TableAliasInfo
                        {
                            Catalog = _defaultCatalog,
                            Schema = null,
                            Name = variableKey,
                            ForceNullableColumns = forceNullableColumns
                        };

                        var aliasName = variable?.Alias?.Value;
                        if (!string.IsNullOrWhiteSpace(aliasName))
                        {
                            RegisterAlias(scope, aliasName!, fallback);
                        }

                        RegisterAlias(scope, variableKey, fallback);
                    }
                    break;
                case JoinParenthesisTableReference parenthesis:
                    CollectTableAliases(parenthesis.Join, scope, forceNullableColumns);
                    break;
                case QueryDerivedTable derived when !string.IsNullOrWhiteSpace(derived.Alias?.Value):
                    var columns = derived.QueryExpression != null
                        ? BuildColumnMap(CollectColumnSources(derived.QueryExpression), derived.Columns?.Select(identifier => identifier?.Value).ToArray())
                        : null;

                    var derivedInfo = new TableAliasInfo
                    {
                        Schema = null,
                        Name = derived.Alias.Value,
                        Columns = CloneAliasColumns(columns, forceNullableColumns),
                        ForceNullableColumns = forceNullableColumns
                    };

                    RegisterAlias(scope, derived.Alias.Value, derivedInfo);
                    break;
            }
        }

        private bool TryHandleApplyReference(TableReference reference, Dictionary<string, TableAliasInfo> scope, bool forceNullableColumns)
        {
            var typeName = reference.GetType().Name;
            if (!string.Equals(typeName, "CrossApplyTableReference", StringComparison.Ordinal) &&
                !string.Equals(typeName, "OuterApplyTableReference", StringComparison.Ordinal))
            {
                return false;
            }

            var left = reference.GetType().GetProperty("LeftTableReference")?.GetValue(reference) as TableReference;
            var right = reference.GetType().GetProperty("RightTableReference")?.GetValue(reference) as TableReference;
            var isOuterApply = string.Equals(typeName, "OuterApplyTableReference", StringComparison.Ordinal);

            if (left != null)
            {
                CollectTableAliases(left, scope, forceNullableColumns);
            }

            if (right != null)
            {
                var rightNullable = forceNullableColumns || isOuterApply;
                CollectTableAliases(right, scope, rightNullable);
            }

            return true;
        }

        private static void RegisterAlias(Dictionary<string, TableAliasInfo> scope, string key, TableAliasInfo info)
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                return;
            }

            scope[key] = info;

            if (IsAliasDebugEnabled())
            {
                var catalogLabel = info.Catalog ?? "<null>";
                var schemaLabel = info.Schema ?? "<null>";
                Console.WriteLine($"[alias-register-key] key={key} catalog={catalogLabel} schema={schemaLabel} name={info.Name} isCte={info.IsCte}");
            }

            if (!string.IsNullOrWhiteSpace(info.Name))
            {
                if (!string.IsNullOrWhiteSpace(info.Schema))
                {
                    var schemaQualified = string.Concat(info.Schema, ".", info.Name);
                    scope[schemaQualified] = info;

                    if (IsAliasDebugEnabled())
                    {
                        Console.WriteLine($"[alias-register-key] key={schemaQualified} catalog={info.Catalog ?? "<null>"} schema={info.Schema} name={info.Name} isCte={info.IsCte}");
                    }
                }

                if (!string.IsNullOrWhiteSpace(info.Catalog))
                {
                    var catalogQualified = string.Concat(info.Catalog, ".", info.Name);
                    scope[catalogQualified] = info;

                    if (IsAliasDebugEnabled())
                    {
                        Console.WriteLine($"[alias-register-key] key={catalogQualified} catalog={info.Catalog} schema={info.Schema ?? "<null>"} name={info.Name} isCte={info.IsCte}");
                    }

                    if (!string.IsNullOrWhiteSpace(info.Schema))
                    {
                        var fullyQualified = string.Concat(info.Catalog, ".", info.Schema, ".", info.Name);
                        scope[fullyQualified] = info;

                        if (IsAliasDebugEnabled())
                        {
                            Console.WriteLine($"[alias-register-key] key={fullyQualified} catalog={info.Catalog} schema={info.Schema} name={info.Name} isCte={info.IsCte}");
                        }
                    }
                }
            }
        }

        private TableAliasInfo? ResolveTableAlias(string? identifier)
        {
            if (string.IsNullOrWhiteSpace(identifier))
            {
                return null;
            }

            foreach (var scope in _aliasScopes)
            {
                if (IsAliasDebugEnabled())
                {
                    Console.WriteLine($"[alias-resolve] identifier={identifier} scope={string.Join(",", scope.Keys)}");
                }

                if (scope.TryGetValue(identifier, out var entry))
                {
                    if (IsAliasDebugEnabled())
                    {
                        var schemaLabel = entry?.Schema ?? "<null>";
                        var nameLabel = entry?.Name ?? "<null>";
                        Console.WriteLine($"[alias-resolve-hit] identifier={identifier} schema={schemaLabel} name={nameLabel} isCte={entry?.IsCte}");
                    }

                    if (entry != null && (!entry.IsCte || entry.Columns == null))
                    {
                        if (_cteColumnCache.TryGetValue(entry.Name, out var cteColumns))
                        {
                            entry.Columns = cteColumns;
                            entry.Schema = null;
                            entry.IsCte = true;
                        }
                    }

                    return entry;
                }
            }

            if (IsAliasDebugEnabled())
            {
                Console.WriteLine($"[alias-resolve-miss] identifier={identifier}");
            }

            return null;
        }

        private (string? Catalog, string? Schema, string Name)? ResolveImplicitTableForUnqualifiedColumn()
        {
            foreach (var scope in _aliasScopes)
            {
                if (scope == null || scope.Count == 0)
                {
                    continue;
                }

                (string? Catalog, string? Schema, string Name)? candidate = null;
                var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (var entry in scope.Values)
                {
                    if (string.IsNullOrWhiteSpace(entry.Name))
                    {
                        continue;
                    }

                    var key = string.Concat(entry.Catalog ?? string.Empty, "|", entry.Schema ?? string.Empty, "|", entry.Name);
                    if (!seen.Add(key))
                    {
                        continue;
                    }

                    if (candidate.HasValue)
                    {
                        candidate = null;
                        break;
                    }

                    candidate = (entry.Catalog, entry.Schema, entry.Name);
                }

                if (candidate.HasValue)
                {
                    return candidate;
                }

                break;
            }

            return null;
        }

        private Dictionary<string, ColumnSourceInfo>? ResolveCteColumns(string? name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            if (_cteColumnCache.TryGetValue(name, out var columns))
            {
                return columns;
            }

            return null;
        }

        private void ProcessCommonTableExpressions(WithCtesAndXmlNamespaces? node)
        {
            if (node?.CommonTableExpressions == null)
            {
                return;
            }

            foreach (var cte in node.CommonTableExpressions)
            {
                var cteName = cte?.ExpressionName?.Value;
                if (string.IsNullOrWhiteSpace(cteName) || cte?.QueryExpression == null)
                {
                    continue;
                }

                RegisterCteQuerySpecifications(cte.QueryExpression);

                if (_cteColumnCache.ContainsKey(cteName))
                {
                    continue;
                }

                try
                {
                    var columnSources = CollectColumnSources(cte.QueryExpression);
                    IReadOnlyList<string?>? overrideNames = null;
                    if (cte.Columns != null && cte.Columns.Count > 0)
                    {
                        overrideNames = cte.Columns.Select(identifier => identifier?.Value).ToArray();
                    }

                    var mapped = BuildColumnMap(columnSources, overrideNames);
                    _cteColumnCache[cteName] = mapped;
                }
                catch
                {
                    // Ignore analysis failures – fallback to default resolution.
                }
            }
        }

        private void RegisterCteQuerySpecifications(QueryExpression expression)
        {
            if (expression == null)
            {
                return;
            }

            var collector = new CteQuerySpecificationCollector(_cteQuerySpecifications);
            expression.Accept(collector);
        }

        private sealed class CteQuerySpecificationCollector : TSqlFragmentVisitor
        {
            private readonly HashSet<QuerySpecification> _targets;

            public CteQuerySpecificationCollector(HashSet<QuerySpecification> targets)
            {
                _targets = targets;
            }

            public override void ExplicitVisit(QuerySpecification node)
            {
                if (node != null)
                {
                    _targets.Add(node);
                }

                base.ExplicitVisit(node);
            }
        }

        private List<(string Name, ColumnSourceInfo Info)> CollectColumnSources(QueryExpression queryExpression)
        {
            switch (queryExpression)
            {
                case QuerySpecification specification:
                    return CollectColumnSources(specification);
                case BinaryQueryExpression binary:
                    return CollectBinaryColumnSources(binary);
                case QueryParenthesisExpression parenthesis when parenthesis.QueryExpression != null:
                    return CollectColumnSources(parenthesis.QueryExpression);
                default:
                    return new List<(string Name, ColumnSourceInfo Info)>();
            }
        }

        private List<(string Name, ColumnSourceInfo Info)> CollectColumnSources(QuerySpecification specification)
        {
            var results = new List<(string Name, ColumnSourceInfo Info)>();

            Dictionary<string, TableAliasInfo>? scope = null;
            if (specification.FromClause != null)
            {
                scope = BuildAliasScope(specification.FromClause);
                if (scope.Count > 0)
                {
                    _aliasScopes.Push(scope);
                    if (IsAliasDebugEnabled())
                    {
                        Console.WriteLine($"[alias-scope] keys={string.Join(",", scope.Keys)} contains-cc={scope.ContainsKey("cc")}");
                    }
                }
            }

            try
            {
                foreach (var element in specification.SelectElements)
                {
                    if (element is SelectStarExpression star)
                    {
                        var expanded = ExpandSelectStarColumns(star, scope);
                        if (expanded.Count == 0)
                        {
                            results.Clear();
                            return results;
                        }

                        foreach (var expandedColumn in expanded)
                        {
                            NormalizeColumnSource(expandedColumn, scope);
                            var expandedName = DetermineColumnSourceName(expandedColumn);
                            if (string.IsNullOrWhiteSpace(expandedName))
                            {
                                continue;
                            }

                            var expandedInfo = CreateColumnSourceInfo(expandedColumn);
                            results.Add((expandedName!, expandedInfo));
                        }

                        continue;
                    }

                    if (element is not SelectScalarExpression scalar)
                    {
                        continue;
                    }

                    var column = BuildColumnFromScalar(scalar);
                    if (column == null)
                    {
                        continue;
                    }

                    NormalizeColumnSource(column, scope);

                    var name = DetermineColumnSourceName(column);
                    if (string.IsNullOrWhiteSpace(name))
                    {
                        continue;
                    }

                    var info = CreateColumnSourceInfo(column);
                    results.Add((name!, info));
                }
            }
            finally
            {
                if (scope is { Count: > 0 })
                {
                    _aliasScopes.Pop();
                }
            }

            return results;
        }

        private static string? DetermineColumnSourceName(ProcedureResultColumn column)
        {
            if (!string.IsNullOrWhiteSpace(column.Name))
            {
                return column.Name;
            }

            if (!string.IsNullOrWhiteSpace(column.SourceColumn))
            {
                return column.SourceColumn;
            }

            return null;
        }

        private List<(string Name, ColumnSourceInfo Info)> CollectBinaryColumnSources(BinaryQueryExpression binary)
        {
            var left = binary.FirstQueryExpression != null
                ? CollectColumnSources(binary.FirstQueryExpression)
                : new List<(string Name, ColumnSourceInfo Info)>();

            var right = binary.SecondQueryExpression != null
                ? CollectColumnSources(binary.SecondQueryExpression)
                : new List<(string Name, ColumnSourceInfo Info)>();

            LogVerbose($"[union-columns] left={left.Count} right={right.Count} node={binary.GetType().Name}");

            if (left.Count == 0 && right.Count == 0)
            {
                return new List<(string Name, ColumnSourceInfo Info)>();
            }

            if (left.Count == 0)
            {
                return right;
            }

            if (right.Count == 0)
            {
                return left;
            }

            var merged = new List<(string Name, ColumnSourceInfo Info)>();
            var count = Math.Max(left.Count, right.Count);

            for (var index = 0; index < count; index++)
            {
                var leftEntry = index < left.Count ? left[index] : default;
                var rightEntry = index < right.Count ? right[index] : default;

                var name = !string.IsNullOrWhiteSpace(leftEntry.Name)
                    ? leftEntry.Name
                    : rightEntry.Name;

                ColumnSourceInfo? info = null;

                if (leftEntry.Info != null && rightEntry.Info != null)
                {
                    info = MergeColumnSourceInfo(leftEntry.Info, rightEntry.Info);
                }
                else if (leftEntry.Info != null)
                {
                    info = CloneColumnSourceInfo(leftEntry.Info);
                }
                else if (rightEntry.Info != null)
                {
                    info = CloneColumnSourceInfo(rightEntry.Info);
                }

                if (info == null)
                {
                    continue;
                }

                if (string.IsNullOrWhiteSpace(name))
                {
                    name = info.Column ?? string.Empty;
                }

                if (string.IsNullOrWhiteSpace(name))
                {
                    name = string.Concat("Column", index + 1);
                }

                merged.Add((name, info));
            }

            return merged;
        }

        private static ColumnSourceInfo CloneColumnSourceInfo(ColumnSourceInfo source, bool? overrideNullable = null)
        {
            return new ColumnSourceInfo
            {
                Catalog = source.Catalog,
                Schema = source.Schema,
                Table = source.Table,
                Column = source.Column,
                Reference = source.Reference == null ? null : CloneReference(source.Reference),
                ReturnsJson = source.ReturnsJson,
                ReturnsJsonArray = source.ReturnsJsonArray,
                ReturnsUnknownJson = source.ReturnsUnknownJson,
                IsNestedJson = source.IsNestedJson,
                SqlTypeName = source.SqlTypeName,
                MaxLength = source.MaxLength,
                Precision = source.Precision,
                Scale = source.Scale,
                IsNullable = overrideNullable ?? source.IsNullable,
                UserTypeSchema = source.UserTypeSchema,
                UserTypeName = source.UserTypeName,
                CastTargetType = source.CastTargetType,
                CastTargetLength = source.CastTargetLength,
                CastTargetPrecision = source.CastTargetPrecision,
                CastTargetScale = source.CastTargetScale
            };
        }

        private static Dictionary<string, ColumnSourceInfo>? CloneAliasColumns(Dictionary<string, ColumnSourceInfo>? source, bool forceNullableColumns)
        {
            if (source == null || source.Count == 0)
            {
                return source;
            }

            if (!forceNullableColumns)
            {
                return source;
            }

            var clone = new Dictionary<string, ColumnSourceInfo>(source.Count, StringComparer.OrdinalIgnoreCase);
            foreach (var entry in source)
            {
                var info = entry.Value;
                if (info == null)
                {
                    continue;
                }

                clone[entry.Key] = CloneColumnSourceInfo(info, true);
            }

            return clone;
        }

        private static ColumnSourceInfo MergeColumnSourceInfo(ColumnSourceInfo primary, ColumnSourceInfo secondary)
        {
            var reference = primary.Reference ?? secondary.Reference;

            return new ColumnSourceInfo
            {
                Catalog = primary.Catalog ?? secondary.Catalog,
                Schema = primary.Schema ?? secondary.Schema,
                Table = primary.Table ?? secondary.Table,
                Column = primary.Column ?? secondary.Column,
                Reference = reference == null ? null : CloneReference(reference),
                ReturnsJson = primary.ReturnsJson ?? secondary.ReturnsJson,
                ReturnsJsonArray = primary.ReturnsJsonArray ?? secondary.ReturnsJsonArray,
                ReturnsUnknownJson = primary.ReturnsUnknownJson ?? secondary.ReturnsUnknownJson,
                IsNestedJson = primary.IsNestedJson ?? secondary.IsNestedJson,
                SqlTypeName = primary.SqlTypeName ?? secondary.SqlTypeName,
                MaxLength = primary.MaxLength ?? secondary.MaxLength,
                Precision = primary.Precision ?? secondary.Precision,
                Scale = primary.Scale ?? secondary.Scale,
                IsNullable = primary.IsNullable ?? secondary.IsNullable,
                UserTypeSchema = primary.UserTypeSchema ?? secondary.UserTypeSchema,
                UserTypeName = primary.UserTypeName ?? secondary.UserTypeName,
                CastTargetType = primary.CastTargetType ?? secondary.CastTargetType,
                CastTargetLength = primary.CastTargetLength ?? secondary.CastTargetLength,
                CastTargetPrecision = primary.CastTargetPrecision ?? secondary.CastTargetPrecision,
                CastTargetScale = primary.CastTargetScale ?? secondary.CastTargetScale
            };
        }

        private List<ProcedureResultColumn> ExpandSelectStarColumns(SelectStarExpression star, Dictionary<string, TableAliasInfo>? scope)
        {
            var expanded = new List<ProcedureResultColumn>();

            if (star == null)
            {
                return expanded;
            }

            var identifiers = star.Qualifier?.Identifiers;
            if (identifiers == null || identifiers.Count == 0)
            {
                if (scope == null || scope.Count == 0)
                {
                    return expanded;
                }

                var processed = new HashSet<TableAliasInfo>();
                foreach (var alias in scope.Values)
                {
                    if (alias?.Columns == null || alias.Columns.Count == 0)
                    {
                        continue;
                    }

                    if (!processed.Add(alias))
                    {
                        continue;
                    }

                    AppendExpandedColumns(expanded, alias);
                }

                return expanded;
            }

            var aliasName = identifiers[^1].Value;
            if (string.IsNullOrWhiteSpace(aliasName))
            {
                return expanded;
            }

            var aliasInfo = TryResolveAlias(aliasName, scope);
            if (aliasInfo?.Columns == null || aliasInfo.Columns.Count == 0)
            {
                return expanded;
            }

            AppendExpandedColumns(expanded, aliasInfo);
            return expanded;
        }

        private static void AppendExpandedColumns(List<ProcedureResultColumn> accumulator, TableAliasInfo aliasInfo)
        {
            if (accumulator == null || aliasInfo?.Columns == null || aliasInfo.Columns.Count == 0)
            {
                return;
            }

            foreach (var entry in aliasInfo.Columns)
            {
                var columnInfo = entry.Value;
                if (columnInfo == null)
                {
                    continue;
                }

                var column = new ProcedureResultColumn
                {
                    Name = entry.Key,
                    SourceCatalog = columnInfo.Catalog ?? aliasInfo.Catalog,
                    SourceSchema = columnInfo.Schema,
                    SourceTable = columnInfo.Table,
                    SourceColumn = columnInfo.Column ?? entry.Key,
                    SqlTypeName = columnInfo.SqlTypeName,
                    CastTargetType = columnInfo.CastTargetType,
                    CastTargetLength = columnInfo.CastTargetLength,
                    CastTargetPrecision = columnInfo.CastTargetPrecision,
                    CastTargetScale = columnInfo.CastTargetScale,
                    MaxLength = columnInfo.MaxLength,
                    IsNullable = columnInfo.IsNullable,
                    UserTypeSchemaName = columnInfo.UserTypeSchema,
                    UserTypeName = columnInfo.UserTypeName,
                    ReturnsJson = columnInfo.ReturnsJson,
                    ReturnsJsonArray = columnInfo.ReturnsJsonArray,
                    ReturnsUnknownJson = columnInfo.ReturnsUnknownJson,
                    IsNestedJson = columnInfo.IsNestedJson,
                    Reference = columnInfo.Reference == null ? null : CloneReference(columnInfo.Reference)
                };

                accumulator.Add(column);
            }
        }

        private TableAliasInfo? TryResolveAlias(string aliasName, Dictionary<string, TableAliasInfo>? localScope)
        {
            if (!string.IsNullOrWhiteSpace(aliasName) && localScope != null && localScope.TryGetValue(aliasName, out var alias))
            {
                return alias;
            }

            return ResolveTableAlias(aliasName);
        }

        private void NormalizeColumnSource(ProcedureResultColumn column, Dictionary<string, TableAliasInfo>? scope)
        {
            if (column == null || scope == null || scope.Count == 0)
            {
                return;
            }

            foreach (var candidate in EnumerateAliasCandidates(column))
            {
                if (string.IsNullOrWhiteSpace(candidate))
                {
                    continue;
                }

                if (!scope.TryGetValue(candidate, out var aliasInfo) || aliasInfo == null)
                {
                    continue;
                }

                if (aliasInfo.Columns != null)
                {
                    continue;
                }

                if (IsAliasDebugEnabled())
                {
                    Console.WriteLine($"[alias-normalize] candidate={candidate} -> schema={aliasInfo.Schema} name={aliasInfo.Name}");
                }

                if (!string.IsNullOrWhiteSpace(aliasInfo.Schema))
                {
                    column.SourceSchema = aliasInfo.Schema;
                }

                if (!string.IsNullOrWhiteSpace(aliasInfo.Catalog))
                {
                    column.SourceCatalog = aliasInfo.Catalog;
                }

                if (!string.IsNullOrWhiteSpace(aliasInfo.Name))
                {
                    column.SourceTable = aliasInfo.Name;
                }

                break;
            }

            if (column.SourceColumn is not { } sourceColumn || string.IsNullOrWhiteSpace(sourceColumn))
            {
                return;
            }

            var matches = new List<(TableAliasInfo Alias, ColumnSourceInfo Source)>();
            var seenAliasEntries = new HashSet<TableAliasInfo>();
            var seenSourceKeys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            if (!string.IsNullOrWhiteSpace(column.SourceAlias) && scope.TryGetValue(column.SourceAlias, out var preferredAlias) && preferredAlias?.Columns != null && preferredAlias.Columns.TryGetValue(sourceColumn!, out var preferredSource) && preferredSource != null)
            {
                matches.Add((preferredAlias, preferredSource));
                seenAliasEntries.Add(preferredAlias);
                var preferredKey = string.Concat(
                    preferredSource.Schema ?? string.Empty,
                    "|",
                    preferredSource.Table ?? string.Empty,
                    "|",
                    preferredSource.Column ?? sourceColumn ?? string.Empty);
                seenSourceKeys.Add(preferredKey);
            }

            foreach (var entry in scope.Values)
            {
                if (entry?.Columns == null)
                {
                    continue;
                }

                if (!seenAliasEntries.Add(entry))
                {
                    continue;
                }

                if (entry.Columns.TryGetValue(sourceColumn!, out var sourceInfo) && sourceInfo != null)
                {
                    var sourceKey = string.Concat(
                        sourceInfo.Schema ?? string.Empty,
                        "|",
                        sourceInfo.Table ?? string.Empty,
                        "|",
                        sourceInfo.Column ?? sourceColumn ?? string.Empty);

                    if (!seenSourceKeys.Add(sourceKey))
                    {
                        continue;
                    }

                    matches.Add((entry, sourceInfo));
                }
            }

            if (IsAliasDebugEnabled())
            {
                Console.WriteLine($"[alias-fallback] column={column.Name} matches={matches.Count}");
            }

            if (matches.Count == 0)
            {
                TryApplyTableTypeMetadata(column);
                return;
            }

            if (matches.Count > 1)
            {
                column.IsAmbiguous = true;
                return;
            }

            var (alias, info) = matches[0];

            if (IsAliasDebugEnabled())
            {
                Console.WriteLine($"[alias-fallback-apply] column={column.Name} alias={alias.Name} sourceColumn={info.Column} type={info.SqlTypeName}");
            }

            if (!string.IsNullOrWhiteSpace(info.Schema))
            {
                column.SourceSchema ??= info.Schema;
            }
            else if (!string.IsNullOrWhiteSpace(alias.Schema))
            {
                column.SourceSchema ??= alias.Schema;
            }

            if (!string.IsNullOrWhiteSpace(info.Catalog))
            {
                column.SourceCatalog ??= info.Catalog;
            }
            else if (!string.IsNullOrWhiteSpace(alias.Catalog))
            {
                column.SourceCatalog ??= alias.Catalog;
            }

            if (!string.IsNullOrWhiteSpace(info.Table))
            {
                column.SourceTable ??= info.Table;
            }
            else if (!string.IsNullOrWhiteSpace(alias.Name))
            {
                column.SourceTable ??= alias.Name;
            }

            if (!string.IsNullOrWhiteSpace(info.Column))
            {
                column.SourceColumn ??= info.Column;
            }

            if (column.Reference == null && info.Reference != null)
            {
                column.Reference = CloneReference(info.Reference);
            }

            if (info.ReturnsJson.HasValue)
            {
                column.ReturnsJson ??= info.ReturnsJson;
            }

            if (info.ReturnsJsonArray.HasValue)
            {
                column.ReturnsJsonArray ??= info.ReturnsJsonArray;
            }

            if (info.ReturnsUnknownJson.HasValue)
            {
                column.ReturnsUnknownJson ??= info.ReturnsUnknownJson;
            }

            if (info.IsNestedJson.HasValue)
            {
                column.IsNestedJson ??= info.IsNestedJson;
            }

            ApplyTypeMetadata(column, info);

            if (alias.ForceNullableColumns)
            {
                column.IsNullable = true;
                column.ForcedNullable ??= true;
            }
        }

        private void TryApplyTableTypeMetadata(ProcedureResultColumn column)
        {
            if (column == null)
            {
                return;
            }

            var lookupName = column.SourceColumn;
            if (string.IsNullOrWhiteSpace(lookupName))
            {
                lookupName = column.Name;
            }

            if (string.IsNullOrWhiteSpace(lookupName))
            {
                return;
            }

            foreach (var binding in ResolveCandidateTableTypeBindings(column))
            {
                if (!binding.Columns.TryGetValue(lookupName, out var sourceInfo) || sourceInfo == null)
                {
                    continue;
                }

                column.SourceSchema ??= binding.Schema;
                column.SourceTable ??= binding.Name;

                if (column.Reference == null && binding.Reference != null)
                {
                    column.Reference = CloneReference(binding.Reference);
                }

                ApplyTypeMetadata(column, sourceInfo);
                return;
            }
        }

        private IEnumerable<TableTypeBinding> ResolveCandidateTableTypeBindings(ProcedureResultColumn column)
        {
            var visited = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

            foreach (var (schema, name) in EnumerateTableTypeBindingCandidates(column))
            {
                var binding = ResolveTableTypeBinding(schema, name);
                if (binding == null || binding.Columns == null || binding.Columns.Count == 0)
                {
                    continue;
                }

                var key = BuildTableTypeKey(binding.Schema, binding.Name);
                if (!visited.Add(key))
                {
                    continue;
                }

                yield return binding;
            }
        }

        private static IEnumerable<(string? Schema, string? Name)> EnumerateTableTypeBindingCandidates(ProcedureResultColumn column)
        {
            if (column == null)
            {
                yield break;
            }

            if (!string.IsNullOrWhiteSpace(column.SourceTable))
            {
                yield return (column.SourceSchema, column.SourceTable);
            }

            if (!string.IsNullOrWhiteSpace(column.SourceAlias))
            {
                yield return (column.SourceSchema, column.SourceAlias);
            }

            if (!string.IsNullOrWhiteSpace(column.UserTypeName))
            {
                yield return (column.UserTypeSchemaName, column.UserTypeName);
            }

            if (column.Reference != null && column.Reference.Kind == ProcedureReferenceKind.TableType)
            {
                yield return (column.Reference.Schema, column.Reference.Name);
            }
        }

        private TableTypeBinding? ResolveTableTypeBinding(string? schema, string? name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            if (_tableTypeBindings.TryGetValue(name, out var binding) && binding != null)
            {
                return binding;
            }

            var normalized = NormalizeVariableName(name);
            if (_tableTypeBindings.TryGetValue(normalized, out binding) && binding != null)
            {
                return binding;
            }

            string? effectiveSchema = schema;
            var effectiveName = name;

            var separatorIndex = effectiveName.IndexOf('.');
            if (separatorIndex >= 0)
            {
                var schemaPart = effectiveName[..separatorIndex];
                var namePart = effectiveName[(separatorIndex + 1)..];
                if (!string.IsNullOrWhiteSpace(schemaPart) && !string.IsNullOrWhiteSpace(namePart))
                {
                    effectiveSchema = schemaPart;
                    effectiveName = namePart;
                }
            }

            if (string.IsNullOrWhiteSpace(effectiveSchema))
            {
                effectiveSchema = _defaultSchema;
            }

            binding = CreateTableTypeBinding(effectiveSchema, effectiveName, _defaultCatalog);
            if (binding != null)
            {
                return binding;
            }

            if (!string.Equals(effectiveSchema, "dbo", StringComparison.OrdinalIgnoreCase))
            {
                binding = CreateTableTypeBinding("dbo", effectiveName, _defaultCatalog);
                if (binding != null)
                {
                    return binding;
                }
            }

            return null;
        }

        private static IEnumerable<string?> EnumerateAliasCandidates(ProcedureResultColumn column)
        {
            if (!string.IsNullOrWhiteSpace(column.SourceTable))
            {
                yield return column.SourceTable;
            }

            if (!string.IsNullOrWhiteSpace(column.SourceAlias))
            {
                yield return column.SourceAlias;
            }
        }

        private static Dictionary<string, ColumnSourceInfo> BuildColumnMap(IReadOnlyList<(string Name, ColumnSourceInfo Info)> columns, IReadOnlyList<string?>? overrideNames)
        {
            var map = new Dictionary<string, ColumnSourceInfo>(StringComparer.OrdinalIgnoreCase);
            if (columns == null || columns.Count == 0)
            {
                return map;
            }

            for (var i = 0; i < columns.Count; i++)
            {
                var name = columns[i].Name;
                if (overrideNames != null && i < overrideNames.Count)
                {
                    var overrideName = overrideNames[i];
                    if (!string.IsNullOrWhiteSpace(overrideName))
                    {
                        name = overrideName;
                    }
                }

                if (string.IsNullOrWhiteSpace(name))
                {
                    continue;
                }

                map[name] = columns[i].Info;
            }

            return map;
        }

        private static ColumnSourceInfo CreateColumnSourceInfo(ProcedureResultColumn column)
        {
            return new ColumnSourceInfo
            {
                Catalog = column.SourceCatalog,
                Schema = column.SourceSchema,
                Table = column.SourceTable,
                Column = column.SourceColumn,
                Reference = column.Reference == null
                    ? null
                    : CloneReference(column.Reference),
                ReturnsJson = column.ReturnsJson,
                ReturnsJsonArray = column.ReturnsJsonArray,
                ReturnsUnknownJson = column.ReturnsUnknownJson,
                IsNestedJson = column.IsNestedJson,
                SqlTypeName = column.SqlTypeName,
                MaxLength = column.MaxLength,
                IsNullable = column.IsNullable,
                UserTypeSchema = column.UserTypeSchemaName,
                UserTypeName = column.UserTypeName,
                CastTargetType = column.CastTargetType,
                CastTargetLength = column.CastTargetLength,
                CastTargetPrecision = column.CastTargetPrecision,
                CastTargetScale = column.CastTargetScale
            };
        }
        private static void ApplyTypeMetadata(ProcedureResultColumn column, ColumnSourceInfo sourceInfo)
        {
            if (column == null || sourceInfo == null)
            {
                return;
            }

            if (!string.IsNullOrWhiteSpace(sourceInfo.Catalog))
            {
                column.SourceCatalog ??= sourceInfo.Catalog;
            }

            if (!string.IsNullOrWhiteSpace(sourceInfo.Schema))
            {
                column.SourceSchema ??= sourceInfo.Schema;
            }

            if (!string.IsNullOrWhiteSpace(sourceInfo.Table))
            {
                column.SourceTable ??= sourceInfo.Table;
            }

            if (!string.IsNullOrWhiteSpace(sourceInfo.Column))
            {
                column.SourceColumn ??= sourceInfo.Column;
            }

            if (!string.IsNullOrWhiteSpace(sourceInfo.SqlTypeName))
            {
                column.SqlTypeName ??= sourceInfo.SqlTypeName;
            }

            if (!string.IsNullOrWhiteSpace(sourceInfo.CastTargetType))
            {
                column.CastTargetType ??= sourceInfo.CastTargetType;
            }

            if (sourceInfo.CastTargetLength.HasValue)
            {
                column.CastTargetLength ??= sourceInfo.CastTargetLength;
            }

            if (sourceInfo.CastTargetPrecision.HasValue)
            {
                column.CastTargetPrecision ??= sourceInfo.CastTargetPrecision;
            }

            if (sourceInfo.CastTargetScale.HasValue)
            {
                column.CastTargetScale ??= sourceInfo.CastTargetScale;
            }

            if (sourceInfo.MaxLength.HasValue)
            {
                column.MaxLength ??= sourceInfo.MaxLength;
            }

            if (sourceInfo.IsNullable.HasValue)
            {
                column.IsNullable ??= sourceInfo.IsNullable;
            }

            if (!string.IsNullOrWhiteSpace(sourceInfo.UserTypeSchema))
            {
                column.UserTypeSchemaName ??= sourceInfo.UserTypeSchema;
            }

            if (!string.IsNullOrWhiteSpace(sourceInfo.UserTypeName))
            {
                column.UserTypeName ??= sourceInfo.UserTypeName;
            }

            ApplyUserTypeResolution(column, sourceInfo);
        }

        private static ProcedureReferenceModel CloneReference(ProcedureReferenceModel reference)
        {
            return new ProcedureReferenceModel
            {
                Kind = reference.Kind,
                Schema = reference.Schema,
                Name = reference.Name
            };
        }

        private (string? Catalog, string? Schema, string? Name) ExtractCatalogSchemaAndName(SchemaObjectName? schemaObject)
        {
            if (schemaObject?.Identifiers == null || schemaObject.Identifiers.Count == 0)
            {
                return (null, null, null);
            }

            var identifiers = schemaObject.Identifiers;
            var name = identifiers[^1].Value;
            string? schema = null;
            string? catalog = null;

            if (identifiers.Count >= 2)
            {
                schema = identifiers[^2].Value;
            }

            if (identifiers.Count >= 3)
            {
                catalog = identifiers[^3].Value;
            }

            schema = string.IsNullOrWhiteSpace(schema) ? _defaultSchema : schema;
            catalog = string.IsNullOrWhiteSpace(catalog) ? _defaultCatalog : catalog;
            return (catalog, schema, name);
        }

        private (string? Schema, string? Name) ExtractSchemaAndName(SchemaObjectName? schemaObject)
        {
            var (_, schema, name) = ExtractCatalogSchemaAndName(schemaObject);
            return (schema, name);
        }

        private static IReadOnlyList<TypeMetadataResolver> LoadTypeMetadataResolvers()
        {
            var resolvers = new List<TypeMetadataResolver>();

            foreach (var root in SnapshotRootLocator.EnumerateSnapshotRoots())
            {
                try
                {
                    resolvers.Add(SnapshotTypeResolverCache.Get(root));
                }
                catch
                {
                    // Ignore individual resolver initialization failures and continue with remaining roots.
                }
            }

            if (resolvers.Count == 0)
            {
                resolvers.Add(new TypeMetadataResolver());
            }

            return resolvers;
        }

        private static IReadOnlyDictionary<string, FunctionInfo> LoadFunctionMetadata()
        {
            var map = new Dictionary<string, FunctionInfo>(StringComparer.OrdinalIgnoreCase);

            foreach (var root in SnapshotRootLocator.EnumerateSnapshotRoots())
            {
                try
                {
                    var functionsDir = Path.Combine(root, ".xtraq", "snapshots", "functions");
                    if (!Directory.Exists(functionsDir))
                    {
                        continue;
                    }

                    var files = Directory.GetFiles(functionsDir, "*.json", SearchOption.TopDirectoryOnly);
                    foreach (var file in files)
                    {
                        try
                        {
                            using var stream = File.OpenRead(file);
                            using var document = JsonDocument.Parse(stream);
                            var info = BuildFunctionInfo(document.RootElement);
                            if (info == null)
                            {
                                continue;
                            }

                            var key = BuildTableTypeKey(info.Schema, info.Name);
                            if (!map.ContainsKey(key))
                            {
                                map[key] = info;
                            }
                        }
                        catch
                        {
                            // Ignore malformed function metadata entries and continue loading remaining files.
                        }
                    }
                }
                catch
                {
                    // Ignore metadata load issues for individual roots.
                }
            }

            return map;
        }

        private static FunctionInfo? BuildFunctionInfo(JsonElement root)
        {
            if (root.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            var name = GetPropertyString(root, "Name");
            if (string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            var schema = GetPropertyString(root, "Schema") ?? "dbo";
            var isTableValued = GetPropertyBool(root, "IsTableValued");
            var columns = new List<FunctionColumnInfo>();

            if (root.TryGetProperty("Columns", out var columnsElement) && columnsElement.ValueKind == JsonValueKind.Array)
            {
                foreach (var columnElement in columnsElement.EnumerateArray())
                {
                    if (columnElement.ValueKind != JsonValueKind.Object)
                    {
                        continue;
                    }

                    var columnName = GetPropertyString(columnElement, "Name");
                    if (string.IsNullOrWhiteSpace(columnName))
                    {
                        continue;
                    }

                    columns.Add(new FunctionColumnInfo
                    {
                        Name = columnName!,
                        TypeRef = GetPropertyString(columnElement, "TypeRef"),
                        SqlTypeName = GetPropertyString(columnElement, "SqlTypeName") ?? GetPropertyString(columnElement, "SqlType"),
                        MaxLength = GetPropertyInt(columnElement, "MaxLength"),
                        Precision = GetPropertyInt(columnElement, "Precision"),
                        Scale = GetPropertyInt(columnElement, "Scale"),
                        IsNullable = GetPropertyBool(columnElement, "IsNullable")
                    });
                }
            }

            if (columns.Count == 0 && isTableValued != true)
            {
                return null;
            }

            if (columns.Count == 0)
            {
                return null;
            }

            return new FunctionInfo
            {
                Schema = string.IsNullOrWhiteSpace(schema) ? "dbo" : schema,
                Name = name!,
                Columns = columns
            };
        }

        private static string? GetPropertyString(JsonElement element, string propertyName)
        {
            if (element.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            if (!element.TryGetProperty(propertyName, out var property))
            {
                return null;
            }

            return property.ValueKind == JsonValueKind.String ? property.GetString() : null;
        }

        private static int? GetPropertyInt(JsonElement element, string propertyName)
        {
            if (element.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            if (!element.TryGetProperty(propertyName, out var property))
            {
                return null;
            }

            if (property.ValueKind == JsonValueKind.Number && property.TryGetInt32(out var intValue))
            {
                return intValue;
            }

            if (property.ValueKind == JsonValueKind.String && int.TryParse(property.GetString(), out var parsed))
            {
                return parsed;
            }

            return null;
        }

        private static bool? GetPropertyBool(JsonElement element, string propertyName)
        {
            if (element.ValueKind != JsonValueKind.Object)
            {
                return null;
            }

            if (!element.TryGetProperty(propertyName, out var property))
            {
                return null;
            }

            return property.ValueKind switch
            {
                JsonValueKind.True => true,
                JsonValueKind.False => false,
                JsonValueKind.String when bool.TryParse(property.GetString(), out var parsed) => parsed,
                _ => null
            };
        }

        private static Dictionary<string, TableTypeInfo> LoadTableTypeMetadata()
        {
            var map = new Dictionary<string, TableTypeInfo>(StringComparer.OrdinalIgnoreCase);

            foreach (var root in SnapshotRootLocator.EnumerateSnapshotRoots())
            {
                try
                {
                    var provider = new TableTypeMetadataProvider(root);
                    foreach (var entry in provider.GetAll())
                    {
                        if (entry == null || string.IsNullOrWhiteSpace(entry.Name))
                        {
                            continue;
                        }

                        var key = BuildTableTypeKey(entry.Schema, entry.Name);
                        if (!map.ContainsKey(key))
                        {
                            map[key] = entry;
                        }
                    }
                }
                catch
                {
                    // Ignore metadata load failures per root and continue with remaining candidates.
                }
            }

            return map;
        }

        private static string BuildTableTypeKey(string? schema, string? name)
        {
            var effectiveSchema = string.IsNullOrWhiteSpace(schema) ? "dbo" : schema;
            var effectiveName = name?.Trim() ?? string.Empty;
            return string.Concat(effectiveSchema, ".", effectiveName);
        }

        private Dictionary<string, ColumnSourceInfo>? ResolveTableColumns(string? schema, string name, string? catalog)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            var table = TryLookupTable(schema, name);
            if (table != null && table.Columns != null && table.Columns.Count > 0)
            {
                var map = BuildColumnInfoMap(table);
                if (map.Count > 0)
                {
                    return map;
                }
            }

            if (_schemaMetadataProvider == null)
            {
                return null;
            }

            var effectiveSchema = string.IsNullOrWhiteSpace(schema) ? (_defaultSchema ?? "dbo") : schema;
            var effectiveCatalog = string.IsNullOrWhiteSpace(catalog) ? _defaultCatalog : catalog;
            var cacheKey = BuildTableCacheKey(effectiveCatalog, effectiveSchema, name);

            if (!string.IsNullOrWhiteSpace(cacheKey) && _fallbackTableColumnCache.TryGetValue(cacheKey, out var cached))
            {
                return cached;
            }

            try
            {
                var metadata = _schemaMetadataProvider
                    .GetTableColumnsAsync(effectiveSchema ?? "dbo", name, effectiveCatalog, CancellationToken.None)
                    .GetAwaiter()
                    .GetResult();

                if (metadata != null && metadata.Count > 0)
                {
                    var map = BuildColumnInfoMap(metadata, effectiveSchema ?? "dbo", name, effectiveCatalog);
                    if (!string.IsNullOrWhiteSpace(cacheKey))
                    {
                        _fallbackTableColumnCache[cacheKey] = map;
                    }

                    return map;
                }
            }
            catch (Exception ex)
            {
                LogVerbose($"[alias-resolver] Fallback metadata lookup failed for {effectiveSchema}.{name}: {ex.Message}");
            }

            if (!string.IsNullOrWhiteSpace(cacheKey))
            {
                _fallbackTableColumnCache[cacheKey] = new Dictionary<string, ColumnSourceInfo>(StringComparer.OrdinalIgnoreCase);
            }

            return null;
        }

        private Dictionary<string, ColumnSourceInfo>? ResolveAliasColumns(TableAliasInfo aliasInfo)
        {
            if (aliasInfo == null)
            {
                return null;
            }

            if (aliasInfo.IsFunction)
            {
                return ResolveFunctionColumns(aliasInfo.Schema, aliasInfo.Name);
            }

            var tableColumns = ResolveTableColumns(aliasInfo.Schema, aliasInfo.Name, aliasInfo.Catalog ?? _defaultCatalog);
            if (tableColumns != null && tableColumns.Count > 0)
            {
                return tableColumns;
            }

            return ResolveFunctionColumns(aliasInfo.Schema, aliasInfo.Name);
        }

        private Dictionary<string, ColumnSourceInfo>? ResolveFunctionColumns(string? schema, string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            var key = BuildTableTypeKey(schema, name);
            if (!FunctionMetadataLookup.Value.TryGetValue(key, out var function) || function == null || function.Columns == null || function.Columns.Count == 0)
            {
                return ResolveFunctionColumnsFromProvider(schema, name);
            }

            return BuildFunctionColumnInfoMap(function);
        }

        private Dictionary<string, ColumnSourceInfo>? ResolveFunctionColumnsFromProvider(string? schema, string name)
        {
            if (_schemaMetadataProvider == null)
            {
                return null;
            }

            var effectiveSchema = string.IsNullOrWhiteSpace(schema) ? (_defaultSchema ?? "dbo") : schema;
            var effectiveCatalog = _defaultCatalog;
            var cacheKey = BuildTableCacheKey(effectiveCatalog, effectiveSchema, name);

            if (!string.IsNullOrWhiteSpace(cacheKey) && _fallbackFunctionColumnCache.TryGetValue(cacheKey, out var cached))
            {
                return cached.Count == 0 ? null : cached;
            }

            try
            {
                var columns = _schemaMetadataProvider
                    .GetTableColumnsAsync(effectiveSchema ?? "dbo", name, effectiveCatalog, CancellationToken.None)
                    .GetAwaiter()
                    .GetResult();

                if (columns != null && columns.Count > 0)
                {
                    var map = BuildFunctionColumnInfoMapFromMetadata(columns, effectiveSchema ?? "dbo", name, effectiveCatalog);
                    if (!string.IsNullOrWhiteSpace(cacheKey))
                    {
                        _fallbackFunctionColumnCache[cacheKey!] = map;
                    }

                    return map;
                }
            }
            catch (Exception ex)
            {
                LogVerbose($"[alias-resolver] Fallback function metadata lookup failed for {effectiveSchema}.{name}: {ex.Message}");
            }

            if (!string.IsNullOrWhiteSpace(cacheKey))
            {
                _fallbackFunctionColumnCache[cacheKey!] = new Dictionary<string, ColumnSourceInfo>(StringComparer.OrdinalIgnoreCase);
            }

            return null;
        }

        private static Dictionary<string, ColumnSourceInfo> BuildColumnInfoMap(TableInfo table)
        {
            var map = new Dictionary<string, ColumnSourceInfo>(StringComparer.OrdinalIgnoreCase);
            if (table.Columns == null)
            {
                return map;
            }

            foreach (var column in table.Columns)
            {
                if (column == null || string.IsNullOrWhiteSpace(column.Name))
                {
                    continue;
                }

                var (_, userTypeSchema, userTypeName) = SplitTypeRef(column.TypeRef);

                map[column.Name] = new ColumnSourceInfo
                {
                    Catalog = table.Catalog,
                    Schema = table.Schema,
                    Table = table.Name,
                    Column = column.Name,
                    SqlTypeName = NormalizeResolvedSqlType(column.SqlType, userTypeSchema, userTypeName),
                    MaxLength = column.MaxLength,
                    Precision = column.Precision,
                    Scale = column.Scale,
                    IsNullable = column.IsNullable,
                    UserTypeSchema = userTypeSchema,
                    UserTypeName = userTypeName
                };
            }

            return map;
        }

        private static Dictionary<string, ColumnSourceInfo> BuildColumnInfoMap(IReadOnlyList<ColumnMetadata> columns, string? schema, string name, string? catalog)
        {
            var map = new Dictionary<string, ColumnSourceInfo>(StringComparer.OrdinalIgnoreCase);
            if (columns == null)
            {
                return map;
            }

            foreach (var column in columns)
            {
                if (column == null || string.IsNullOrWhiteSpace(column.Name))
                {
                    continue;
                }

                var sqlType = NormalizeResolvedSqlType(column.SqlTypeName, column.UserTypeSchema, column.UserTypeName);

                map[column.Name] = new ColumnSourceInfo
                {
                    Catalog = string.IsNullOrWhiteSpace(column.Catalog) ? catalog : column.Catalog,
                    Schema = schema,
                    Table = name,
                    Column = column.Name,
                    SqlTypeName = sqlType,
                    MaxLength = column.MaxLength,
                    Precision = column.Precision,
                    Scale = column.Scale,
                    IsNullable = column.IsNullable,
                    UserTypeSchema = column.UserTypeSchema,
                    UserTypeName = column.UserTypeName
                };
            }

            return map;
        }

        private static string BuildTableCacheKey(string? catalog, string? schema, string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return string.Empty;
            }

            var parts = new List<string>();
            if (!string.IsNullOrWhiteSpace(catalog))
            {
                parts.Add(catalog.Trim());
            }

            if (!string.IsNullOrWhiteSpace(schema))
            {
                parts.Add(schema.Trim());
            }

            parts.Add(name.Trim());
            return string.Join('.', parts);
        }

        private static Dictionary<string, ColumnSourceInfo> BuildFunctionColumnInfoMap(FunctionInfo function)
        {
            var map = new Dictionary<string, ColumnSourceInfo>(StringComparer.OrdinalIgnoreCase);
            if (function.Columns == null)
            {
                return map;
            }

            var reference = new ProcedureReferenceModel
            {
                Kind = ProcedureReferenceKind.Function,
                Schema = function.Schema,
                Name = function.Name
            };

            foreach (var column in function.Columns)
            {
                if (column == null || string.IsNullOrWhiteSpace(column.Name))
                {
                    continue;
                }

                var typeRef = column.TypeRef;
                var sqlType = column.SqlTypeName;
                var maxLength = column.MaxLength;
                var precision = column.Precision;
                var scale = column.Scale;
                var isNullable = column.IsNullable;
                string? userTypeSchema = null;
                string? userTypeName = null;

                if (!string.IsNullOrWhiteSpace(typeRef))
                {
                    var (_, schemaPart, namePart) = SplitTypeRef(typeRef);
                    userTypeSchema = schemaPart;
                    userTypeName = namePart;
                }

                if (string.IsNullOrWhiteSpace(sqlType) && !string.IsNullOrWhiteSpace(typeRef))
                {
                    foreach (var resolver in TypeMetadataResolvers.Value)
                    {
                        try
                        {
                            var resolved = resolver.Resolve(typeRef, maxLength, precision, scale);
                            if (!resolved.HasValue)
                            {
                                continue;
                            }

                            sqlType = resolved.Value.SqlType;
                            maxLength ??= resolved.Value.MaxLength;
                            precision ??= resolved.Value.Precision;
                            scale ??= resolved.Value.Scale;
                            isNullable ??= resolved.Value.IsNullable;
                            break;
                        }
                        catch
                        {
                            // Ignore resolver failures for individual entries and continue with remaining resolvers.
                        }
                    }
                }

                map[column.Name] = new ColumnSourceInfo
                {
                    Schema = function.Schema,
                    Table = function.Name,
                    Column = column.Name,
                    Reference = reference,
                    SqlTypeName = NormalizeResolvedSqlType(sqlType, userTypeSchema, userTypeName),
                    MaxLength = maxLength,
                    Precision = precision,
                    Scale = scale,
                    IsNullable = isNullable,
                    UserTypeSchema = userTypeSchema,
                    UserTypeName = userTypeName
                };
            }

            return map;
        }

        private static Dictionary<string, ColumnSourceInfo> BuildFunctionColumnInfoMapFromMetadata(IReadOnlyList<ColumnMetadata> columns, string schema, string name, string? catalog)
        {
            var map = new Dictionary<string, ColumnSourceInfo>(StringComparer.OrdinalIgnoreCase);
            if (columns == null || columns.Count == 0)
            {
                return map;
            }

            var reference = new ProcedureReferenceModel
            {
                Kind = ProcedureReferenceKind.Function,
                Schema = schema,
                Catalog = catalog,
                Name = name
            };

            foreach (var column in columns)
            {
                if (column == null || string.IsNullOrWhiteSpace(column.Name))
                {
                    continue;
                }

                var sqlType = NormalizeResolvedSqlType(column.SqlTypeName, column.UserTypeSchema, column.UserTypeName);

                map[column.Name] = new ColumnSourceInfo
                {
                    Catalog = string.IsNullOrWhiteSpace(column.Catalog) ? catalog : column.Catalog,
                    Schema = schema,
                    Table = name,
                    Column = column.Name,
                    Reference = reference,
                    SqlTypeName = sqlType,
                    MaxLength = column.MaxLength,
                    Precision = column.Precision,
                    Scale = column.Scale,
                    IsNullable = column.IsNullable,
                    UserTypeSchema = column.UserTypeSchema,
                    UserTypeName = column.UserTypeName
                };
            }

            return map;
        }

        private static TableInfo? TryLookupTable(string? schema, string name)
        {
            var effectiveSchema = string.IsNullOrWhiteSpace(schema) ? "dbo" : schema.Trim();
            foreach (var root in SnapshotRootLocator.EnumerateSnapshotRoots())
            {
                try
                {
                    var provider = new TableMetadataProvider(root);
                    var table = provider.TryGet(effectiveSchema, name);
                    if (table != null)
                    {
                        return table;
                    }
                }
                catch
                {
                    // Ignore provider initialization issues for individual roots and continue probing remaining roots.
                }
            }

            return null;
        }

        private static string? NormalizeResolvedSqlType(string? sqlType, string? userTypeSchema, string? userTypeName)
        {
            if (!string.IsNullOrWhiteSpace(sqlType))
            {
                return sqlType;
            }

            return BuildTypeRef(userTypeSchema, userTypeName);
        }

        private ColumnSourceInfo? BuildScalarVariableMetadata(DataTypeReference? dataType, string? schema, string? name)
        {
            if (dataType == null && string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            if (dataType is SqlDataTypeReference sqlDataType)
            {
                return BuildSqlDataTypeMetadata(sqlDataType);
            }

            if (dataType is UserDataTypeReference userType)
            {
                var (userSchema, userName) = ExtractSchemaAndName(userType.Name);
                userSchema ??= schema;
                userName ??= name;
                var typeName = string.IsNullOrWhiteSpace(userName)
                    ? null
                    : string.IsNullOrWhiteSpace(userSchema)
                        ? userName
                        : string.Concat(userSchema, ".", userName);

                return new ColumnSourceInfo
                {
                    UserTypeSchema = userSchema,
                    UserTypeName = userName,
                    SqlTypeName = typeName
                };
            }

            var baseName = dataType?.Name?.BaseIdentifier?.Value ?? name;
            if (!string.IsNullOrWhiteSpace(baseName))
            {
                var resolvedType = string.IsNullOrWhiteSpace(schema)
                    ? baseName
                    : string.Concat(schema, ".", baseName);

                return new ColumnSourceInfo
                {
                    SqlTypeName = resolvedType,
                    UserTypeSchema = schema,
                    UserTypeName = name
                };
            }

            return null;
        }

        private static ColumnSourceInfo BuildSqlDataTypeMetadata(SqlDataTypeReference sqlDataType)
        {
            var sqlTypeName = NormalizeSqlTypeOption(sqlDataType.SqlDataTypeOption);
            int? maxLength = null;
            int? precision = null;
            int? scale = null;

            if (sqlDataType.Parameters != null && sqlDataType.Parameters.Count > 0)
            {
                var first = TryParseInt(sqlDataType.Parameters[0]);
                if (sqlDataType.SqlDataTypeOption is SqlDataTypeOption.Decimal or SqlDataTypeOption.Numeric)
                {
                    precision = first;
                    if (sqlDataType.Parameters.Count > 1)
                    {
                        scale = TryParseInt(sqlDataType.Parameters[1]);
                    }
                }
                else
                {
                    maxLength = first;
                }
            }

            return new ColumnSourceInfo
            {
                SqlTypeName = sqlTypeName,
                MaxLength = maxLength,
                Precision = precision,
                Scale = scale
            };
        }

        private static string? NormalizeSqlTypeOption(SqlDataTypeOption option)
        {
            var name = option.ToString();
            return string.IsNullOrWhiteSpace(name) ? null : name.ToLowerInvariant();
        }

        private static int? TryParseInt(TSqlFragment fragment)
        {
            if (fragment is null)
            {
                return null;
            }

            if (fragment is IntegerLiteral integerLiteral && int.TryParse(integerLiteral.Value, out var integerValue))
            {
                return integerValue;
            }

            if (fragment is NumericLiteral numericLiteral && int.TryParse(numericLiteral.Value, out var numericValue))
            {
                return numericValue;
            }

            if (fragment is Literal literal)
            {
                var raw = literal.Value;
                if (string.Equals(raw, "max", StringComparison.OrdinalIgnoreCase))
                {
                    return -1;
                }

                if (int.TryParse(raw, out var literalValue))
                {
                    return literalValue;
                }
            }

            return null;
        }

        private static TableTypeBinding? CreateTableTypeBinding(string? schema, string? name, string? defaultCatalog)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            schema ??= "dbo";
            var key = BuildTableTypeKey(schema, name);
            if (!TableTypeMetadataCache.Value.TryGetValue(key, out var metadata) || metadata == null || metadata.Columns == null || metadata.Columns.Count == 0)
            {
                return null;
            }

            var reference = new ProcedureReferenceModel
            {
                Kind = ProcedureReferenceKind.TableType,
                Schema = schema,
                Name = name
            };

            var columns = new Dictionary<string, ColumnSourceInfo>(StringComparer.OrdinalIgnoreCase);
            foreach (var column in metadata.Columns)
            {
                if (column == null || string.IsNullOrWhiteSpace(column.Name))
                {
                    continue;
                }

                var (_, userTypeSchema, userTypeName) = SplitTypeRef(column.TypeRef);

                columns[column.Name] = new ColumnSourceInfo
                {
                    Catalog = defaultCatalog,
                    Schema = schema,
                    Table = name,
                    Column = column.Name,
                    Reference = reference,
                    SqlTypeName = string.IsNullOrWhiteSpace(column.SqlType) ? null : column.SqlType,
                    MaxLength = column.MaxLength,
                    Precision = column.Precision,
                    Scale = column.Scale,
                    IsNullable = column.IsNullable,
                    UserTypeSchema = userTypeSchema,
                    UserTypeName = userTypeName
                };
            }

            if (columns.Count == 0)
            {
                return null;
            }

            return new TableTypeBinding
            {
                Catalog = defaultCatalog,
                Schema = schema,
                Name = name,
                Columns = columns,
                Reference = reference
            };
        }

        private static string? BuildTypeRef(string? schema, string? name)
            => BuildTypeRef(null, schema, name);

        private static string? BuildTypeRef(string? catalog, string? schema, string? name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return null;
            }

            var namePart = name.Trim();
            if (namePart.Length == 0)
            {
                return null;
            }

            var schemaPart = string.IsNullOrWhiteSpace(schema) ? null : schema.Trim();
            var catalogPart = string.IsNullOrWhiteSpace(catalog) ? null : catalog.Trim();

            if (!string.IsNullOrWhiteSpace(catalogPart))
            {
                if (!string.IsNullOrWhiteSpace(schemaPart))
                {
                    return string.Concat(catalogPart, ".", schemaPart, ".", namePart);
                }

                return string.Concat(catalogPart, ".", namePart);
            }

            if (!string.IsNullOrWhiteSpace(schemaPart))
            {
                return string.Concat(schemaPart, ".", namePart);
            }

            return namePart;
        }

        private static (string? Catalog, string? Schema, string? Name) SplitTypeRef(string? typeRef)
        {
            if (string.IsNullOrWhiteSpace(typeRef))
            {
                return (null, null, null);
            }

            var parts = typeRef.Trim().Split('.', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length == 0)
            {
                return (null, null, null);
            }

            var name = string.IsNullOrWhiteSpace(parts[^1]) ? null : parts[^1];
            var schema = parts.Length >= 2 ? (string.IsNullOrWhiteSpace(parts[^2]) ? null : parts[^2]) : null;
            var catalog = parts.Length >= 3 ? (string.IsNullOrWhiteSpace(parts[^3]) ? null : parts[^3]) : null;
            return (catalog, schema, name);
        }

        private bool TryApplyColumnMetadataFromScopes(ProcedureResultColumn column, string? columnName)
        {
            if (column == null || string.IsNullOrWhiteSpace(columnName))
            {
                return false;
            }

            var matches = new List<(TableAliasInfo Alias, ColumnSourceInfo Source)>();

            foreach (var scope in _aliasScopes)
            {
                if (scope == null || scope.Count == 0)
                {
                    continue;
                }

                var seenEntries = new HashSet<TableAliasInfo>();
                foreach (var aliasEntry in scope.Values)
                {
                    if (aliasEntry?.Columns == null || !seenEntries.Add(aliasEntry))
                    {
                        continue;
                    }

                    if (aliasEntry.Columns.TryGetValue(columnName, out var sourceInfo) && sourceInfo != null)
                    {
                        matches.Add((aliasEntry, sourceInfo));
                    }
                }

                if (matches.Count > 0)
                {
                    break;
                }
            }

            if (matches.Count == 0)
            {
                return false;
            }

            if (matches.Count > 1)
            {
                column.IsAmbiguous = true;
                return false;
            }

            var (alias, info) = matches[0];

            if (!string.IsNullOrWhiteSpace(info.Schema))
            {
                column.SourceSchema ??= info.Schema;
            }
            else if (!string.IsNullOrWhiteSpace(alias.Schema))
            {
                column.SourceSchema ??= alias.Schema;
            }

            if (!string.IsNullOrWhiteSpace(info.Catalog))
            {
                column.SourceCatalog ??= info.Catalog;
            }
            else if (!string.IsNullOrWhiteSpace(alias.Catalog))
            {
                column.SourceCatalog ??= alias.Catalog;
            }

            if (!string.IsNullOrWhiteSpace(info.Table))
            {
                column.SourceTable ??= info.Table;
            }
            else if (!string.IsNullOrWhiteSpace(alias.Name))
            {
                column.SourceTable ??= alias.Name;
            }

            if (!string.IsNullOrWhiteSpace(info.Column))
            {
                column.SourceColumn = info.Column;
            }

            if (column.Reference == null && info.Reference != null)
            {
                column.Reference = CloneReference(info.Reference);
            }

            if (info.ReturnsJson.HasValue)
            {
                column.ReturnsJson ??= info.ReturnsJson;
            }

            if (info.ReturnsJsonArray.HasValue)
            {
                column.ReturnsJsonArray ??= info.ReturnsJsonArray;
            }

            if (info.ReturnsUnknownJson.HasValue)
            {
                column.ReturnsUnknownJson ??= info.ReturnsUnknownJson;
            }

            if (info.IsNestedJson.HasValue)
            {
                column.IsNestedJson ??= info.IsNestedJson;
            }

            if (!string.IsNullOrWhiteSpace(info.SqlTypeName))
            {
                column.SqlTypeName ??= info.SqlTypeName;
            }

            if (!string.IsNullOrWhiteSpace(info.CastTargetType))
            {
                column.CastTargetType ??= info.CastTargetType;
            }

            if (info.CastTargetLength.HasValue)
            {
                column.CastTargetLength ??= info.CastTargetLength;
            }

            if (info.CastTargetPrecision.HasValue)
            {
                column.CastTargetPrecision ??= info.CastTargetPrecision;
            }

            if (info.CastTargetScale.HasValue)
            {
                column.CastTargetScale ??= info.CastTargetScale;
            }

            if (info.MaxLength.HasValue)
            {
                column.MaxLength ??= info.MaxLength;
            }

            if (info.IsNullable.HasValue)
            {
                column.IsNullable ??= info.IsNullable;
            }

            if (!string.IsNullOrWhiteSpace(info.UserTypeSchema))
            {
                column.UserTypeSchemaName ??= info.UserTypeSchema;
            }

            if (!string.IsNullOrWhiteSpace(info.UserTypeName))
            {
                column.UserTypeName ??= info.UserTypeName;
            }

            ApplyUserTypeResolution(column, info);

            return true;
        }

        private static string NormalizeVariableName(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return string.Empty;
            }

            return value.StartsWith("@", StringComparison.Ordinal) ? value : string.Concat("@", value);
        }

        private TableTypeBinding? BuildInlineTableVariableBinding(string variableName, IList<ColumnDefinition>? definitions)
        {
            if (string.IsNullOrWhiteSpace(variableName) || definitions == null || definitions.Count == 0)
            {
                return null;
            }

            var normalizedName = NormalizeTableVariableDisplayName(variableName);
            var columns = new Dictionary<string, ColumnSourceInfo>(StringComparer.OrdinalIgnoreCase);

            foreach (var definition in definitions)
            {
                if (definition == null)
                {
                    continue;
                }

                var columnName = definition.ColumnIdentifier?.Value;
                if (string.IsNullOrWhiteSpace(columnName))
                {
                    continue;
                }

                var baseInfo = BuildScalarVariableMetadata(definition.DataType, null, null);
                var columnInfo = new ColumnSourceInfo
                {
                    Catalog = _defaultCatalog,
                    Schema = null,
                    Table = normalizedName,
                    Column = columnName,
                    SqlTypeName = baseInfo?.SqlTypeName,
                    MaxLength = baseInfo?.MaxLength,
                    Precision = baseInfo?.Precision,
                    Scale = baseInfo?.Scale,
                    IsNullable = baseInfo?.IsNullable,
                    UserTypeSchema = baseInfo?.UserTypeSchema,
                    UserTypeName = baseInfo?.UserTypeName
                };

                columns[columnName] = columnInfo;
            }

            if (columns.Count == 0)
            {
                return null;
            }

            return new TableTypeBinding
            {
                Catalog = _defaultCatalog,
                Schema = null,
                Name = normalizedName,
                Columns = columns
            };
        }

        private TableTypeBinding? TryBuildInlineTableVariableBinding(string variableName, DataTypeReference? dataType)
        {
            if (string.IsNullOrWhiteSpace(variableName) || dataType == null)
            {
                return null;
            }

            IList<ColumnDefinition>? columns = null;
            var type = dataType.GetType();
            var columnDefinitionsProperty = type.GetProperty("ColumnDefinitions", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
            if (columnDefinitionsProperty?.GetValue(dataType) is IList<ColumnDefinition> directColumns)
            {
                columns = directColumns;
            }
            else
            {
                var definitionProperty = type.GetProperty("Definition", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                if (definitionProperty?.GetValue(dataType) is { } definition)
                {
                    var definitionType = definition.GetType();
                    var definitionColumnsProperty = definitionType.GetProperty("ColumnDefinitions", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
                    if (definitionColumnsProperty?.GetValue(definition) is IList<ColumnDefinition> definitionColumns)
                    {
                        columns = definitionColumns;
                    }
                }
            }

            return columns == null ? null : BuildInlineTableVariableBinding(variableName, columns);
        }

        private static bool NeedsUserTypeResolution(string? sqlTypeName)
        {
            if (string.IsNullOrWhiteSpace(sqlTypeName))
            {
                return true;
            }

            return sqlTypeName.Contains('.', StringComparison.Ordinal);
        }

        private static void ApplyUserTypeResolution(ProcedureResultColumn column, ColumnSourceInfo? sourceInfo = null)
        {
            if (column == null)
            {
                return;
            }

            if (!NeedsUserTypeResolution(column.SqlTypeName))
            {
                return;
            }

            var userTypeSchema = column.UserTypeSchemaName ?? sourceInfo?.UserTypeSchema;
            var userTypeName = column.UserTypeName ?? sourceInfo?.UserTypeName;
            if (string.IsNullOrWhiteSpace(userTypeName))
            {
                return;
            }

            var catalog = column.SourceCatalog ?? sourceInfo?.Catalog;
            var typeRef = BuildTypeRef(catalog, userTypeSchema, userTypeName);
            if (string.IsNullOrWhiteSpace(typeRef))
            {
                return;
            }

            var length = sourceInfo?.MaxLength ?? column.MaxLength;
            var precision = sourceInfo?.Precision;
            var scale = sourceInfo?.Scale;

            foreach (var resolver in TypeMetadataResolvers.Value)
            {
                try
                {
                    var resolved = resolver.Resolve(typeRef, length, precision, scale);
                    if (!resolved.HasValue)
                    {
                        continue;
                    }

                    var resolvedType = resolved.Value.SqlType;
                    if (!string.IsNullOrWhiteSpace(resolvedType))
                    {
                        column.SqlTypeName = resolvedType;
                    }

                    if (!column.MaxLength.HasValue && resolved.Value.MaxLength.HasValue)
                    {
                        column.MaxLength = resolved.Value.MaxLength;
                    }

                    return;
                }
                catch
                {
                    // Ignore resolver failures and continue with remaining candidates.
                }
            }
        }

        private static string NormalizeTableVariableDisplayName(string variableName)
        {
            if (string.IsNullOrWhiteSpace(variableName))
            {
                return string.Empty;
            }

            var trimmed = variableName.Trim();
            return trimmed.StartsWith("@", StringComparison.Ordinal)
                ? trimmed[1..]
                : trimmed;
        }

        private void LogVerbose(string message)
        {
            if (!_verboseParsing)
            {
                return;
            }

            Console.WriteLine(message);
        }

        private static bool IsAliasDebugEnabled()
        {
            return LogLevelConfiguration.IsAtLeast(LogLevelThreshold.Debug);
        }
    }
}
