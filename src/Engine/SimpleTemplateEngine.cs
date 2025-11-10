namespace Xtraq.Engine;

/// <summary>
/// Lightweight templating engine with nested block support (placeholders, #if/#elseif/#else, #each).
/// </summary>
public sealed class SimpleTemplateEngine : ITemplateRenderer
{
    /// <inheritdoc />
    public string Render(string template, object? model)
    {
        if (template is null) throw new ArgumentNullException(nameof(template));
        if (string.IsNullOrEmpty(template) || model is null)
        {
            return template;
        }

        var nodes = Parse(template);
        var json = JsonSerializer.SerializeToElement(model, new JsonSerializerOptions
        {
            PropertyNameCaseInsensitive = true
        });

        var scope = new EvaluationScope(json, json);
        var buffer = new StringBuilder(template.Length);
        RenderNodes(nodes, scope, buffer);
        return buffer.ToString();
    }

    private static IReadOnlyList<Node> Parse(string template)
    {
        var frames = new Stack<BlockFrame>();
        var root = BlockFrame.CreateRoot();
        frames.Push(root);

        var position = 0;
        while (position < template.Length)
        {
            var open = template.IndexOf("{{", position, StringComparison.Ordinal);
            if (open < 0)
            {
                frames.Peek().AddText(template[position..]);
                break;
            }

            if (open > position)
            {
                frames.Peek().AddText(template[position..open]);
            }

            var close = template.IndexOf("}}", open + 2, StringComparison.Ordinal);
            if (close < 0)
            {
                frames.Peek().AddText(template[open..]);
                position = template.Length;
                break;
            }

            var rawToken = template[(open + 2)..close];
            var token = rawToken.Trim();

            var standalone = IsStandaloneCandidate(token)
                ? AnalyzeStandalone(template, open, close + 2)
                : default;

            if (standalone.IsStandalone)
            {
                frames.Peek().TrimTrailingWhitespaceOnCurrentLine();
            }

            ProcessToken(token, frames);
            position = standalone.IsStandalone ? standalone.NextIndex : close + 2;
        }

        if (frames.Count != 1)
        {
            throw new InvalidOperationException("Unterminated template block detected.");
        }

        return root.Nodes;
    }

    private static bool IsStandaloneCandidate(string token)
    {
        if (string.IsNullOrEmpty(token))
        {
            return false;
        }

        if (token[0] == '#' || token[0] == '/' || token[0] == '!')
        {
            return true;
        }

        if (token.Equals("else", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        if (token.StartsWith("elseif", StringComparison.OrdinalIgnoreCase))
        {
            return true;
        }

        return false;
    }

    private static StandaloneDirectiveInfo AnalyzeStandalone(string template, int openIndex, int postCloseIndex)
    {
        var lineStart = openIndex;
        while (lineStart > 0)
        {
            var prior = template[lineStart - 1];
            if (prior == '\n' || prior == '\r')
            {
                break;
            }

            lineStart--;
        }

        for (var i = lineStart; i < openIndex; i++)
        {
            var ch = template[i];
            if (ch != ' ' && ch != '\t')
            {
                return default;
            }
        }

        var index = postCloseIndex;
        while (index < template.Length)
        {
            var ch = template[index];
            if (ch == ' ' || ch == '\t')
            {
                index++;
                continue;
            }

            if (ch == '\r')
            {
                index++;
                if (index < template.Length && template[index] == '\n')
                {
                    index++;
                }

                return new StandaloneDirectiveInfo(true, index);
            }

            if (ch == '\n')
            {
                return new StandaloneDirectiveInfo(true, index + 1);
            }

            return default;
        }

        return new StandaloneDirectiveInfo(true, index);
    }

    private static void ProcessToken(string token, Stack<BlockFrame> frames)
    {
        if (string.IsNullOrEmpty(token))
        {
            // Treat empty directive as literal braces (consistent with previous behaviour).
            frames.Peek().AddText("{{}}");
            return;
        }

        if (token.StartsWith("!--", StringComparison.Ordinal) || token[0] == '!')
        {
            return; // comment / noop directive
        }

        if (token.StartsWith("#each", StringComparison.OrdinalIgnoreCase))
        {
            var spec = token[5..].Trim();
            var (path, alias) = ParseEachSpec(spec);
            frames.Push(BlockFrame.CreateEach(path, alias));
            return;
        }

        if (token.Equals("/each", StringComparison.OrdinalIgnoreCase))
        {
            CloseEach(frames);
            return;
        }

        if (token.StartsWith("#if", StringComparison.OrdinalIgnoreCase))
        {
            var expr = token[3..].Trim();
            if (string.IsNullOrWhiteSpace(expr))
            {
                throw new InvalidOperationException("The #if block requires a condition.");
            }
            frames.Push(BlockFrame.CreateIf(expr));
            return;
        }

        if (token.Equals("/if", StringComparison.OrdinalIgnoreCase))
        {
            CloseIf(frames);
            return;
        }

        if (token.StartsWith("#elseif", StringComparison.OrdinalIgnoreCase))
        {
            var expr = token[7..].Trim();
            EnsureIfFrame(frames).StartElseIfBranch(expr);
            return;
        }

        if (token.StartsWith("elseif", StringComparison.OrdinalIgnoreCase))
        {
            var expr = token[6..].Trim();
            EnsureIfFrame(frames).StartElseIfBranch(expr);
            return;
        }

        if (token.Equals("else", StringComparison.OrdinalIgnoreCase))
        {
            EnsureIfFrame(frames).StartElseBranch();
            return;
        }

        frames.Peek().AddNode(new PlaceholderNode(SplitPath(token)));
    }

    private static BlockFrame EnsureIfFrame(Stack<BlockFrame> frames)
    {
        if (frames.Count == 0 || frames.Peek().Kind != BlockKind.If)
        {
            throw new InvalidOperationException("Conditional branch directive requires an open #if block.");
        }

        return frames.Peek();
    }

    private static void CloseEach(Stack<BlockFrame> frames)
    {
        if (frames.Count <= 1)
        {
            throw new InvalidOperationException("Encountered /each without a matching #each block.");
        }

        var frame = frames.Pop();
        if (frame.Kind != BlockKind.Each)
        {
            throw new InvalidOperationException("Encountered /each while the active block is not #each.");
        }

        frames.Peek().AddNode(new EachNode(frame.EachPathSegments!, frame.EachAlias, frame.Nodes));
    }

    private static void CloseIf(Stack<BlockFrame> frames)
    {
        if (frames.Count <= 1)
        {
            throw new InvalidOperationException("Encountered /if without a matching #if block.");
        }

        var frame = frames.Pop();
        if (frame.Kind != BlockKind.If)
        {
            throw new InvalidOperationException("Encountered /if while the active block is not #if.");
        }

        frames.Peek().AddNode(new IfNode(frame.IfBuilder!.Branches));
    }

    private static (string Path, string? Alias) ParseEachSpec(string spec)
    {
        if (string.IsNullOrWhiteSpace(spec))
        {
            throw new InvalidOperationException("The #each block requires a data source.");
        }

        var parts = spec.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        if (parts.Length == 0)
        {
            throw new InvalidOperationException("The #each block requires a data source.");
        }

        string path = parts[0];
        string? alias = null;

        if (parts.Length >= 3 && parts[1].Equals("as", StringComparison.OrdinalIgnoreCase))
        {
            alias = parts[2];
        }

        return (path, alias);
    }

    private static string[] SplitPath(string value)
    {
        return value.Split('.', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }

    private static void RenderNodes(IReadOnlyList<Node> nodes, EvaluationScope scope, StringBuilder output)
    {
        foreach (var node in nodes)
        {
            switch (node)
            {
                case TextNode text:
                    output.Append(text.Text);
                    break;
                case PlaceholderNode placeholder:
                    if (TryResolve(scope, placeholder.PathSegments, out var resolved) && resolved.ValueKind != JsonValueKind.Null && resolved.ValueKind != JsonValueKind.Undefined)
                    {
                        output.Append(resolved.ToString());
                    }
                    break;
                case EachNode each:
                    RenderEach(each, scope, output);
                    break;
                case IfNode conditional:
                    RenderIf(conditional, scope, output);
                    break;
            }
        }
    }

    private static void RenderEach(EachNode node, EvaluationScope scope, StringBuilder output)
    {
        if (!TryResolve(scope, node.PathSegments, out var resolved) || resolved.ValueKind != JsonValueKind.Array)
        {
            return;
        }

        foreach (var element in resolved.EnumerateArray())
        {
            var childScope = scope.CreateChild(element, node.Alias);
            RenderNodes(node.Body, childScope, output);
        }
    }

    private static void RenderIf(IfNode node, EvaluationScope scope, StringBuilder output)
    {
        foreach (var branch in node.Branches)
        {
            if (branch.ExpressionSegments == null)
            {
                RenderNodes(branch.Body, scope, output);
                return;
            }

            if (TryResolve(scope, branch.ExpressionSegments, out var result) && IsTruthy(result))
            {
                RenderNodes(branch.Body, scope, output);
                return;
            }
        }
    }

    private static bool TryResolve(EvaluationScope scope, string[] path, out JsonElement value)
    {
        if (path.Length == 0)
        {
            value = scope.Current;
            return true;
        }

        // this.* always maps to the current scope element
        if (path[0].Equals("this", StringComparison.OrdinalIgnoreCase))
        {
            return TryResolveFromElement(scope.Current, path, 1, out value);
        }

        if (scope.HasAlias && path[0].Equals(scope.Alias, StringComparison.OrdinalIgnoreCase))
        {
            return TryResolveFromElement(scope.AliasElement, path, 1, out value);
        }

        if (TryResolveFromElement(scope.Current, path, 0, out value))
        {
            return true;
        }

        if (scope.HasAlias && TryResolveFromElement(scope.AliasElement, path, 0, out value))
        {
            return true;
        }

        if (scope.Parent != null && TryResolve(scope.Parent, path, out value))
        {
            return true;
        }

        return TryResolveFromElement(scope.Root, path, 0, out value);
    }

    private static bool TryResolveFromElement(JsonElement element, string[] path, int startIndex, out JsonElement value)
    {
        value = element;
        if (startIndex >= path.Length)
        {
            return true;
        }

        for (var i = startIndex; i < path.Length; i++)
        {
            var segment = path[i];
            if (value.ValueKind == JsonValueKind.Object && value.TryGetProperty(segment, out var next))
            {
                value = next;
                continue;
            }

            if (value.ValueKind == JsonValueKind.Array && int.TryParse(segment, NumberStyles.Integer, CultureInfo.InvariantCulture, out var index))
            {
                if (index >= 0 && index < value.GetArrayLength())
                {
                    var enumerator = value.EnumerateArray();
                    for (var currentIndex = 0; currentIndex <= index; currentIndex++)
                    {
                        enumerator.MoveNext();
                    }

                    value = enumerator.Current;
                    continue;
                }
            }

            value = default;
            return false;
        }

        return true;
    }

    private static bool IsTruthy(JsonElement value)
    {
        return value.ValueKind switch
        {
            JsonValueKind.False => false,
            JsonValueKind.Null => false,
            JsonValueKind.Undefined => false,
            JsonValueKind.String => !string.IsNullOrEmpty(value.ToString()),
            JsonValueKind.Array => value.GetArrayLength() > 0,
            JsonValueKind.Object => value.EnumerateObject().Any(),
            JsonValueKind.Number => true,
            JsonValueKind.True => true,
            _ => false
        };
    }

    private abstract record Node;

    private sealed record TextNode(string Text) : Node;

    private sealed record PlaceholderNode(string[] PathSegments) : Node;

    private sealed record EachNode(string[] PathSegments, string? Alias, IReadOnlyList<Node> Body) : Node;

    private sealed record IfNode(IReadOnlyList<IfBranch> Branches) : Node;

    private sealed record IfBranch(string[]? ExpressionSegments, IReadOnlyList<Node> Body);

    private enum BlockKind
    {
        Root,
        Each,
        If
    }

    private sealed class BlockFrame
    {
        private BlockFrame(BlockKind kind)
        {
            Kind = kind;
            Nodes = new List<Node>();
        }

        public BlockKind Kind { get; }

        public List<Node> Nodes { get; }

        public IfBuilder? IfBuilder { get; private set; }

        public string[]? EachPathSegments { get; private set; }

        public string? EachAlias { get; private set; }

        private List<Node> CurrentNodes => Kind == BlockKind.If ? IfBuilder!.CurrentBody : Nodes;

        public static BlockFrame CreateRoot() => new(BlockKind.Root);

        public static BlockFrame CreateEach(string path, string? alias)
        {
            return new BlockFrame(BlockKind.Each)
            {
                EachPathSegments = SplitPath(path),
                EachAlias = alias
            };
        }

        public static BlockFrame CreateIf(string expr)
        {
            var frame = new BlockFrame(BlockKind.If)
            {
                IfBuilder = new IfBuilder()
            };
            frame.IfBuilder!.BeginCondition(expr);
            return frame;
        }

        public void AddText(string text)
        {
            if (string.IsNullOrEmpty(text))
            {
                return;
            }

            var current = CurrentNodes;
            if (current.Count > 0 && current[^1] is TextNode existing)
            {
                current[^1] = existing with { Text = existing.Text + text };
            }
            else
            {
                current.Add(new TextNode(text));
            }
        }

        public void AddNode(Node node)
        {
            CurrentNodes.Add(node);
        }

        public void TrimTrailingWhitespaceOnCurrentLine()
        {
            var current = CurrentNodes;
            if (current.Count == 0)
            {
                return;
            }

            if (current[^1] is TextNode existing)
            {
                var trimmed = TrimTrailingIndent(existing.Text);
                if (trimmed.Length == 0)
                {
                    current.RemoveAt(current.Count - 1);
                }
                else if (!ReferenceEquals(trimmed, existing.Text))
                {
                    current[^1] = existing with { Text = trimmed };
                }
            }
        }

        public void StartElseIfBranch(string expr)
        {
            if (Kind != BlockKind.If)
            {
                throw new InvalidOperationException("#elseif directive requires an open #if block.");
            }

            IfBuilder!.BeginCondition(expr);
        }

        public void StartElseBranch()
        {
            if (Kind != BlockKind.If)
            {
                throw new InvalidOperationException("else directive requires an open #if block.");
            }

            IfBuilder!.BeginElse();
        }
    }

    private sealed class IfBuilder
    {
        private readonly List<IfBranch> _branches = new();
        private List<Node> _currentBody = new();

        public IReadOnlyList<IfBranch> Branches => _branches;

        public List<Node> CurrentBody => _currentBody;

        public bool HasElse { get; private set; }

        public void BeginCondition(string expression)
        {
            if (HasElse)
            {
                throw new InvalidOperationException("#elseif cannot appear after an else branch.");
            }

            if (string.IsNullOrWhiteSpace(expression))
            {
                throw new InvalidOperationException("Conditional branch requires an expression.");
            }

            StartBranch(SplitPath(expression));
        }

        public void BeginElse()
        {
            if (HasElse)
            {
                throw new InvalidOperationException("Multiple else branches are not supported.");
            }

            HasElse = true;
            StartBranch(null);
        }

        private void StartBranch(string[]? expression)
        {
            _currentBody = new List<Node>();
            _branches.Add(new IfBranch(expression, _currentBody));
        }
    }

    private static string TrimTrailingIndent(string text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return text;
        }

        var span = text.AsSpan();
        var lastNewline = span.LastIndexOf('\n');
        if (lastNewline < 0)
        {
            lastNewline = span.LastIndexOf('\r');
        }

        var lineStart = lastNewline >= 0 ? lastNewline + 1 : 0;
        for (var i = lineStart; i < span.Length; i++)
        {
            var ch = span[i];
            if (ch != ' ' && ch != '\t')
            {
                return text;
            }
        }

        return lineStart == 0 ? string.Empty : span[..lineStart].ToString();
    }

    private sealed class EvaluationScope
    {
        public EvaluationScope(JsonElement root, JsonElement current)
        {
            Root = root;
            Current = current;
        }

        private EvaluationScope(JsonElement root, JsonElement current, EvaluationScope parent, string? alias, JsonElement aliasElement, bool hasAlias)
        {
            Root = root;
            Current = current;
            Parent = parent;
            Alias = alias;
            AliasElement = aliasElement;
            HasAlias = hasAlias;
        }

        public JsonElement Root { get; }

        public JsonElement Current { get; }

        public EvaluationScope? Parent { get; }

        public string? Alias { get; }

        public JsonElement AliasElement { get; } = default;

        public bool HasAlias { get; }

        public EvaluationScope CreateChild(JsonElement element, string? alias)
        {
            var hasAlias = !string.IsNullOrWhiteSpace(alias);
            var aliasElement = hasAlias ? element : default;
            return new EvaluationScope(Root, element, this, alias, aliasElement, hasAlias);
        }
    }

    private readonly record struct StandaloneDirectiveInfo(bool IsStandalone, int NextIndex);
}
