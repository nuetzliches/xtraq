namespace Xtraq.Utils;

internal static class NamePolicy
{
    private static readonly HashSet<string> CSharpKeywords = new(new[]
    {
        "class","namespace","string","int","long","short","byte","bool","decimal","double","float","object","record","struct","event","base","this","new","public","internal","protected","private","static","void","using","return"
    });

    internal static string Input(string operation) => Sanitize(operation) + "Input";
    internal static string Output(string operation) => Sanitize(operation) + "Output";
    internal static string Result(string operation) => Sanitize(operation) + "Result";
    internal static string Procedure(string operation) => Sanitize(operation);
    // New unified result naming: Each result set becomes an inline record inside <Proc>Result.cs
    // For internal referencing we still need a deterministic type name per set.
    // Spec: Type name = <Proc><SetNameOrIndex>Result (no 'Row' suffix)
    internal static string ResultSet(string operation, string setName) => Sanitize(operation) + Sanitize(setName) + "Result";
    internal static string Sanitize(string raw)
    {
        if (string.IsNullOrWhiteSpace(raw)) return "Name";
        // Split on hyphen or underscore boundaries first, then sanitize parts to enable workflow-state -> WorkflowState
        var segments = raw.Split(new[] { '-', '_' }, StringSplitOptions.RemoveEmptyEntries);
        var builder = new StringBuilder();
        foreach (var seg in segments)
        {
            var cleanedSeg = Regex.Replace(seg, "[^A-Za-z0-9]", "");
            if (string.IsNullOrWhiteSpace(cleanedSeg)) continue;
            if (char.IsDigit(cleanedSeg[0])) cleanedSeg = "N" + cleanedSeg;
            cleanedSeg = char.ToUpperInvariant(cleanedSeg[0]) + (cleanedSeg.Length > 1 ? cleanedSeg.Substring(1) : string.Empty);
            builder.Append(cleanedSeg);
        }
        var cleaned = builder.ToString();
        if (cleaned.Length == 0) cleaned = "Name";
        if (CSharpKeywords.Contains(cleaned)) cleaned = "@" + cleaned;
        return cleaned;
    }
}
