using Xtraq.Core;

namespace Xtraq.Services;

internal sealed class OutputService(IConsoleService consoleService)
{
    public async Task WriteAsync(string targetFileName, string content, bool isDryRun)
    {
        var directoryName = Path.GetDirectoryName(targetFileName);
        if (string.IsNullOrWhiteSpace(directoryName))
        {
            throw new System.InvalidOperationException($"OutputService: Unable to determine directory for '{targetFileName}'.");
        }
        var folderName = new DirectoryInfo(directoryName).Name;
        var fileName = Path.GetFileName(targetFileName);
        var fileAction = FileActionEnum.Created;
        var outputFileText = content;

        // Legacy XML auto-generated header removed to reduce diff churn and align with minimalist output style.

        bool exists = File.Exists(targetFileName);
        if (exists)
        {
            var existingFileText = await File.ReadAllTextAsync(targetFileName);

            // Normalize volatile timestamp remark line so only semantic changes produce a diff.
            static string NormalizeForComparison(string text)
            {
                if (string.IsNullOrEmpty(text)) return text;
                // Remove legacy volatile <remarks> lines (no longer emitted) for backward compatibility when comparing existing files.
                text = System.Text.RegularExpressions.Regex.Replace(
                    text,
                    @"^.*///\s<remarks>Generated at .*?</remarks>.*$",
                    string.Empty,
                    System.Text.RegularExpressions.RegexOptions.Multiline);
                // Normalize line endings to \n
                text = text.Replace("\r\n", "\n");
                // Trim trailing whitespace per line
                var lines = text.Split('\n');
                for (int i = 0; i < lines.Length; i++)
                {
                    lines[i] = lines[i].TrimEnd();
                }
                text = string.Join("\n", lines);
                if (!text.EndsWith("\n")) text += "\n";
                return text;
            }

            var normExisting = NormalizeForComparison(existingFileText);
            var normNew = NormalizeForComparison(outputFileText);
            var upToDate = string.Equals(normExisting, normNew, System.StringComparison.Ordinal);
            fileAction = upToDate ? FileActionEnum.UpToDate : FileActionEnum.Modified;
        }
        // Write strategy: overwrite when modified, leave the file untouched when it is up to date
        if (!isDryRun && fileAction != FileActionEnum.UpToDate)
        {
            await File.WriteAllTextAsync(targetFileName, outputFileText);
        }

        consoleService.PrintFileActionMessage($"{folderName}/{fileName}", fileAction);
    }

}

