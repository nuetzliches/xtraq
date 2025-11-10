using System.Text.Json.Serialization;
using Xtraq.Utils;

namespace Xtraq.Services;

/// <summary>
/// Very lightweight local metadata cache. Not committed to source control.
/// Stores per stored procedure last known ModifiedTicks to allow skipping expensive detail loading.
/// </summary>
internal interface ILocalCacheService
{
    ProcedureCacheSnapshot? Load(string fingerprint);
    void Save(string fingerprint, ProcedureCacheSnapshot snapshot);

    /// <summary>
    /// Invalidates a specific cache entry by fingerprint.
    /// </summary>
    void Invalidate(string fingerprint);

    /// <summary>
    /// Invalidates all cache entries.
    /// </summary>
    void InvalidateAll();

    /// <summary>
    /// Invalidates cache entries matching a pattern (e.g., for schema-based invalidation).
    /// </summary>
    void InvalidateByPattern(string fingerprintPattern);
}

internal sealed class LocalCacheService : ILocalCacheService
{
    private string? _rootDir; // lazily resolved based on working directory
    private string? _lastWorkingDir;
    private readonly JsonSerializerOptions _jsonOptions = new()
    {
        WriteIndented = true,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
    };

    public LocalCacheService() { }

    private void EnsureRoot()
    {
        var working = DirectoryUtils.GetWorkingDirectory();
        if (string.IsNullOrWhiteSpace(working))
        {
            _rootDir = null;
            _lastWorkingDir = null;
            return; // nothing we can do yet
        }

        if (_rootDir == null || !string.Equals(_lastWorkingDir, working, StringComparison.OrdinalIgnoreCase))
        {
            var dotDir = Path.Combine(working, ".xtraq");
            var candidate = Path.Combine(dotDir, "cache");
            try { Directory.CreateDirectory(candidate); } catch { /* ignore */ }
            _rootDir = candidate;
            _lastWorkingDir = working;
        }
    }

    private string? GetPath(string fingerprint)
    {
        if (string.IsNullOrWhiteSpace(fingerprint))
        {
            return null;
        }

        EnsureRoot();
        return _rootDir == null ? null : Path.Combine(_rootDir, $"{fingerprint}.json");
    }

    public ProcedureCacheSnapshot? Load(string fingerprint)
    {
        if (string.IsNullOrWhiteSpace(fingerprint))
        {
            return null;
        }

        try
        {
            var path = GetPath(fingerprint);
            if (string.IsNullOrEmpty(path) || !File.Exists(path)) return null;
            var json = File.ReadAllText(path);
            return JsonSerializer.Deserialize<ProcedureCacheSnapshot>(json, _jsonOptions);
        }
        catch { return null; }
    }

    public void Save(string fingerprint, ProcedureCacheSnapshot snapshot)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
        ArgumentNullException.ThrowIfNull(snapshot);

        try
        {
            var path = GetPath(fingerprint);
            if (string.IsNullOrEmpty(path)) return; // not initialized
            var json = JsonSerializer.Serialize(snapshot, _jsonOptions);
            File.WriteAllText(path, json);
        }
        catch { /* ignore */ }
    }

    public void Invalidate(string fingerprint)
    {
        if (string.IsNullOrWhiteSpace(fingerprint))
        {
            return;
        }

        try
        {
            var path = GetPath(fingerprint);
            if (!string.IsNullOrEmpty(path) && File.Exists(path))
            {
                File.Delete(path);
            }
        }
        catch { /* ignore */ }
    }

    public void InvalidateAll()
    {
        try
        {
            EnsureRoot();
            if (string.IsNullOrEmpty(_rootDir) || !Directory.Exists(_rootDir)) return;

            var files = Directory.GetFiles(_rootDir, "*.json", SearchOption.TopDirectoryOnly);
            foreach (var file in files)
            {
                try { File.Delete(file); } catch { /* ignore individual failures */ }
            }
        }
        catch { /* ignore */ }
    }

    public void InvalidateByPattern(string fingerprintPattern)
    {
        try
        {
            EnsureRoot();
            if (string.IsNullOrEmpty(_rootDir) || !Directory.Exists(_rootDir)) return;

            if (string.IsNullOrWhiteSpace(fingerprintPattern))
            {
                InvalidateAll();
                return;
            }

            // Convert simple pattern to file system search pattern
            var sanitizedPattern = fingerprintPattern.Trim();
            if (!sanitizedPattern.Contains('*', StringComparison.Ordinal))
            {
                sanitizedPattern = sanitizedPattern + "*";
            }

            var searchPattern = sanitizedPattern.EndsWith(".json", StringComparison.OrdinalIgnoreCase)
                ? sanitizedPattern
                : sanitizedPattern + ".json";

            var files = Directory.GetFiles(_rootDir, searchPattern, SearchOption.TopDirectoryOnly);
            foreach (var file in files)
            {
                try { File.Delete(file); } catch { /* ignore individual failures */ }
            }
        }
        catch { /* ignore */ }
    }
}

internal sealed class ProcedureCacheSnapshot
{
    public string Fingerprint { get; set; } = string.Empty;
    public DateTime CreatedUtc { get; set; } = DateTime.UtcNow;
    public List<ProcedureCacheEntry> Procedures { get; set; } = new();

    public long? GetModifiedTicks(string schema, string name)
        => Procedures.FirstOrDefault(p => p.Schema == schema && p.Name == name)?.ModifiedTicks;
}

internal sealed class ProcedureCacheEntry
{
    public string Schema { get; set; } = string.Empty;
    public string Name { get; set; } = string.Empty;
    public long ModifiedTicks { get; set; }
}
