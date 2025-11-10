using System.Text.Json.Serialization;
using Xtraq.Utils;

namespace Xtraq.Cache;

/// <summary>
/// Manages the persisted catalog index (object-index.json) containing last-known modify_date per schema object.
/// Used to diff catalog snapshots across warm runs.
/// </summary>
public interface ISchemaObjectIndexManager
{
    /// <summary>
    /// Initialise the manager by loading any existing index file.
    /// </summary>
    Task InitializeAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// Get the last persisted entries for the specified object type.
    /// </summary>
    IReadOnlyList<SchemaObjectIndexEntry> GetEntries(SchemaObjectType objectType);

    /// <summary>
    /// Replace the entries for a specific object type (used on cold runs / full scans).
    /// </summary>
    void ReplaceEntries(SchemaObjectType objectType, IEnumerable<SchemaObjectIndexEntry> entries);

    /// <summary>
    /// Upsert entries for a specific object type (used for delta updates).
    /// </summary>
    void UpsertEntries(IEnumerable<SchemaObjectIndexEntry> entries);

    /// <summary>
    /// Persist pending changes to disk if required.
    /// </summary>
    Task FlushAsync(CancellationToken cancellationToken = default);
}

/// <summary>
/// Lightweight descriptor used by the object index.
/// </summary>
public sealed class SchemaObjectIndexEntry
{
    /// <summary>
    /// Gets or sets the schema object type represented by the entry.
    /// </summary>
    public SchemaObjectType Type { get; set; }
    /// <summary>
    /// Gets or sets the schema that owns the object.
    /// </summary>
    public string Schema { get; set; } = string.Empty;
    /// <summary>
    /// Gets or sets the object name within the schema.
    /// </summary>
    public string Name { get; set; } = string.Empty;
    /// <summary>
    /// Gets or sets the last observed modification timestamp in UTC.
    /// </summary>
    public DateTime LastModifiedUtc { get; set; }

    /// <summary>
    /// Gets the fully qualified name suitable for logging and diagnostics.
    /// </summary>
    public string FullName => string.IsNullOrWhiteSpace(Schema) ? Name : $"{Schema}.{Name}";
}

internal sealed class SchemaObjectIndexManager : ISchemaObjectIndexManager
{
    private readonly object _sync = new();
    private readonly Dictionary<SchemaObjectType, Dictionary<string, SchemaObjectIndexEntry>> _entries = new();
    private bool _initialized;
    private bool _dirty;
    private string _indexDirectory = string.Empty;
    private string _indexFilePath = string.Empty;

    private static readonly JsonSerializerOptions SerializerOptions = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
        Converters = { new JsonStringEnumConverter() }
    };

    public Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        if (_initialized)
        {
            return Task.CompletedTask;
        }

        cancellationToken.ThrowIfCancellationRequested();

        var projectRoot = ProjectRootResolver.ResolveCurrent();
        _indexDirectory = Path.Combine(projectRoot, ".xtraq", "cache");
        _indexFilePath = Path.Combine(_indexDirectory, "object-index.json");
        Directory.CreateDirectory(_indexDirectory);

        if (File.Exists(_indexFilePath))
        {
            try
            {
                using var stream = File.OpenRead(_indexFilePath);
                var document = JsonSerializer.Deserialize<ObjectIndexDocument>(stream, SerializerOptions);
                if (document?.Entries != null)
                {
                    lock (_sync)
                    {
                        _entries.Clear();
                        foreach (var entry in document.Entries)
                        {
                            AddEntry(entry);
                        }
                    }
                }
            }
            catch
            {
                // Ignore read issues; a fresh index will be created during the next flush.
            }
        }

        _initialized = true;
        return Task.CompletedTask;
    }

    public IReadOnlyList<SchemaObjectIndexEntry> GetEntries(SchemaObjectType objectType)
    {
        lock (_sync)
        {
            if (!_entries.TryGetValue(objectType, out var map))
            {
                return Array.Empty<SchemaObjectIndexEntry>();
            }

            return map.Values.Select(Clone).ToArray();
        }
    }

    public void ReplaceEntries(SchemaObjectType objectType, IEnumerable<SchemaObjectIndexEntry> entries)
    {
        if (entries == null)
        {
            return;
        }

        lock (_sync)
        {
            _entries[objectType] = entries
                .Where(static e => !string.IsNullOrWhiteSpace(e.Name))
                .Select(Clone)
                .ToDictionary(static e => BuildKey(e.Schema, e.Name), StringComparer.OrdinalIgnoreCase);
            _dirty = true;
        }
    }

    public void UpsertEntries(IEnumerable<SchemaObjectIndexEntry> entries)
    {
        if (entries == null)
        {
            return;
        }

        lock (_sync)
        {
            foreach (var entry in entries)
            {
                if (entry == null || string.IsNullOrWhiteSpace(entry.Name))
                {
                    continue;
                }

                var key = BuildKey(entry.Schema, entry.Name);
                if (!_entries.TryGetValue(entry.Type, out var map))
                {
                    map = new Dictionary<string, SchemaObjectIndexEntry>(StringComparer.OrdinalIgnoreCase);
                    _entries[entry.Type] = map;
                }

                map[key] = Clone(entry);
                _dirty = true;
            }
        }
    }

    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_dirty)
        {
            return;
        }

        List<SchemaObjectIndexEntry> snapshot;
        lock (_sync)
        {
            snapshot = _entries
                .SelectMany(static pair => pair.Value.Values)
                .OrderBy(static entry => entry.Type)
                .ThenBy(static entry => entry.Schema, StringComparer.OrdinalIgnoreCase)
                .ThenBy(static entry => entry.Name, StringComparer.OrdinalIgnoreCase)
                .Select(Clone)
                .ToList();
            _dirty = false;
        }

        Directory.CreateDirectory(_indexDirectory);
        var document = new ObjectIndexDocument
        {
            Version = 1,
            LastUpdatedUtc = DateTime.UtcNow,
            Entries = snapshot
        };

        var tempFile = _indexFilePath + ".tmp";
        await using (var stream = File.Create(tempFile))
        {
            await JsonSerializer.SerializeAsync(stream, document, SerializerOptions, cancellationToken).ConfigureAwait(false);
        }

        if (File.Exists(_indexFilePath))
        {
            File.Replace(tempFile, _indexFilePath, null);
        }
        else
        {
            File.Move(tempFile, _indexFilePath);
        }
    }

    private void AddEntry(SchemaObjectIndexEntry entry)
    {
        if (entry == null || string.IsNullOrWhiteSpace(entry.Name))
        {
            return;
        }

        var key = BuildKey(entry.Schema, entry.Name);
        if (!_entries.TryGetValue(entry.Type, out var map))
        {
            map = new Dictionary<string, SchemaObjectIndexEntry>(StringComparer.OrdinalIgnoreCase);
            _entries[entry.Type] = map;
        }

        map[key] = Clone(entry);
    }

    private static SchemaObjectIndexEntry Clone(SchemaObjectIndexEntry entry) => new()
    {
        Type = entry.Type,
        Schema = entry.Schema,
        Name = entry.Name,
        LastModifiedUtc = entry.LastModifiedUtc
    };

    private static string BuildKey(string schema, string name)
    {
        return string.IsNullOrWhiteSpace(schema) ? name : $"{schema}.{name}";
    }

    private sealed class ObjectIndexDocument
    {
        public int Version { get; set; }
        public DateTime LastUpdatedUtc { get; set; }
        public List<SchemaObjectIndexEntry> Entries { get; set; } = new();
    }
}
