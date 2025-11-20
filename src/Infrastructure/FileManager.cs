using System.Text.Json.Serialization;
using Xtraq.Extensions;
using Xtraq.Services;
using Xtraq.Utils;

namespace Xtraq.Infrastructure;

internal interface IFileManager<TConfig> where TConfig : class, IVersioned
{
    TConfig Config { get; }

    Task InitializeAsync(CancellationToken cancellationToken = default);

    Task ReloadAsync(CancellationToken cancellationToken = default);
}

internal sealed class FileManager<TConfig>(
    XtraqService xtraq,
    string fileName,
    TConfig? defaultConfig = default
) : IFileManager<TConfig>, IDisposable where TConfig : class, IVersioned
{
    private readonly SemaphoreSlim _configGate = new(1, 1);
    private readonly string _fullPath = DirectoryUtils.GetWorkingDirectory(fileName);
    private readonly CancellationTokenSource _reloadCts = new();
    private Task? _initializationTask;
    private FileSystemWatcher? _watcher;

    public TConfig? DefaultConfig
    {
        get => defaultConfig;
        set
        {
            defaultConfig = value;
            ScheduleReload();
        }
    }

    private TConfig? _config;
    public TConfig Config => _config ?? throw new InvalidOperationException($"Configuration '{fileName}' has not been initialized. Call InitializeAsync before accessing it.");

    private TConfig? _overwriteWithConfig;
    public TConfig? OverwriteWithConfig
    {
        get => _overwriteWithConfig;
        set
        {
            _overwriteWithConfig = value;
            ScheduleReload();
        }
    }

    private JsonSerializerOptions? _deserializerOptions;
    private JsonSerializerOptions DeserializerOptions
    {
        get => _deserializerOptions ??= new JsonSerializerOptions
        {
            Converters =
            {
                new JsonStringEnumConverter(JsonNamingPolicy.CamelCase)
            }
        };
    }

    private JsonSerializerOptions? _serializerOptions;
    private JsonSerializerOptions SerializerOptions
    {
        get => _serializerOptions ??= new JsonSerializerOptions()
        {
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
            WriteIndented = true,
            Converters = {
                new JsonStringEnumConverter()
            }
        };
    }

    public bool Exists()
    {
        return File.Exists(_fullPath);
    }

    public async Task InitializeAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var task = _initializationTask;
        if (task != null)
        {
            await task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        task = InitializeInternalAsync(cancellationToken);
        if (Interlocked.CompareExchange(ref _initializationTask, task, null) is { } existing)
        {
            task = existing;
        }

        await task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task InitializeInternalAsync(CancellationToken cancellationToken)
    {
        await ReloadAsync(cancellationToken).ConfigureAwait(false);
        StartWatcher();
    }

    public async Task ReloadAsync(CancellationToken cancellationToken = default)
    {
        await _configGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var config = await ReadAsync(cancellationToken).ConfigureAwait(false);
            _config = MergeConfigurations(config);
        }
        finally
        {
            _configGate.Release();
        }
    }

    private async Task<TConfig?> ReadAsync(CancellationToken cancellationToken)
    {
        if (!Exists())
        {
            return DefaultConfig;
        }

        await using var stream = new FileStream(_fullPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite, 4096, useAsync: true);
        using var reader = new StreamReader(stream, Encoding.UTF8);
        var content = await reader.ReadToEndAsync(cancellationToken).ConfigureAwait(false);
        var config = JsonSerializer.Deserialize<TConfig>(content, DeserializerOptions);
        return config ?? DefaultConfig;
    }

    private TConfig? MergeConfigurations(TConfig? source)
    {
        var baseConfig = DefaultConfig == null
            ? source
            : DefaultConfig.OverwriteWith(source ?? DefaultConfig);

        if (OverwriteWithConfig == null)
        {
            return baseConfig;
        }

        return baseConfig == null
            ? OverwriteWithConfig
            : baseConfig.OverwriteWith(OverwriteWithConfig);
    }

    public async Task SaveAsync(TConfig config, CancellationToken cancellationToken = default)
    {
        config.Version = xtraq.Version;

        var json = JsonSerializer.Serialize(config, SerializerOptions);
        var directory = Path.GetDirectoryName(_fullPath);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await File.WriteAllTextAsync(_fullPath, json, cancellationToken).ConfigureAwait(false);
        ScheduleReload();
    }

    private void StartWatcher()
    {
        if (_watcher != null)
        {
            return;
        }

        var directory = Path.GetDirectoryName(_fullPath);
        var file = Path.GetFileName(_fullPath);
        if (string.IsNullOrEmpty(directory) || string.IsNullOrEmpty(file))
        {
            return;
        }

        _watcher = new FileSystemWatcher(directory, file)
        {
            NotifyFilter = NotifyFilters.LastWrite | NotifyFilters.FileName | NotifyFilters.Size | NotifyFilters.CreationTime
        };

        _watcher.Changed += HandleConfigFileChanged;
        _watcher.Created += HandleConfigFileChanged;
        _watcher.Deleted += HandleConfigFileChanged;
        _watcher.Renamed += HandleConfigFileChanged;
        _watcher.EnableRaisingEvents = true;
    }

    private void HandleConfigFileChanged(object sender, FileSystemEventArgs e)
    {
        ScheduleReload();
    }

    private void ScheduleReload()
    {
        if (_initializationTask == null || _reloadCts.IsCancellationRequested)
        {
            return;
        }

        _ = Task.Run(async () =>
        {
            try
            {
                await ReloadAsync(_reloadCts.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
            }
            catch
            {
                // Swallow to avoid surfacing background reload exceptions in callers.
            }
        });
    }

    public void Dispose()
    {
        _reloadCts.Cancel();
        _watcher?.Dispose();
        _configGate.Dispose();
    }

}

