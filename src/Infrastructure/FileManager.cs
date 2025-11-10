using System.Text.Json.Serialization;
using Xtraq.Extensions;
using Xtraq.Services;
using Xtraq.Utils;

namespace Xtraq.Infrastructure;

internal interface IFileManager<TConfig> where TConfig : class, IVersioned
{
    TConfig Config { get; }
}

internal sealed class FileManager<TConfig>(
    XtraqService xtraq,
    string fileName,
    TConfig? defaultConfig = default
) : IFileManager<TConfig> where TConfig : class, IVersioned
{
    public TConfig? DefaultConfig
    {
        get => defaultConfig;
        set => defaultConfig = value;
    }

    private TConfig? _config;
    private TConfig? _overwritenWithConfig;
    public TConfig Config
    {
        get
        {
            if (_config == null || _overwritenWithConfig != OverwriteWithConfig)
            {
                var config = ReadAsync().GetAwaiter().GetResult();

                if (DefaultConfig == null)
                {
                    _config = config;
                }
                else
                {
                    var source = config ?? DefaultConfig;
                    _config = DefaultConfig.OverwriteWith(source);
                }

                if (OverwriteWithConfig != null)
                {
                    _config = _config == null
                        ? OverwriteWithConfig
                        : _config.OverwriteWith(OverwriteWithConfig);
                }
                _overwritenWithConfig = OverwriteWithConfig;
            }
            return _config ?? throw new InvalidOperationException($"Unable to load configuration '{fileName}'. Provide a default config or ensure the file exists.");
        }
        set => _config = value;
    }

    private TConfig? _overwriteWithConfig;
    public TConfig? OverwriteWithConfig
    {
        get => _overwriteWithConfig;
        set => _overwriteWithConfig = value;
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
        var path = DirectoryUtils.GetWorkingDirectory(fileName);
        return File.Exists(path);
    }

    public async Task<TConfig?> ReadAsync()
    {
        if (!Exists())
        {
            return DefaultConfig;
        }
        var path = DirectoryUtils.GetWorkingDirectory(fileName);
        var content = await File.ReadAllTextAsync(path);

        var config = JsonSerializer.Deserialize<TConfig>(content, DeserializerOptions);

        return config ?? DefaultConfig;
    }

    public async Task SaveAsync(TConfig config)
    {
        config.Version = xtraq.Version;

        var json = JsonSerializer.Serialize(config, SerializerOptions);
        var path = DirectoryUtils.GetWorkingDirectory(fileName);
        var directory = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(directory))
        {
            Directory.CreateDirectory(directory);
        }
        await File.WriteAllTextAsync(path, json);
    }

}

