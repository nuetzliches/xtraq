namespace Xtraq.Samples.RestApi.Xtraq;

using System;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

public static class XtraqDbContextServiceCollectionExtensions
{
    /// <summary>Register generated XtraqDbContext and its options.
    /// Connection string precedence (runtime only):
    /// 1) options.ConnectionString (delegate provided)
    /// 2) IConfiguration.GetConnectionString("DefaultConnection")
    /// </summary>
    public static IServiceCollection AddXtraqDbContext(this IServiceCollection services, Action<XtraqDbContextOptions>? configure = null)
    {
        var explicitOptions = new XtraqDbContextOptions();
        configure?.Invoke(explicitOptions);

        services.AddSingleton(provider =>
        {
            var cfg = provider.GetService<IConfiguration>();
            var name = explicitOptions.ConnectionStringName ?? "DefaultConnection";
            var conn = explicitOptions.ConnectionString ?? cfg?.GetConnectionString(name);
            if (string.IsNullOrWhiteSpace(conn))
                throw new InvalidOperationException($"No connection string resolved for XtraqDbContext (options / IConfiguration:GetConnectionString('{name}')).");
            explicitOptions.ConnectionString = conn;
            if (explicitOptions.CommandTimeout is null or <= 0) explicitOptions.CommandTimeout = 30;
            if (explicitOptions.MaxOpenRetries is not null and < 0)
                throw new InvalidOperationException("MaxOpenRetries must be >= 0");
            if (explicitOptions.RetryDelayMs is not null and <= 0)
                throw new InvalidOperationException("RetryDelayMs must be > 0");
            if (explicitOptions.JsonSerializerOptions == null)
            {
                var jsonOpts = new System.Text.Json.JsonSerializerOptions
                {
                    PropertyNameCaseInsensitive = true,
                    DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull,
                    NumberHandling = System.Text.Json.Serialization.JsonNumberHandling.AllowReadingFromString
                };
                // Converters can be extended later (e.g. relaxed numeric handling)
                explicitOptions.JsonSerializerOptions = jsonOpts;
            }
            return explicitOptions;
        });

        services.AddScoped<IXtraqDbContext>(sp => new XtraqDbContext(sp.GetRequiredService<XtraqDbContextOptions>()));
        return services;
    }
}

