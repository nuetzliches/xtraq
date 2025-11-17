
namespace Xtraq.Engine;

/// <summary>
/// ServiceCollection extensions for Xtraq templating components.
/// </summary>
public static class TemplatingServiceCollectionExtensions
{
    /// <summary>
    /// Registers the simple template engine and a file system loader.
    /// </summary>
    /// <param name="services">DI collection.</param>
    /// <param name="templateRoot">Directory containing *.xqt template files when available.</param>
    public static IServiceCollection AddXtraqTemplating(this IServiceCollection services, string templateRoot)
    {
        ArgumentNullException.ThrowIfNull(services);
        if (string.IsNullOrWhiteSpace(templateRoot))
        {
            throw new ArgumentException("Template root required", nameof(templateRoot));
        }

        services.AddSingleton<ITemplateRenderer, SimpleTemplateEngine>();
        services.AddSingleton<ITemplateLoader>(_ =>
        {
            if (Directory.Exists(templateRoot))
            {
                return new FileSystemTemplateLoader(templateRoot);
            }

            return new EmbeddedResourceTemplateLoader(typeof(TemplatingServiceCollectionExtensions).Assembly, "Xtraq.Templates.");
        });
        return services;
    }
}
