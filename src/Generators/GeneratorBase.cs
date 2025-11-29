using Xtraq.Configuration;
using Xtraq.Engine;
using Xtraq.Utils;

namespace Xtraq.Generators;

/// <summary>
/// Provides shared infrastructure for generators (template coordination and environment configuration helpers).
/// </summary>
internal abstract class GeneratorBase
{
    protected GeneratorBase(ITemplateRenderer renderer, ITemplateLoader? loader, XtraqConfiguration? configuration)
    {
        Templates = new TemplateCoordinator(renderer, loader);
        Configuration = configuration;
    }

    protected TemplateCoordinator Templates { get; }

    protected XtraqConfiguration? Configuration { get; }

    protected bool ShouldEmitJsonIncludeNullValues()
    {
        if (Configuration?.ResultSetJsonIncludeNullValues == true)
        {
            return true;
        }

        return EnvironmentHelper.IsTrue("XTRAQ_RESULTSET_JSON_INCLUDE_NULL_VALUES");
    }

    protected bool ShouldEmitMinimalApiExtensions()
    {
        if (Configuration?.ApiMode == ApiMode.Minimal)
        {
            return true;
        }

        return EnvironmentHelper.IsTrue("XTRAQ_API_MODE_MINIMAL") || string.Equals(Environment.GetEnvironmentVariable("XTRAQ_API_MODE"), "minimal", StringComparison.OrdinalIgnoreCase);
    }

    protected bool ShouldEmitEntityFrameworkIntegration()
    {
        if (Configuration?.EntityFrameworkEnabled == true)
        {
            return true;
        }

        return EnvironmentHelper.IsTrue("XTRAQ_ENTITY_FRAMEWORK_ENABLED");
    }

}
