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
        if (Configuration?.EmitJsonIncludeNullValuesAttribute == true)
        {
            return true;
        }

        return EnvironmentHelper.IsTrue("XTRAQ_JSON_INCLUDE_NULL_VALUES");
    }

    protected bool ShouldEmitMinimalApiExtensions()
    {
        if (Configuration?.EnableMinimalApiExtensions == true)
        {
            return true;
        }

        return EnvironmentHelper.IsTrue("XTRAQ_MINIMAL_API");
    }

    protected bool ShouldEmitEntityFrameworkIntegration()
    {
        if (Configuration?.EnableEntityFrameworkIntegration == true)
        {
            return true;
        }

        return EnvironmentHelper.IsTrue("XTRAQ_ENTITY_FRAMEWORK");
    }
}
