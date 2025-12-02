using Xtraq.Configuration;
using Xtraq.Engine;

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
        return Configuration?.ResultSetJsonIncludeNullValues == true;
    }

    protected bool ShouldEmitApiIntegrations()
    {
        return Configuration?.ApiEnabled == true;
    }

    protected bool ShouldEmitEntityFrameworkIntegration()
    {
        return Configuration?.EntityFrameworkEnabled == true;
    }

}
