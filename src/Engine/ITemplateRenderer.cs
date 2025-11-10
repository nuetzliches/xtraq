namespace Xtraq.Engine;

/// <summary>
/// Defines the contract used to transform template text into generated content.
/// </summary>
public interface ITemplateRenderer
{
    /// <summary>
    /// Renders a template with the provided model.
    /// </summary>
    /// <param name="template">Template string (may contain placeholders).</param>
    /// <param name="model">Arbitrary model (anonymous or strongly typed object) – optional.</param>
    /// <returns>Rendered text.</returns>
    string Render(string template, object? model);
}
