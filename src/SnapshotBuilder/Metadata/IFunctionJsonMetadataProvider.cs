
namespace Xtraq.SnapshotBuilder.Metadata;

internal interface IFunctionJsonMetadataProvider
{
    Task<FunctionJsonMetadata?> ResolveAsync(string? schema, string name, CancellationToken cancellationToken);
}

internal sealed class FunctionJsonMetadata
{
    public FunctionJsonMetadata(bool returnsJson, bool returnsJsonArray, string? rootProperty)
    {
        ReturnsJson = returnsJson;
        ReturnsJsonArray = returnsJsonArray;
        RootProperty = rootProperty;
    }

    public bool ReturnsJson { get; }
    public bool ReturnsJsonArray { get; }
    public string? RootProperty { get; }
}
