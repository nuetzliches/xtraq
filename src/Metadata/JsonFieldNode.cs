
namespace Xtraq.Metadata;

/// <summary>
/// Represents a node in a JSON payload graph derived from snapshot metadata. Each node corresponds to a property path
/// and captures whether the node materializes as an array as well as its nested structure.
/// </summary>
public sealed record JsonFieldNode(
    string Name,
    string Path,
    bool IsArray,
    IReadOnlyList<JsonFieldNode> Children
);
