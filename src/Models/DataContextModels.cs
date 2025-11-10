namespace Xtraq.Models;

internal sealed class DataContextModel : IDirectoryModel
{
    public string Path { get; set; } = string.Empty;
    public DataContextInputsModel Inputs { get; set; } = new();
    public DataContextOutputsModel Outputs { get; set; } = new();
    public DataContextModelsModel Models { get; set; } = new();
    public DataContextStoredProceduresModel StoredProcedures { get; set; } = new();
    public DataContextTableTypesModel TableTypes { get; set; } = new();
}

internal sealed class DataContextInputsModel : IDirectoryModel
{
    public string Path { get; set; } = string.Empty;
}

internal sealed class DataContextOutputsModel : IDirectoryModel
{
    public string Path { get; set; } = string.Empty;
}

internal sealed class DataContextModelsModel : IDirectoryModel
{
    public string Path { get; set; } = string.Empty;
}

internal sealed class DataContextTableTypesModel : IDirectoryModel
{
    public string Path { get; set; } = string.Empty;
}

internal sealed class DataContextStoredProceduresModel : IDirectoryModel
{
    public string Path { get; set; } = string.Empty;
}

internal interface IDirectoryModel
{
    string Path { get; set; }
}
