using Xtraq.Configuration;
using Xtraq.Models;

namespace Xtraq.Services;

internal sealed class XtraqService
{
    public Version Version { get; }

    public string InformationalVersion { get; }

    public XtraqService()
    {
        var assembly = GetType().Assembly;
        Version = assembly.GetName().Version ?? new Version(0, 0);
        InformationalVersion = assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()?
            .InformationalVersion
            ?? Version.ToString();
    }

    public ConfigurationModel GetDefaultConfiguration(string? targetFramework = null, string appNamespace = "", string connectionString = "")
    {
        return new ConfigurationModel
        {
            Version = Version,
            TargetFramework = targetFramework ?? Constants.DefaultTargetFramework.ToFrameworkString(),
            Project = new ProjectModel
            {
                DataBase = new DataBaseModel
                {
                    // the default appsettings.json ConnectString Identifier
                    // you can customize this one later on in the xtraq.json
                    RuntimeConnectionStringIdentifier = "DefaultConnection",
                    ConnectionString = connectionString ?? ""
                },
                Output = new OutputModel
                {
                    Namespace = appNamespace,
                    DataContext = new DataContextModel
                    {
                        Path = "./DataContext",
                        Inputs = new DataContextInputsModel
                        {
                            Path = "./Inputs",
                        },
                        Outputs = new DataContextOutputsModel
                        {
                            Path = "./Outputs",
                        },
                        Models = new DataContextModelsModel
                        {
                            Path = "./Models",
                        },
                        TableTypes = new DataContextTableTypesModel
                        {
                            Path = "./TableTypes",
                        },
                        StoredProcedures = new DataContextStoredProceduresModel
                        {
                            Path = "./StoredProcedures",
                        }
                    }
                }
            },
            Schema = []
        };
    }
}
