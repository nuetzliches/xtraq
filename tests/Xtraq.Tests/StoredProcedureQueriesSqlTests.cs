using System.Text.Json;
using Xtraq.Data.Models;
using Xtraq.SnapshotBuilder.Writers;
using Xunit;

namespace Xtraq.Tests;

public class StoredProcedureQueriesSqlTests
{
    [Fact]
    public void ProcedureSnapshot_MarksDefaultedParametersAsNullable()
    {
        var descriptor = new Xtraq.SnapshotBuilder.Models.ProcedureDescriptor { Schema = "sample", Name = "DefaultsProbe" };
        var inputs = new[]
        {
            new StoredProcedureInput
            {
                name = "@RecentPaymentCount",
                is_nullable = 0,
                has_default_value = 1,
                system_type_name = "int"
            }
        };

        var payload = ProcedureSnapshotDocumentBuilder.BuildProcedureJson(
            descriptor,
            inputs,
            procedure: null,
            requiredTypeRefs: null,
            requiredTableTypeRefs: null,
            requiredTableRefs: null,
            jsonEnhancementService: null);

        using var doc = JsonDocument.Parse(payload);
        var param = doc.RootElement.GetProperty("Parameters")[0];
        Assert.True(param.GetProperty("IsNullable").GetBoolean());
        Assert.True(param.GetProperty("HasDefaultValue").GetBoolean());
    }
}
