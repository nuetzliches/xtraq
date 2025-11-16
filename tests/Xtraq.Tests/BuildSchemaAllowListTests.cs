using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Xtraq.Metadata;
using Xunit;

namespace Xtraq.Tests.Runtime;

/// <summary>
/// Validates that build-time schema allow lists constrain table type dependencies as expected.
/// </summary>
public sealed class BuildSchemaAllowListTests
{
    [Fact]
    public void CollectRequiredTableTypeReferences_WhenSchemaAllowListApplied_IncludesOnlyMatchingProcedures()
    {
        var procedures = new List<ProcedureDescriptor>
        {
            new(
                ProcedureName: "SyncUserContacts",
                Schema: "sample",
                OperationName: "SyncUserContacts",
                InputParameters: Array.Empty<FieldDescriptor>(),
                OutputFields: Array.Empty<FieldDescriptor>(),
                ResultSets: Array.Empty<ResultSetDescriptor>(),
                TableTypeParameters: new[]
                {
                    new TableTypeParameterDescriptor(
                        ParameterName: "Contacts",
                        TableTypeSchema: "sample",
                        TableTypeName: "UserContactTableType",
                        NormalizedTypeReference: "sample.UserContactTableType")
                }
            ),
            new(
                ProcedureName: "WriteAuditLogEntries",
                Schema: "sample",
                OperationName: "WriteAuditLogEntries",
                InputParameters: Array.Empty<FieldDescriptor>(),
                OutputFields: Array.Empty<FieldDescriptor>(),
                ResultSets: Array.Empty<ResultSetDescriptor>(),
                TableTypeParameters: new[]
                {
                    new TableTypeParameterDescriptor(
                        ParameterName: "Entries",
                        TableTypeSchema: "shared",
                        TableTypeName: "AuditLogEntryTableType",
                        NormalizedTypeReference: "shared.AuditLogEntryTableType")
                }
            ),
            new(
                ProcedureName: "AdminOnly",
                Schema: "admin",
                OperationName: "AdminOnly",
                InputParameters: Array.Empty<FieldDescriptor>(),
                OutputFields: Array.Empty<FieldDescriptor>(),
                ResultSets: Array.Empty<ResultSetDescriptor>(),
                TableTypeParameters: new[]
                {
                    new TableTypeParameterDescriptor(
                        ParameterName: "Payload",
                        TableTypeSchema: "admin",
                        TableTypeName: "AdminAuditTableType",
                        NormalizedTypeReference: "admin.AdminAuditTableType")
                }
            )
        };

        var allowList = new[] { "sample" };

        var method = typeof(Xtraq.Runtime.XtraqCliRuntime)
            .GetMethod("CollectRequiredTableTypeReferences", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(method);

        var result = method!.Invoke(null, new object?[] { procedures, allowList })
            as HashSet<string> ?? throw new InvalidOperationException("Invocation returned null");

        var normalized = result.OrderBy(static entry => entry, StringComparer.OrdinalIgnoreCase).ToArray();

        Assert.Equal(new[]
        {
            "sample.UserContactTableType",
            "shared.AuditLogEntryTableType"
        }, normalized);
    }
}
