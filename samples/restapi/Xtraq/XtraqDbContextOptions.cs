
#nullable enable
namespace Xtraq.Samples.RestApi.Xtraq;

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

public sealed class XtraqDbContextOptions
{
    public string? ConnectionString { get; set; }
    /// <summary>Optional logical name (IConfiguration.GetConnectionString) â€“ defaults to "DefaultConnection".</summary>
    public string? ConnectionStringName { get; set; }
    /// <summary>Command execution timeout in seconds; null or &lt;= 0 falls back to default (30).</summary>
    public int? CommandTimeout { get; set; }
    /// <summary>Max retry attempts for transient open failures (SqlException); null/0 disables retry.</summary>
    public int? MaxOpenRetries { get; set; }
    /// <summary>Delay in milliseconds between retries (linear backoff). Default 200ms if retries enabled.</summary>
    public int? RetryDelayMs { get; set; }
    public JsonSerializerOptions? JsonSerializerOptions { get; set; }
    public bool EnableDiagnostics { get; set; } = true;
    /// <summary>Optional factory used to create the scoped transaction orchestrator instance.</summary>
    public Func<IServiceProvider, IXtraqTransactionOrchestrator>? TransactionOrchestratorFactory { get; set; }
    /// <summary>Ambient parameter bindings (scalar + table types) resolved from DI/host context.</summary>
    public ParameterBindingOptions ParameterBindings { get; } = new();
    /// <summary>Session-level SQL settings applied immediately after opening a connection.</summary>
    public SqlSessionSettings SessionSettings { get; } = SqlSessionSettings.SqlServerRecommended();
}

/// <summary>Fluent builder for SQL Server session flags applied after opening connections.</summary>
public sealed class SqlSessionSettings
{
    private readonly List<string> _commands = new();

    /// <summary>Returns an empty settings object (no commands).</summary>
    public static SqlSessionSettings Empty() => new();

    /// <summary>Returns the recommended baseline for SQL Server (quoted identifiers, ANSI behaviors, ARITHABORT, NUMERIC_ROUNDABORT OFF).</summary>
    public static SqlSessionSettings SqlServerRecommended() => Empty()
        .WithQuotedIdentifierOn()
        .WithAnsiNullsOn()
        .WithAnsiPaddingOn()
        .WithAnsiWarningsOn()
        .WithConcatNullYieldsNull()
        .WithArithAbortOn()
        .WithNoCountOn()
        .WithNumericRoundAbortOff();

    public bool HasSettings => _commands.Count > 0;

    public SqlSessionSettings Clear()
    {
        _commands.Clear();
        return this;
    }

    public SqlSessionSettings WithQuotedIdentifierOn() => Add("SET QUOTED_IDENTIFIER ON;");
    public SqlSessionSettings WithAnsiNullsOn() => Add("SET ANSI_NULLS ON;");
    public SqlSessionSettings WithAnsiPaddingOn() => Add("SET ANSI_PADDING ON;");
    public SqlSessionSettings WithAnsiWarningsOn() => Add("SET ANSI_WARNINGS ON;");
    public SqlSessionSettings WithAnsiWarningOn() => WithAnsiWarningsOn(); // alias
    public SqlSessionSettings WithConcatNullYieldsNull() => Add("SET CONCAT_NULL_YIELDS_NULL ON;");
    public SqlSessionSettings WithConcatNullYieldsNullOn() => WithConcatNullYieldsNull(); // alias
    public SqlSessionSettings WithArithAbortOn() => Add("SET ARITHABORT ON;");
    public SqlSessionSettings WithArithabort() => WithArithAbortOn(); // alias
    public SqlSessionSettings WithNumericRoundAbortOff() => Add("SET NUMERIC_ROUNDABORT OFF;");
    public SqlSessionSettings WithNoCountOn() => Add("SET NOCOUNT ON;");

    public void Apply(DbConnection connection)
    {
        ArgumentNullException.ThrowIfNull(connection);
        if (!HasSettings) return;
        if (connection is not Microsoft.Data.SqlClient.SqlConnection)
        {
            return;
        }
        if (connection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException("Connection must be open before applying session settings.");
        }

        using var cmd = connection.CreateCommand();
        cmd.CommandType = CommandType.Text;
        cmd.CommandText = string.Join('\n', _commands);
        cmd.ExecuteNonQuery();
    }

    public async Task ApplyAsync(DbConnection connection, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(connection);
        if (!HasSettings) return;
        if (connection is not Microsoft.Data.SqlClient.SqlConnection)
        {
            return;
        }
        if (connection.State != ConnectionState.Open)
        {
            throw new InvalidOperationException("Connection must be open before applying session settings.");
        }

        await using var cmd = connection.CreateCommand();
        cmd.CommandType = CommandType.Text;
        cmd.CommandText = string.Join('\n', _commands);
        await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private SqlSessionSettings Add(string command)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(command);
        foreach (var existing in _commands)
        {
            if (string.Equals(existing, command, StringComparison.OrdinalIgnoreCase))
            {
                return this;
            }
        }

        _commands.Add(command);
        return this;
    }
}
