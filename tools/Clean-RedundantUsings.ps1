# Clean-RedundantUsings.ps1
# Script to automatically remove redundant using statements that are already in GlobalUsings.cs

param(
    [string]$ProjectPath = "src/",
    [switch]$DryRun = $false,
    [switch]$Verbose = $false
)

# Define global usings that should be removed from individual files
$GlobalUsings = @(
    "using System;",
    "using System.Collections.Generic;",
    "using System.IO;", 
    "using System.Linq;",
    "using System.Threading;",
    "using System.Threading.Tasks;",
    "using System.Text;",
    "using System.Text.Json;",
    "using System.Text.RegularExpressions;",
    "using System.Collections.Concurrent;",
    "using System.Diagnostics;",
    "using System.Globalization;",
    "using System.Reflection;",
    "using System.Security.Cryptography;",
    "using Microsoft.Extensions.DependencyInjection;"
)

function Remove-RedundantUsings {
    param([string]$FilePath)
    
    $content = Get-Content $FilePath -Raw
    $originalContent = $content
    $removedUsings = @()
    
    foreach ($globalUsing in $GlobalUsings) {
        $pattern = [regex]::Escape($globalUsing)
        if ($content -match $pattern) {
            $content = $content -replace "$pattern\r?\n?", ""
            $removedUsings += $globalUsing
            if ($Verbose) {
                Write-Host "  - Removed: $globalUsing" -ForegroundColor Yellow
            }
        }
    }
    
    # Clean up extra blank lines
    $content = $content -replace "(\r?\n){3,}", "`r`n`r`n"
    
    if ($content -ne $originalContent) {
        if (!$DryRun) {
            Set-Content $FilePath $content -NoNewline
        }
        return $removedUsings.Count
    }
    
    return 0
}

Write-Host "Cleaning redundant using statements..." -ForegroundColor Cyan

if ($DryRun) {
    Write-Host "DRY RUN MODE - No files will be modified" -ForegroundColor Yellow
}

$csFiles = Get-ChildItem -Path $ProjectPath -Filter "*.cs" -Recurse | Where-Object { 
    $_.Name -ne "GlobalUsings.cs" -and $_.Name -notlike "*.GlobalUsings.g.cs" -and $_.Name -notlike "*.AssemblyInfo.cs" -and $_.Name -notlike ".NETCoreApp,Version=*.AssemblyAttributes.cs"
}

$totalFilesProcessed = 0
$totalUsingsRemoved = 0
$filesModified = 0

foreach ($file in $csFiles) {
    $totalFilesProcessed++
    
    if ($Verbose) {
        Write-Host "Processing: $($file.FullName)" -ForegroundColor Gray
    }
    
    $removedCount = Remove-RedundantUsings -FilePath $file.FullName
    
    if ($removedCount -gt 0) {
        $filesModified++
        $totalUsingsRemoved += $removedCount
        Write-Host "OK $($file.Name): Removed $removedCount redundant using(s)" -ForegroundColor Green
    }
}

Write-Host ""
Write-Host "Summary:" -ForegroundColor Cyan
Write-Host "  Files processed: $totalFilesProcessed"
Write-Host "  Files modified: $filesModified"
Write-Host "  Total usings removed: $totalUsingsRemoved"

if ($DryRun -and $totalUsingsRemoved -gt 0) {
    Write-Host ""
    Write-Host "Run without -DryRun to apply changes" -ForegroundColor Yellow
}

# Run dotnet format to organize remaining usings
if (!$DryRun -and $filesModified -gt 0) {
    Write-Host ""
    Write-Host "Running dotnet format to organize remaining usings..." -ForegroundColor Cyan
    
    try {
        $formatResult = dotnet format $ProjectPath --include "*.cs" --verbosity minimal 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Code formatting completed successfully" -ForegroundColor Green
        } else {
            Write-Host "WARNING: Code formatting completed with warnings" -ForegroundColor Yellow
        }
    }
    catch {
        Write-Host "ERROR: Error running dotnet format: $_" -ForegroundColor Red
    }
}