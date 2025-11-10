---
title: update
description: Update the installed xtraq global tool to the latest available version.
versionIntroduced: 1.0.0-alpha
experimental: false
authoritative: true
authors: [core]
aiTags: [cli, update]
---

# update

Installs the latest published xtraq global tool package on the current machine. When invoked from CI this command can keep runner environments current before a build.

## Usage

```bash
xtraq update [options]
```

## Options

`xtraq update` reuses global switches documented on the [CLI overview](../index.md):

- `-v, --verbose` prints the feed URL and the executed `dotnet tool update` command.
- `--ci` suppresses Spectre.Console decorations, producing plain text suitable for log files.

> `update` ignores project-path arguments because the tool update always targets the global installation.

## Behavior Contract (Draft)

```json
{
  "command": "update",
  "reads": [],
  "writes": [],
  "exitCodes": {
    "0": "Success",
    "1": "UpdateFailed"
  }
}
```

## Examples

```bash
# Update with default output
xtraq update

# Include verbose diagnostics for troubleshooting
xtraq update --verbose
```
