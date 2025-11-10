---
title: version
description: Display installed and available xtraq CLI versions.
versionIntroduced: 1.0.0-alpha
experimental: false
authoritative: true
authors: [core]
aiTags: [cli, version]
---

# version

Prints the currently installed xtraq tool version and checks for the latest published package.

## Usage

```bash
xtraq version [options]
```

Typical output during the alpha cycle:

```text
Version: 1.0.0-alpha
```

## Options

`xtraq version` does not define command-specific options. The global switches from the [CLI overview](../index.md) continue to apply:

- `-v, --verbose` emits additional diagnostic information (e.g., configured feeds).
- `--ci` forces plain-text output for build logs.

## Behavior Contract (Draft)

```json
{
  "command": "version",
  "reads": [],
  "writes": [],
  "exitCodes": {
    "0": "Success",
    "1": "VersionCheckFailed"
  }
}
```

## Examples

```bash
# Show local and remote versions (useful in CI)
xtraq version --ci

# Inspect diagnostic output while troubleshooting feed issues
xtraq version --verbose
```
