---
title: Installation
description: Installing xtraq and basic requirements.
---

# Installation

## Prerequisites

- .NET SDK (6.0 or higher, recommended 8.0+)
- Access to SQL Server instance
- Git (optional for project integration)

## Global Installation

```bash
dotnet tool install --global xtraq
```

Update:

```bash
dotnet tool update --global xtraq
```

Check version:

```bash
xtraq version
```

## Local (project-bound) Installation

```bash
dotnet new tool-manifest
dotnet tool install xtraq
```

Execute (local):

```bash
dotnet tool run xtraq version
```

## Next Step

Continue to [Quickstart](/getting-started/quickstart).
