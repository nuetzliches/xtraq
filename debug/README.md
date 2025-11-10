```bash
# snapshot
dotnet run --project src/Xtraq.csproj --framework net8.0 -- snapshot -p debug
# build
dotnet run --project src/Xtraq.csproj --framework net8.0 -- build -p debug
# build --refresh-snapshot
dotnet run --project src/Xtraq.csproj --framework net8.0 -- build -p debug --refresh-snapshot

# Ohne Cache: --no-cache
```

SQL-Referenzen und weitere Infos in `README.LocalOnly.md`
