FROM ubuntu:focal

ENV DEBIAN_FRONTEND=noninteractive \
    ACCEPT_EULA=Y \
    MSSQL_PID=Developer \
    PATH="${PATH}:/opt/mssql-tools/bin:/opt/mssql-tools18/bin"

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends curl ca-certificates apt-transport-https gnupg software-properties-common locales; \
    locale-gen en_US.UTF-8; \
    update-locale LANG=en_US.UTF-8; \
    curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | apt-key add -; \
    curl -fsSL https://packages.microsoft.com/config/ubuntu/20.04/packages-microsoft-prod.deb -o /tmp/packages-microsoft-prod.deb; \
    dpkg -i /tmp/packages-microsoft-prod.deb; \
    rm /tmp/packages-microsoft-prod.deb; \
    curl -fsSL https://packages.microsoft.com/config/ubuntu/20.04/mssql-server-2022.list | tee /etc/apt/sources.list.d/mssql-server-2022.list > /dev/null; \
    apt-get update; \
    apt-get install -y mssql-server; \
    apt-get install -y mssql-server-fts; \
    ACCEPT_EULA=Y apt-get install -y mssql-tools18 unixodbc-dev; \
    ln -s /opt/mssql-tools18 /opt/mssql-tools; \
    apt-get clean; \
    rm -rf /var/lib/apt/lists/*

RUN useradd -r -u 10001 -g root mssql; \
    mkdir -p /var/opt/mssql /var/opt/mssql/log; \
    chown -R mssql:root /var/opt/mssql

COPY --chown=mssql:root scripts/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 1433
USER mssql
ENV MSSQL_ENABLE_HADR=0
ENTRYPOINT ["/entrypoint.sh"]
CMD ["bash", "-c", "tail -f /var/opt/mssql/log/startup.log"]
