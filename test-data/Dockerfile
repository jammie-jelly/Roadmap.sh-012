FROM ubuntu:24.04

RUN apt-get update && \
    apt-get install -y \
    postgresql-16 postgresql-contrib \
    mysql-server \
    sqlite3 \
    faker \
    python3 && \
    apt-get clean

# Mongodb
RUN apt-get update && apt-get install -y gnupg curl && \
    curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | gpg --dearmor -o /usr/share/keyrings/mongodb-server-8.0.gpg && \
    echo "deb [ arch=amd64,arm64 signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] https://repo.mongodb.org/apt/ubuntu noble/mongodb-org/8.0 multiverse" > /etc/apt/sources.list.d/mongodb-org-8.0.list && \
    apt-get update && \
    apt-get install -y mongodb-org && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /data

COPY init.sh /init.sh
RUN chmod +x /init.sh

EXPOSE 5432 3306 27017

CMD ["/init.sh"]
