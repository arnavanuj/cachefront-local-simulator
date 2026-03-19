#!/bin/sh
set -eu

CONNECT_URL="${CONNECT_URL:-http://debezium:8083}"
CONNECTOR_NAME="${CONNECTOR_NAME:-mysql-cachefront-connector}"

echo "Waiting for Debezium Connect at ${CONNECT_URL} ..."
until curl -fsS "${CONNECT_URL}/connectors" >/dev/null; do
  sleep 3
done

if curl -fsS "${CONNECT_URL}/connectors/${CONNECTOR_NAME}" >/dev/null 2>&1; then
  echo "Connector ${CONNECTOR_NAME} already exists"
  exit 0
fi

echo "Registering connector ${CONNECTOR_NAME}"
curl -fsS -X POST "${CONNECT_URL}/connectors" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "mysql-cachefront-connector",
    "config": {
      "connector.class": "io.debezium.connector.mysql.MySqlConnector",
      "database.hostname": "mysql",
      "database.port": "3306",
      "database.user": "debezium",
      "database.password": "dbz",
      "database.server.id": "5401",
      "topic.prefix": "cachefront",
      "database.include.list": "appdb",
      "table.include.list": "appdb.users",
      "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
      "schema.history.internal.kafka.topic": "schema-changes.cachefront",
      "include.schema.changes": "false",
      "snapshot.mode": "initial"
    }
  }'

echo
echo "Connector registration submitted"
