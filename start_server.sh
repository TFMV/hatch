#!/bin/bash

# Exit on error
set -e

# Default configuration
ADDRESS="0.0.0.0:32010"
DATABASE=":memory:"
LOG_LEVEL="info"
MAX_CONNECTIONS=100
CONNECTION_TIMEOUT="30s"
QUERY_TIMEOUT="5m"
MAX_MESSAGE_SIZE=16777216  # 16MB
METRICS_ENABLED=true
METRICS_ADDRESS=":9090"
HEALTH_ENABLED=true
REFLECTION_ENABLED=true
SHUTDOWN_TIMEOUT="30s"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --address)
      ADDRESS="$2"
      shift 2
      ;;
    --database)
      DATABASE="$2"
      shift 2
      ;;
    --log-level)
      LOG_LEVEL="$2"
      shift 2
      ;;
    --max-connections)
      MAX_CONNECTIONS="$2"
      shift 2
      ;;
    --connection-timeout)
      CONNECTION_TIMEOUT="$2"
      shift 2
      ;;
    --query-timeout)
      QUERY_TIMEOUT="$2"
      shift 2
      ;;
    --max-message-size)
      MAX_MESSAGE_SIZE="$2"
      shift 2
      ;;
    --metrics-enabled)
      METRICS_ENABLED="$2"
      shift 2
      ;;
    --metrics-address)
      METRICS_ADDRESS="$2"
      shift 2
      ;;
    --health-enabled)
      HEALTH_ENABLED="$2"
      shift 2
      ;;
    --reflection-enabled)
      REFLECTION_ENABLED="$2"
      shift 2
      ;;
    --shutdown-timeout)
      SHUTDOWN_TIMEOUT="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Print configuration
echo "Starting Porter Flight SQL Server with configuration:"
echo "Address: $ADDRESS"
echo "Database: $DATABASE"
echo "Log Level: $LOG_LEVEL"
echo "Max Connections: $MAX_CONNECTIONS"
echo "Connection Timeout: $CONNECTION_TIMEOUT"
echo "Query Timeout: $QUERY_TIMEOUT"
echo "Max Message Size: $MAX_MESSAGE_SIZE"
echo "Metrics Enabled: $METRICS_ENABLED"
echo "Metrics Address: $METRICS_ADDRESS"
echo "Health Enabled: $HEALTH_ENABLED"
echo "Reflection Enabled: $REFLECTION_ENABLED"
echo "Shutdown Timeout: $SHUTDOWN_TIMEOUT"

# Build command with conditional boolean flags
CMD="porter serve \
  --address \"$ADDRESS\" \
  --database \"$DATABASE\" \
  --log-level \"$LOG_LEVEL\" \
  --max-connections \"$MAX_CONNECTIONS\" \
  --connection-timeout \"$CONNECTION_TIMEOUT\" \
  --query-timeout \"$QUERY_TIMEOUT\" \
  --max-message-size \"$MAX_MESSAGE_SIZE\" \
  --metrics-address \"$METRICS_ADDRESS\" \
  --shutdown-timeout \"$SHUTDOWN_TIMEOUT\""

# Add boolean flags conditionally
if [ "$METRICS_ENABLED" = "true" ]; then
  CMD="$CMD --metrics"
fi

if [ "$HEALTH_ENABLED" = "true" ]; then
  CMD="$CMD --health"
fi

if [ "$REFLECTION_ENABLED" = "true" ]; then
  CMD="$CMD --reflection"
fi

# Execute the command
eval $CMD 