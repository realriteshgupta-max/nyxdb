#!/bin/bash
# Script to run NyxFlightServerStarter using the fat JAR
# This avoids all classpath issues by bundling all dependencies

set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Build if needed
echo "📦 Building nyx-flight fat JAR..."
./mvnw -pl nyx-flight -am clean package -DskipTests -q

FAT_JAR="nyx-flight/target/nyx-flight-1.0-SNAPSHOT-fat.jar"

if [ ! -f "$FAT_JAR" ]; then
    echo "❌ Error: Fat JAR not found at $FAT_JAR"
    exit 1
fi

echo "✅ Running Flight Server from: $FAT_JAR"
echo "🚀 Server will start on localhost:8815"
echo ""
echo "Connection details:"
echo "  JDBC URL: jdbc:arrow-flight-sql://localhost:8815?useEncryption=false"
echo ""

# Run the server with required JVM flags
java \
    --enable-native-access=ALL-UNNAMED \
    --add-opens=java.base/java.nio=ALL-UNNAMED \
    --add-opens=jdk.unsupported/sun.misc=ALL-UNNAMED \
    -cp "$FAT_JAR" \
    org.nyxdb.flight.NyxFlightServerStarter
