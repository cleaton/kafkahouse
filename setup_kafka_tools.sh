#!/usr/bin/env bash

set -euo pipefail

# Script to download and setup Kafka tools for local use
KAFKA_VERSION="3.7.2"
SCALA_VERSION="2.13"
KAFKA_DIR="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
KAFKA_TGZ="${KAFKA_DIR}.tgz"
# SHA512 sum from https://downloads.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}.sha512
EXPECTED_SHA="65c09fbe8c78098b1efb26632a35d90ee709327a7aab13b37b2023692e1649105714ea513253a19f2cb685dc2c0c3837f32e39e9fb1d8d2367fe4650e5ad3cdc"

# Create deps directory if it doesn't exist
DEPS_DIR="deps"
mkdir -p "${DEPS_DIR}"
cd "${DEPS_DIR}"

echo "Setting up Kafka tools..."
echo "- Version: ${KAFKA_VERSION}"
echo "- Scala Version: ${SCALA_VERSION}"
echo

# Download Kafka binary distribution if not already present
if [ ! -f "${KAFKA_TGZ}" ]; then
    echo "Downloading Kafka ${KAFKA_VERSION}..."
    wget "https://dlcdn.apache.org/kafka/${KAFKA_VERSION}/${KAFKA_TGZ}"
    
    echo "Verifying SHA512 checksum..."
    echo "${EXPECTED_SHA}  ${KAFKA_TGZ}" | sha512sum --check
    if [ $? -ne 0 ]; then
        echo "SHA512 verification failed! Removing downloaded file."
        rm "${KAFKA_TGZ}"
        exit 1
    fi
else
    echo "Kafka archive already downloaded."
fi

# Extract if directory doesn't exist
if [ ! -d "${KAFKA_DIR}" ]; then
    echo "Extracting Kafka binaries..."
    tar -xzf "${KAFKA_TGZ}"
else
    echo "Kafka binaries already extracted."
fi

# Create/update the version-independent symlink
echo "Creating version-independent symlink..."
ln -sf "${KAFKA_DIR}" kafka

echo
echo "Setup complete!"
echo "Kafka tools are available in ${DEPS_DIR}/kafka/bin/"
echo "Run './benchmark_kafka_producer.sh' to start the performance test." 