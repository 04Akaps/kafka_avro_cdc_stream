#!/bin/bash

set -e

echo "Cleaning up existing Spring Boot processes..."
pkill -f "com.example.MainKt" > /dev/null 2>&1 || true
sleep 2

echo "Starting application with Gradle..."
./gradlew bootRun