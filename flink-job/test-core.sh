#!/bin/sh
# Compile and run the pattern-logic tests with a plain JDK (17+). No Maven, no Flink, no network.
set -e
cd "$(dirname "$0")"
out=$(mktemp -d)
javac -d "$out" src/main/java/cep/core/*.java src/test/java/cep/core/TestCore.java
java -cp "$out" cep.core.TestCore
rm -rf "$out"
