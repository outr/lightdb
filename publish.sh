#!/usr/bin/env bash

set -e

sbt clean
sbt Test/compile
sbt test
sbt docs/mdoc
sbt publishSigned
sbt sonatypeBundleRelease