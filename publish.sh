#!/usr/bin/env bash

sbt +clean +test +coreJS/publishSigned +coreJVM/publishSigned +halo/publishSigned +lucene/publishSigned sonatypeRelease