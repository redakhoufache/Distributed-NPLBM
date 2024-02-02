#!/bin/bash

scala -J-Xmx1024m ./target/DisNPLBM-1.0-jar-with-dependencies.jar local[4] 35 20 2.0 2.0
