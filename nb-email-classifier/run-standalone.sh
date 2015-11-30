#!/bin/bash

MASTER=spark://mapr1node:7077 /opt/mapr/spark/spark-1.4.1/bin/spark-submit ./target/nb-email-classifier-1.0.jar
