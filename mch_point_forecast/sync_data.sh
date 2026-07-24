#!/bin/bash

AWS_ACCESS_KEY_ID=oIMm0sngRvhRQmP1YkqK
AWS_SECRET_ACCESS_KEY=BXvbIt0kakxBWDRMigLjaF0pzcyjia8BLA6QoUmF

AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin aws --endpoint-url http://localhost:9000 s3 sync . s3://raw-data/mch/point-forecast 
