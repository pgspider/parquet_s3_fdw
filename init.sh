#!/bin/sh

rm -rf /tmp/data_local || true
rm -rf /tmp/data_s3 || true

mkdir -p /tmp/data_local || true
mkdir -p /tmp/data_s3 || true

cp -a data /tmp/data_local
cp -a data/ported_postgres /tmp/data_local
cp -a data /tmp/data_s3
cp -a data/ported_postgres /tmp/data_s3
cp -a data/test-bucket /tmp/data_s3

# start server minio/s3, by docker:
container_name='minio_server'

if [ ! "$(docker ps -q -f name=^/${container_name}$)" ]; then
    if [ "$(docker ps -aq -f status=exited -f status=created -f name=^/${container_name}$)" ]; then
        # cleanup
        docker rm ${container_name} 
    fi
    # run minio container
   sudo docker run -d --name ${container_name} -it -p 9000:9000 -e "MINIO_ACCESS_KEY=minioadmin" -e "MINIO_SECRET_KEY=minioadmin" -v /tmp/data_s3:/data minio/minio server /data
fi
