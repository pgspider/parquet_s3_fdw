#!/bin/sh

container_name='minio_server'
schemaless_container_name='minio_server_schemaless'
minio_image='minio/minio:RELEASE.2021-04-22T15-44-28Z.hotfix.56647434e'

echo "clean up ..."
if [ "$(docker ps -aq -f name=^/${container_name}$)" ]; then
    # cleanup
    docker stop ${container_name} || true && docker rm ${container_name} || true
fi
if [ "$(docker ps -aq -f name=^/${schemaless_container_name}$)" ]; then
    # cleanup
    docker stop ${schemaless_container_name} || true && docker rm ${schemaless_container_name} || true
fi

# clean-up old data
rm -rf /tmp/data_local || true
rm -rf /tmp/data_s3 || true
rm -rf /tmp/data_local_schemaless || true
rm -rf /tmp/data_s3_schemaless || true

mkdir -p /tmp/data_local || true
mkdir -p /tmp/data_s3 || true
mkdir -p /tmp/data_s3_schemaless || true

mkdir -p data/test-modify/parquet_modify_7
cp -a data /tmp/data_local
cp -a data/ported_postgres /tmp/data_local
cp -a data/ddlcommand /tmp/data_local
cp -a data /tmp/data_s3
cp -a data/ported_postgres /tmp/data_s3
cp -a data/ddlcommand /tmp/data_s3
cp -a data/test-bucket /tmp/data_s3

# Init data for schemaless mode
cp -a data /tmp/data_s3_schemaless
mkdir -p /tmp/data_s3_schemaless/data/test-modify/parquet_modify_7 || true
cp -a data/ported_postgres /tmp/data_s3_schemaless
cp -a data/test-bucket /tmp/data_s3_schemaless

# start server minio/s3, by docker:
if [ ! "$(docker ps -q -f name=^/${container_name}$ -f name=^/${schemaless_container_name}$)" ]; then
    echo "start minio docker ..."
    # run minio container
    docker run  -d --name ${container_name} -it -p 9000:9000 \
                -e "MINIO_ACCESS_KEY=minioadmin" -e "MINIO_SECRET_KEY=minioadmin" \
                -v /tmp/data_s3:/data \
                ${minio_image} \
                server /data

    # run minio container for schemaless mode
    docker run  -d --name ${schemaless_container_name} -it -p 9001:9000 \
                -e "MINIO_ACCESS_KEY=minioadmin" -e "MINIO_SECRET_KEY=minioadmin" \
                -v /tmp/data_s3_schemaless:/data \
                ${minio_image} \
                server /data
fi
