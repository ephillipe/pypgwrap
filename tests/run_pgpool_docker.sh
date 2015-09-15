#!/usr/bin/env bash

export PGPOOL_BACKENDS=0:pat01550:5432
export PGPOOL_PORT=54321
export PCP_USER=postgres
export PCP_USER_PASSWORD=150282

docker stop $(docker ps -q -f name=pgpool)
docker rm $(docker ps -a -q -f name=pgpool)
docker run -d --name pgpool -p 54321:5454 \
           -v /opt/python/current/app/pgpool:/etc/pgpool2 \
           -e PGPOOL_BACKENDS=$PGPOOL_BACKENDS \
           -e PGPOOL_PORT=$PGPOOL_PORT \
           -e PCP_USER=$PCP_USER \
           -e PCP_USER_PASSWORD=$PCP_USER_PASSWORD bettervoice/pgpool2-container:3.3.4
docker logs -f pgpool