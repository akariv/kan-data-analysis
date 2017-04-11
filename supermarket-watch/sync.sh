#!/bin/bash
IP_ADDRESS=139.59.191.123
ssh root@$IP_ADDRESS -t 'docker exec -it `docker ps -f name=dpp --format {{.ID}}` apk --update add libxslt'
ssh root@$IP_ADDRESS -t 'docker exec -it `docker ps -f name=dpp --format {{.ID}}` pip install pyquery'
ssh root@$IP_ADDRESS -t 'docker exec -it `docker ps -f name=dpp --format {{.ID}}` pip install https://github.com/frictionlessdata/jsontableschema-sql-py/archive/fix/autoincrement-adjustments.zip'
#ssh root@$IP_ADDRESS -t 'docker exec -it `docker ps -f name=postgres --format {{.ID}}` createdb redash'
#ssh root@$IP_ADDRESS -t 'docker exec -it `docker ps -f name=postgres --format {{.ID}}` su postgres /bin/sh -c "createdb data"'
scp datapipelines/pipelines/*{yaml,py,csv} root@$IP_ADDRESS:/root/pipelines/
scp nginx.conf root@$IP_ADDRESS:/root/
ssh root@$IP_ADDRESS -t 'docker restart `docker ps -f name=dpp --format {{.ID}}`'

