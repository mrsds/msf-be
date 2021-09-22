#!/bin/sh


HOSTADDR=${1:-0.0.0.0}
HOSTPORT=${2:-9090}
PGENDPOINT=${3:-localhost}
PGPORT=${4:-5432}
PGUSER=${5:-user}
PGPASSWORD=${6:-foo}
S3BUCKET=${7:-bucket}
NUMSUBPROCS=${8:-0}

export PG_USER=$PGUSER
export PG_PWD=$PGPASSWORD

echo Host: $HOSTADDR
echo Port: $HOSTPORT
echo PG Endpoint: $PGENDPOINT
echo PG Port: $PGPORT
#echo PG User: $PGUSER
#echo PG Password: ------
echo S3 Bucket: $S3BUCKET
echo Number of Subprocesses: $NUMSUBPROCS

python -m msfbe.main --address=$HOSTADDR --port=$HOSTPORT --pgendpoint=$PGENDPOINT --pgport=$PGPORT --s3bucket=$S3BUCKET --subprocesses=$NUMSUBPROCS



