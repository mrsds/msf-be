FROM continuumio/anaconda
COPY . /msf
WORKDIR /msf

RUN apt-get --allow-releaseinfo-change update
RUN apt-get install -y build-essential
RUN apt-get install -y autoconf automake gdb git libffi-dev zlib1g-dev libssl-dev
RUN conda install -y gdal
WORKDIR /msf 
RUN python setup.py install


CMD sh run-msf-be.sh $HOSTADDR $HOSTPORT $PGENDPOINT $PGPORT $PGUSER $PGPASSWORD $S3BUCKET $NUMSUBPROCS