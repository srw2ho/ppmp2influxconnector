FROM python:slim-buster

RUN apt update && apt install -y git gcc
RUN git config --global credential.helper cache

COPY ./ ./

RUN pip install Cython

RUN pip install git+https://github.com/srw2ho/ppmp2influxconnector.git



ENTRYPOINT tail -f /dev/null
