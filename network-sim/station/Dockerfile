FROM ubuntu:16.04

# Update image
SHELL ["/bin/bash", "-c"]

RUN apt-get update && apt-get install -y --no-install-recommends libcurl4-openssl-dev python-pip python libboost-python-dev python-dev

WORKDIR /station
COPY ./station /station
COPY ./common /station

RUN pip install --upgrade pip
RUN pip install azure-iothub-device-client

EXPOSE 80

CMD ["python", "station.py", "${STATION_ID}", "80", "${CONN_STRING}"]
