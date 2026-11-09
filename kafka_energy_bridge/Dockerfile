ARG BUILD_FROM
FROM ${BUILD_FROM}

# confluent-kafka richiede librdkafka
RUN apk add --no-cache python3 py3-pip librdkafka-dev gcc musl-dev python3-dev librdkafka

RUN pip3 install --no-cache-dir --break-system-packages \
    confluent-kafka==2.5.3 \
    websockets==13.1

COPY run.sh /run.sh
COPY bridge.py /bridge.py
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
