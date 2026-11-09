ARG BUILD_FROM
FROM ${BUILD_FROM}

# kafka-python e' puro Python: niente compilazione di librdkafka, build affidabile su tutte le arch.
RUN apk add --no-cache python3 py3-pip

RUN pip3 install --no-cache-dir --break-system-packages \
    kafka-python==2.0.2 \
    websockets==13.1

COPY run.sh /run.sh
COPY bridge.py /bridge.py
RUN chmod a+x /run.sh

CMD [ "/run.sh" ]
