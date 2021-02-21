FROM python:3.9.2-alpine3.13

# Configuration
ENV MQTT_URL "tcp://localhost:8883"
# ENV CONFIG_TOPIC=<MQTT topic> # example sensors/rtl_433/something
# ENV CONFIG_PWS_ID=<PWS Weather station id
# ENV CONFIG_PWS_PASS=<PWS Weather password>
# ENV CONFIG_WU_ID=<Weather Underground station id>
# ENV CONFIG_WU_KEY=<Weather Underground password>

ADD requirements.txt /
RUN pip install -r requirements.txt

ADD publish.py /
CMD ["python", "/publish.py"]
