FROM bitnami/spark:latest

WORKDIR /app
USER root

COPY jars/* /opt/bitnami/spark/jars/

COPY app.py /app/app.py

RUN pip install pyspark python-dotenv


# add user 'trannam'
RUN useradd -m -s /bin/bash trannam \
    && echo "trannam ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

RUN chown -R trannam:trannam /opt/bitnami
# switch user  'trannam'
USER trannam

CMD ["spark-submit", "/app/app.py"]

