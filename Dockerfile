FROM python:3.12.7-slim

WORKDIR /

RUN apt-get update -y

COPY requirments.txt /
RUN pip install --no-cache-dir -r requirments.txt

COPY etl_pipeline.py /

ENTRYPOINT ["python", "/etl_pipeline.py"]