# syntax=docker/dockerfile:1

FROM python:3.8.12-bullseye

WORKDIR /p2r/

RUN python3 -m venv /opt/venv
# Enable venv
ENV PATH="/opt/venv/bin:$PATH"
ENV PYTHONUNBUFFERED 1

COPY requirements.txt requirements.txt

RUN pip3 install install -Ur requirements.txt

COPY . .

ENV TTN_LW_HEALTHCHECK_URL=http://cloud.lorawan.internal.aceso.no:1885/healthz/live

HEALTHCHECK --interval=1m --timeout=5s CMD curl -f $TTN_LW_HEALTHCHECK_URL || exit 1
