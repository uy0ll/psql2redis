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

# Expose the PostgreSQL, Redis ports
EXPOSE 5432 6379

CMD [ "python3", "-m", "psycopg2", "./cloud.py", "--host=0.0.0.0" ] 
