
purpose
-----

Python replication for TheThingStack Postgres logic decoder and copy data from local to remote Redis DB .

-----
Settings
=========================

In your TheThingStack docker-compose.yml configuration file you need to enable replication:

  postgres:
    image: debezium/postgres:14
    command:
      - "postgres"
      - "-c"
      - "wal_level=logical"
      - "-c"
      - "max_replication_slots=1"
      - "-c"
      - "max_wal_senders=4"
      - "-c"
      - "shared_preload_libraries=wal2json"
    restart: unless-stopped
    environment:
      - POSTGRES_PASSWORD=root
      - POSTGRES_USER=root
      - POSTGRES_DB=ttn_lorawan_dev
    volumes:
      - ${DEV_DATA_DIR:-.env/data}/postgres:/var/lib/postgresql/data
    ports:
      - "127.0.0.1:5432:5432"

before you start:

1. create_replication_slot
2. set postgres and redis config in your config.yml


ref
-----
https://www.postgresql.org/docs/10/static/logicaldecoding-example.html
http://initd.org/psycopg/docs/advanced.html#replication-support
https://redis.io/documentation

