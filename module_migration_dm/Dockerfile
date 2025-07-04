FROM python:3.11

WORKDIR /module_migration_dm
ADD . /module_migration_dm

RUN apt-get update
RUN pip install -r requirements.txt

CMD [ "alembic", "upgrade", "head" ]
