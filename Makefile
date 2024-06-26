EXTENSION = pg_hot_cold_start
MODULES = src/pg_hot_cold_start

CC = gcc
PG_CONFIG = /home/jan/postgres/src/bin/pg_config/pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)