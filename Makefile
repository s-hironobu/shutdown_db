# shutdown_db/Makefile

MODULE_big = shutdown_db
OBJS = shutdown_db.o bgworker.o functions.o hashtable.o

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
else
subdir = contrib/shutdown_db
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
endif

