MODULE_big = parquet_s3_fdw
OBJS = parquet_impl.o parquet_fdw.o
# Add file for S3
OBJS += parquet_s3_fdw.o parquet_s3_fdw_connection.o parquet_s3_fdw_server_option.o

PGFILEDESC = "parquet_s3_fdw - foreign data wrapper for parquet on S3"

SHLIB_LINK = -lm -lstdc++ -lparquet -larrow
# Add libraries for S3
SHLIB_LINK += -laws-cpp-sdk-core -laws-cpp-sdk-s3

EXTENSION = parquet_s3_fdw
DATA = parquet_s3_fdw--0.1.sql parquet_s3_fdw--0.1--0.2.sql

REGRESS = parquet_fdw import parquet_s3_fdw import_s3 parquet_s3_fdw2

EXTRA_CLEAN = sql/parquet_fdw.sql expected/parquet_fdw.out

PG_CONFIG ?= pg_config

# parquet_impl.cpp requires C++ 11.
override PG_CXXFLAGS += -std=c++11 -O3

PGXS := $(shell $(PG_CONFIG) --pgxs)

# pass CCFLAGS (when defined) to both C and C++ compilers.
ifdef CCFLAGS
	override PG_CXXFLAGS += $(CCFLAGS)
	override PG_CFLAGS += $(CCFLAGS)
endif

include $(PGXS)

# XXX: PostgreSQL below 11 does not automatically add -fPIC or equivalent to C++
# flags when building a shared library, have to do it here explicitely.
ifeq ($(shell test $(VERSION_NUM) -lt 110000; echo $$?), 0)
	override CXXFLAGS += $(CFLAGS_SL)
endif

# PostgreSQL uses link time optimization option which may break compilation
# (this happens on travis-ci). Redefine COMPILE.cxx.bc without this option.
COMPILE.cxx.bc = $(CLANG) -xc++ -Wno-ignored-attributes $(BITCODE_CXXFLAGS) $(CPPFLAGS) -emit-llvm -c

# XXX: a hurdle to use common compiler flags when building bytecode from C++
# files. should be not unnecessary, but src/Makefile.global omits passing those
# flags for an unnknown reason.
%.bc : %.cpp
	$(COMPILE.cxx.bc) $(CXXFLAGS) $(CPPFLAGS)  -o $@ $<
