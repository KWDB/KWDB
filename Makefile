# Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.  All rights reserved.

BASEDIR := $(shell pwd)
BUILDDIR := $(BASEDIR)/build
GOPATH ?=

CC ?= $(shell which gcc)
CXX ?= $(shell which g++)

KWBASE_OSS ?= ON

PROTOBUF_DIR ?=

PARALLEL_FLAG = $(findstring -j, $(MAKEFLAGS))

# cmake configuration options
BUILD_TYPE ?= Debug
WITH_TESTS ?= OFF
INSTALL_PATH ?= $(BASEDIR)/install
CMAKE_CONFIG_OPTIONS ?= -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) \
	-DCMAKE_INSTALL_PREFIX=$(INSTALL_PATH) -DKWBASE_OSS=$(KWBASE_OSS) \
	-DPROTOBUF_DIR=$(PROTOBUF_DIR)

ALL_TESTS := cpplint kwdbts2-test kwbase-test

# if GOPATH not set, check and set available GOPATH
ifndef GOPATH
ifeq ($(shell echo $(BASEDIR) | grep -Po ".*/src/gitee.com/kwbasedb"),)
  $(error Current source path isn't match GOPATH rule: ".*/src/gitee.com/kwbasedb")
else
  GOPATH := $(shell echo $(BASEDIR) | sed -nE 's!(.*)/src/gitee.com/kwbasedb!\1!p')
endif
endif

all: build

################################################################
#                                                              #
#                    Build scope                               #
#                                                              #
################################################################
$(BUILDDIR):
	@rm -rf $@ && mkdir -p $(BUILDDIR)

build: export GO111MODULE = off
build: .ALWAYS_REBUILD | bin/.submodules-initialized
	$(info ========== $@ ==========)
	@if [ -n "$(PARALLEL_FLAG)" ]; then \
		NUMBER=$$(echo $(MAKEFLAGS) | egrep -o '(-j)[0-9]*' | sed 's/-j//'); \
		if [ -z "$${NUMBER}" ]; then \
			NUMBER=$$(getconf _NPROCESSORS_ONLN || sysctl -n hw.ncpu || nproc); \
		fi; \
		echo "Parallel build with -j$${NUMBER}"; \
		PARALLEL="-DCMAKE_BUILD_PARALLEL_LEVEL=$${NUMBER}"; \
	fi; \
	GOPATH=$(GOPATH) cmake -B $(BUILDDIR) -S $(BASEDIR) $(CMAKE_CONFIG_OPTIONS) $${PARALLEL}
	$(MAKE) -C $(BUILDDIR) \
		BUILDTYPE=$(if $(findstring $(BUILD_TYPE),debug Debug),development,release)

################################################################
#                                                              #
#                    Base test                                 #
#                                                              #
################################################################
cpplint: .ALWAYS_REBUILD
	$(info ========== $@ ==========)
	cpplint --recursive --exclude=kwdbts2/mmap --exclude=kwdbts2/third_party \
		--exclude=kwdbts2/roachpb --exclude=kwdbts2/**/tests \
		--filter=-build/c++11,-build/include_subdir,-runtime/references,-readability/fn_size \
		--linelength=125 $(BASEDIR)/kwdbts2

kwdbts2-test: BUILDDIR := $(addsuffix -kwdbts2-test, $(BUILDDIR))
kwdbts2-test: CMAKE_CONFIG_OPTIONS := -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) \
		-DWITH_TESTS=ON -DBUILD_KWBASE=OFF
kwdbts2-test: .ALWAYS_REBUILD build | $(BUILDDIR)
	$(info ========== $@ ==========)
	$(MAKE) -C $(BUILDDIR) test

kwbase-test: export KWBASE_LOGIC_TEST_SKIP = true
kwbase-test: export KWBASE_NIGHTLY_STRESS = true
kwbase-test: BUILDDIR := $(addsuffix -kwbase-test, $(BUILDDIR))
kwbase-test: CMAKE_CONFIG_OPTIONS := -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) \
	-DCMAKE_INSTALL_PREFIX=$(INSTALL_PATH) -DWITH_TESTS=OFF -DBUILD_KWBASE=ON
kwbase-test: .ALWAYS_REBUILD build | $(BUILDDIR)
	$(info ========== $@ ==========)
	$(info kwbase lint)
	stdbuf -oL -eL $(MAKE) -C $(BASEDIR)/kwbase lint TESTTIMEOUT=45m \
		GOPATH=$(GOPATH) KWDB_LIB_DIR=$(BUILDDIR)/lib
	$(info Test kwbase)
	stdbuf -oL -eL $(MAKE) -C $(BASEDIR)/kwbase test TESTTIMEOUT=45m \
		GOPATH=$(GOPATH) KWDB_LIB_DIR=$(BUILDDIR)/lib

test: .ALWAYS_REBUILD $(ALL_TESTS) | $(BUILDDIR)
	$(info ========== $@ ==========)

################################################################
#                                                              #
#                    Integration test                          #
#                                                              #
################################################################
MEMCHECK_FILES := \
	$(wildcard $(addsuffix -kwdbts2-test, $(BUILDDIR))/tests/*)
$(MEMCHECK_FILES): .ALWAYS_REBUILD
	$(info ========== $@ ==========)
	cd $@ && valgrind --tool=memcheck --leak-check=full --max-threads=10000 \
			--error-exitcode=1 $@/$(shell sed -nE 's/.*\/(.*).dir/\1/p' <<< $@)

memcheck: .ALWAYS_REBUILD $(MEMCHECK_FILES) | kwdbts2-test
	$(info ========== $@ ==========)

test-logic: .ALWAYS_REBUILD | build
	$(info ========== $@ ==========)
	stdbuf -oL -eL $(MAKE) -C $(BASEDIR)/kwbase testlogic TESTTIMEOUT=45m \
		GOPATH=$(GOPATH) KWDB_LIB_DIR=$(BUILDDIR)/lib

TEST_NAME ?= TEST_v2_integration_basic_v2.sh
TEST_TOPOLOGIES ?= 1n 5c 5cr

regression-test: T ?= *.sql
regression-test: BUILDDIR := $(addsuffix -regression-test, $(BUILDDIR))
regression-test: .ALWAYS_REBUILD | install
	$(info ========== $@ ==========)
	cd $(BASEDIR) && qa/run_test_local_v2.sh $(TEST_NAME) $(T) $(TEST_TOPOLOGIES)

regression-test-ha: TEST_NAME := TEST_v2_integration_distribute_v2.sh
regression-test-ha: TEST_TOPOLOGIES := 5c
regression-test-ha: .ALWAYS_REBUILD | regression-test

################################################################
#                                                              #
#                    PHONY                                     #
#                                                              #
################################################################

install: .ALWAYS_REBUILD | build
	$(info ========== $@ ==========)
	$(MAKE) -C $(BUILDDIR) install

.PHONY: .ALWAYS_REBUILD install clean help bin/.submodules-initialized
.ALWAYS_REBUILD:

bin/.submodules-initialized:
	gitdir=$$(git rev-parse --git-dir 2>/dev/null || true); \
	if test -n "$$gitdir"; then \
	   git submodule update --init --recursive; \
	fi

clean:
	rm -rf build*
	rm -rf log/
	rm -rf install/
	rm -rf kwbase/.buildinfo
	rm -rf kwbase/bin
	rm -rf kwbase/build/defs.mk kwbase/build/defs.mk.sig
	rm -rf qa/TEST_integration
	@echo Clean Done.

help:
	@echo "help: "
	@echo "  all:                 build alias"
	@echo "  build:               project build"
	@echo "  cpplint:             code specifications scan of kwdbts2"
	@echo "  kwdbts2-test:        kwdbts2 unit test"
	@echo "  kwbase-test:         kwbase lint test and unit test"
	@echo "  test:                collection of cpplint,kwdbts2-test,kwbase-test"
	@echo "  memcheck:            memory leak  test of kwdbts2 unit"
	@echo "  test-logic:          kwbase logic test"
	@echo "  regression-test:     regression test of 1n,5c,5cr mode"
	@echo "  regression-test-ha:  regression test of ha mode"
	@echo "  install:             install build artifact to install directory"
	@echo "  clean:               clean build and test files"
