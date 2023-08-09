subdir=src/backend/cdb/dispatcher
top_builddir = ../../../../..
include $(top_builddir)/src/Makefile.global

TARGETS=cdbdispatchresult \
		cdbgang \
		cdbdisp_async

include $(top_srcdir)/src/backend/mock.mk


# cdbconn_mock is for cdbconn_isBadConnection
# elog_mock is for expect_ereport
cdbdisp_async.t: \
	$(MOCK_DIR)/backend/cdb/dispatcher/cdbconn_mock.o \
	$(MOCK_DIR)/backend/utils/error/elog_mock.o \
	$(MOCK_DIR)/backend/access/hash/hash_mock.o \
	$(MOCK_DIR)/backend/utils/fmgr/fmgr_mock.o

cdbdispatchresult.t: \
	$(MOCK_DIR)/backend/libpq/pqexpbuffer_mock.o \
	$(MOCK_DIR)/backend/cdb/cdbrelsize_mock.o \
	$(MOCK_DIR)/backend/cdb/cdbsrlz_mock.o \
	$(MOCK_DIR)/backend/access/hash/hash_mock.o \
	$(MOCK_DIR)/backend/utils/fmgr/fmgr_mock.o \

cdbgang.t: \
	$(MOCK_DIR)/backend/catalog/namespace_mock.o \
	$(MOCK_DIR)/backend/storage/lmgr/proc_mock.o \
	$(MOCK_DIR)/backend/access/transam/xact_mock.o \
	$(MOCK_DIR)/backend/cdb/cdbutil_mock.o \
	$(MOCK_DIR)/backend/cdb/cdbfts_mock.o \
	$(MOCK_DIR)/backend/utils/misc/superuser_mock.o \
	$(MOCK_DIR)/backend/libpq/fe-connect_mock.o \
	$(MOCK_DIR)/backend/utils/mmgr/redzone_handler_mock.o \
	$(MOCK_DIR)/backend/utils/misc/faultinjector_mock.o \
	$(MOCK_DIR)/backend/access/hash/hash_mock.o \
	$(MOCK_DIR)/backend/utils/fmgr/fmgr_mock.o \
