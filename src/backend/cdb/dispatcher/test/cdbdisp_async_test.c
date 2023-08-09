/**
 * @author interma
 * @brief demo unitest for cdbdisp_async
 */

#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include <string.h>
#include "cmockery.h"

#include "postgres.h"
#include "cdb/cdbdisp.h"
#include "cdb/cdbdispatchresult.h"
#include "miscadmin.h"
#include "cdb/cdbvars.h"
#include "utils/memutils.h"

#include "../cdbdisp_async.c" // the file which need to be tested

// helper functions
// copyed them from src/backend/utils/init/test/postinit_test.c
#undef PG_RE_THROW
#define PG_RE_THROW() siglongjmp(*PG_exception_stack, 1)

static void _errfinish_impl()
{
	PG_RE_THROW();
}

static void expect_ereport(int expect_elevel)
{
	expect_value(errstart, elevel, expect_elevel);
	expect_any(errstart, domain);
	if (expect_elevel < ERROR)
		will_return(errstart, false);
    else
		will_return_with_sideeffect(errstart, false, &_errfinish_impl, NULL);
}
// end helper functions

// callback function for will_return_with_sideeffect(cdbconn_isBadConnection ...)
static void stop_result(void *arg)
{
	CdbDispatchCmdAsync *pParms = (CdbDispatchCmdAsync *)arg;
    for (int i = 0; i < pParms->dispatchCount; i++)
		pParms->dispatchResultPtrArray[i]->stillRunning = false;
}

#define UNITTEST_NUM_SEGS 2
#define QUERY_TEXT "select * from cdbdisp_async_test;"
// construct CdbDispatcherState manually
static CdbDispatcherState *makeDispatcherState()
{
    struct CdbDispatcherState *ds = 
		(struct CdbDispatcherState *) palloc0(sizeof(struct CdbDispatcherState));

    struct CdbDispatchResults *results = 
		(struct CdbDispatchResults *) palloc0(sizeof(struct CdbDispatchResults));
	results->resultArray = palloc0(UNITTEST_NUM_SEGS * sizeof(results->resultArray[0]));
	results->resultCapacity = UNITTEST_NUM_SEGS;
    ds->primaryResults = results;

    cdbdisp_makeDispatchParams(ds, 1, QUERY_TEXT, strlen(QUERY_TEXT));
    CdbDispatchCmdAsync *pParms = (CdbDispatchCmdAsync *)(ds->dispatchParams);
    pParms->dispatchCount = UNITTEST_NUM_SEGS;
    pParms->waitMode = DISPATCH_WAIT_NONE;
    pParms->dispatchResultPtrArray = (struct CdbDispatchResult **) palloc0(sizeof(struct CdbDispatchResult *));
    for (int i = 0; i < pParms->dispatchCount; i++)
    {
        CdbDispatchResult *dispatchResult =
			(struct CdbDispatchResult *) palloc0(sizeof(struct CdbDispatchResult));
        dispatchResult->stillRunning = true;
		dispatchResult->segdbDesc = 
			(struct SegmentDatabaseDescriptor *) palloc0(sizeof(struct SegmentDatabaseDescriptor));
		dispatchResult->segdbDesc->conn = palloc0(sizeof(PGconn));
		PGconn *conn = dispatchResult->segdbDesc->conn;
		conn->status = CONNECTION_STARTED;
		conn->sock = 1234;
		conn->outCount = 0;
		pParms->dispatchResultPtrArray[i] = dispatchResult;
    }
	return ds;
}

// unit test for checkDispatchResult 
static void cdbdisp_checkDispatchResult_test(void **state)
{
	// prepare input param: ds for cdbdisp_checkDispatchResult
	struct CdbDispatcherState *ds = makeDispatcherState();

	// let the first call of cdbconn_isBadConnection return false
	expect_any(cdbconn_isBadConnection, segdbDesc);
    will_return(cdbconn_isBadConnection, false);

	// let the second call of cdbconn_isBadConnection return true
	// and stop the loop
	expect_any(cdbconn_isBadConnection, segdbDesc);
    will_return_with_sideeffect(cdbconn_isBadConnection, true, stop_result, ds->dispatchParams);	

	// expect the warning happens:
	// 	elog(WARNING, "Connection (%s) is broken, PQerrorMessage:%s"
	expect_ereport(WARNING);

	PG_TRY();
	{
		cdbdisp_checkDispatchResult(ds, DISPATCH_WAIT_NONE);
	}
	PG_CATCH();
	{
		proc_exit_inprogress = true;
		AtAbort_DispatcherState();
	}
	PG_END_TRY();
}

// a empty unit test
static void dummy_test(void **state)
{}

int main(int argc, char *argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
        unit_test(cdbdisp_checkDispatchResult_test),
		unit_test(dummy_test)
	};

	MemoryContextInit();
	DispatcherContext = AllocSetContextCreate(TopMemoryContext,
											  "Dispatch Context",
											  ALLOCSET_DEFAULT_MINSIZE,
											  ALLOCSET_DEFAULT_INITSIZE,
											  ALLOCSET_DEFAULT_MAXSIZE);

	CurrentResourceOwner = ResourceOwnerCreate(NULL, "cdbdisp_async_test");
	Gp_role = GP_ROLE_DISPATCH;
	GpIdentity.dbid = 1;
	GpIdentity.segindex = -1;

	return run_tests(tests);
}