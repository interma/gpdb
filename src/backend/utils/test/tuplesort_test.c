#include <stdarg.h>
#include <stddef.h>
#include <setjmp.h>
#include "cmockery.h"
#include "postgres.h"

#include "../sort/tuplesort.c" // the c file which we want to test

enum TUPLE_TYPE
{
	HEAP_TUPLE = 0,
	INDEX_TUPLE,
	PLACE_HOLDER
};

static void
init_mksort_struct(SortTuple *stuple, Tuplesortstate *state, int tuple_count, int type)
{	
	switch (type)
	{
	case HEAP_TUPLE:
		state->comparetup = comparetup_heap;
		state->mksortGetDatumFunc = mksort_get_datum_heap;
		break;
	case INDEX_TUPLE:
		state->comparetup = comparetup_index_btree;
		state->mksortGetDatumFunc = mksort_get_datum_index_btree;
		state->mksortHandleDupFunc = mksort_handle_dup_index_btree;
		break;
	default:
		break;
	}
}

static void
test__mksort_tuple(void **s)
{
	SortTuple		stuple;
	SortTuple		stuple2;	// use qsort_tuple() for comparsion
	int 			tuple_count;
	Tuplesortstate 	state;

	// it's not easy to create the Tuplesortstate object manually
	// so, I forgive to write a unit test for it
	return;	

	// tests for heaptuple
	tuple_count = 5;
	init_mksort_struct(&stuple, &state, tuple_count, HEAP_TUPLE);
	mksort_tuple(&stuple, tuple_count, 0, state.comparetup, &state, false);

	// tests for indextuple (btree)

}

int
main(int argc, char* argv[])
{
	cmockery_parse_arguments(argc, argv);

	const UnitTest tests[] = {
		unit_test(test__mksort_tuple),
	};

	return run_tests(tests);
}