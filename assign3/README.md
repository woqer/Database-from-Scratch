###Team Memeber:
* Wilfrido Vidana
* Zhiqun Nie
* An Shi
* Matthew Jankowiak


###Description:
The program is trying to implements a record manager which handles tables with a fixed schema.Clients can insert records, delete records, update records,and scan through the records in a table.


###Files included:
buffer_mgr.c       expr.c           storage_mgr.c          test_assign3_1.c
buffer_mgr.h       expr.h           storage_mgr.h          test_expr.c
buffer_mgr_stat.c  HELP.txt         tables.h               test_helper.h
buffer_mgr_stat.h  Makefile         test_assign1_1.c       test_simple
dberror.c          README.md        test_assign1_extra.c   test_simple.c
dberror.h          record_mgr.c     test_assign2_1.c
design_issues.txt  record_mgr.h     test_assign2_simple.c
dt.h               rm_serializer.c  test_assign3_1


### New error codes:
* assign2
  * RC_PINNED_PAGES 100
  * RC_PINNED_LRU 101

* assign3
  * RC_TABLE_NOT_FOUND 400
  * RC_TABLE_NAME_TOO_LONG 401
  * RC_RECORD_NOT_ACTIVE 402
  * RC_RECORD_OUT_OF_RANGE 403

###Testing:
All test cases pass in the following test files.
* test_expr:
  -> make expr
     ./test_expr
* test_assign3_1
  -> make main
     ./test_assign3_1
* Extra test:
  * test_simple
    ->make simple
      ./test_simple
