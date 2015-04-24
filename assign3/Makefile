DEBUG ?= 1
	OPT = ""
ifneq (DEBUG, 1)
	OPT = -g
endif

all: simple expr main

main:
	gcc $(OPT) \
	dberror.c \
	storage_mgr.c \
	buffer_mgr.c \
	buffer_mgr_stat.c \
	expr.c \
	record_mgr.c \
	test_assign3_1.c -o test_assign3_1

test1_basic:
	gcc $(OPT) dberror.c storage_mgr.c test_assign1_1.c -o test_assign1_1

test1: test1_basic
	gcc $(OPT) dberror.c storage_mgr.c test_assign1_extra.c -o test_assign1_extra

test2_basic:
	gcc $(OPT) \
	dberror.c \
	buffer_mgr.c \
	buffer_mgr_stat.c \
	storage_mgr.c \
	test_assign2_simple.c -o test_assign2_simple

test2: test2_basic
	gcc $(OPT) \
	dberror.c \
	buffer_mgr.c \
	buffer_mgr_stat.c \
	storage_mgr.c \
	test_assign2_1.c -o test_assign2_1


expr:
	gcc $(OPT) \
	dberror.c \
	expr.c \
	record_mgr.c \
	test_expr.c -o test_expr	

simple:
	gcc $(OPT) \
	dberror.c \
	storage_mgr.c \
	buffer_mgr.c \
	buffer_mgr_stat.c \
	record_mgr.c \
	expr.c \
	test_simple.c -o test_simple	

clean:
	rm -f test_assign1_1
	rm -f test_assign1_extra
	rm -f test_assign2_1
	rm -f test_assign2_simple
	rm -f test_assign3_1
	rm -f test_expr
	rm -f test_simple
	rm -f *.bin
