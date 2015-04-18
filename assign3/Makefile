all: simple expr main

main:
	gcc \
	dberror.c \
	storage_mgr.c \
	buffer_mgr.c \
	buffer_mgr_stat.c \
	expr.c \
	record_mgr.c \
	test_assign3_1.c -o test_assign3_1


expr:
	gcc \
	dberror.c \
	expr.c \
	record_mgr.c \
	test_expr.c -o test_expr	

simple:
	gcc \
	dberror.c \
	storage_mgr.c \
	buffer_mgr.c \
	buffer_mgr_stat.c \
	record_mgr.c \
	expr.c \
	test_simple.c -o test_simple	

clean:
	rm -f test_assign3_1
	rm -f test_expr
	rm -f test_simple
	rm -f *.bin
