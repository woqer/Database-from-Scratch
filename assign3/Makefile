all:
	gcc \
	dberror.c \
	storage_mgr.c \
	buffer_mgr.c \
	buffer_mgr_stat.c \
	expr.c \
	rm_serializer.c \
	record_mgr.c \
	test_assign3_1.c -o test_assign3_1

	gcc \
	dberror.c \
	expr.c \
	rm_serializer.c \
	record_mgr.c \
	test_expr.c -o test_expr

clean:
	rm -f test_assign3_1
	rm -f test_expr
	rm -f *.bin
