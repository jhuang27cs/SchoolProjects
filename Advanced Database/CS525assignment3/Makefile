dberror.o: dberror.c
	cc -o dberror.o -c dberror.c

storage_mgr.o: storage_mgr.c
	cc -o storage_mgr.o -c storage_mgr.c

buffer_mgr.o: buffer_mgr.c
	cc -o buffer_mgr.o -c buffer_mgr.c

buffer_mgr_stat.o: buffer_mgr_stat.c
	cc -o buffer_mgr_stat.o -c buffer_mgr_stat.c

record_mgr.o: record_mgr.c
	cc -o record_mgr.o -c record_mgr.c 

rm_serializer.o: rm_serializer.c
	cc -o rm_serializer.o -c rm_serializer.c

expr.o: expr.c
	cc -o expr.o -c expr.c

test_expr.o: test_expr.c
	cc -o test_expr.o -c test_expr.c 

test_assign3_1.o: test_assign3_1.c
	cc -o test_assign3_1.o -c test_assign3_1.c

assign3_1: test_assign3_1.o record_mgr.o rm_serializer.o expr.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o dberror.o
	cc -o assign3_1 test_assign3_1.o record_mgr.o rm_serializer.o expr.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o dberror.o -I. 

expr: test_expr.o record_mgr.o rm_serializer.o expr.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o dberror.o
	cc -o expr test_expr.o record_mgr.o rm_serializer.o expr.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o dberror.o -I.

clean: 
	rm assign3_1 expr test_assign3_1.o test_expr.o record_mgr.o rm_serializer.o expr.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o dberror.o


