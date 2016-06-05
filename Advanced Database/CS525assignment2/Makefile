dberror.o: dberror.c
	cc -o dberror.o -c dberror.c

storage_mgr.o: storage_mgr.c
	cc -o storage_mgr.o -c storage_mgr.c

buffer_mgr.o: buffer_mgr.c
	cc -o buffer_mgr.o -c buffer_mgr.c

buffer_mgr_stat.o: buffer_mgr_stat.c
	cc -o buffer_mgr_stat.o -c buffer_mgr_stat.c

test_assign2_1.o: test_assign2_1.c
	cc -o test_assign2_1.o -c test_assign2_1.c 

test_assign2_2.o: test_assign2_2.c
	cc -o test_assign2_2.o -c test_assign2_2.c 

assign2_1: test_assign2_1.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o dberror.o
	cc -o assign2_1 test_assign2_1.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o dberror.o -I. 

assign2_2: test_assign2_2.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o dberror.o
	cc -o assign2_2 test_assign2_2.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o dberror.o -I.

clean: 
	rm assign2_1 assign2_2 test_assign2_1.o test_assign2_2.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o dberror.o


