Group Member:
* Wilfrido Vidana
* Matthew Jankowiak
* Zhiqun Nie
* An Shi

Description:
The program is trying to implement a buffer manager that is able to handle buffers in memory.

Files included:
dberror.c  README              test_assign1_1.c 
dberror.h   buffer_mgr.c       test_assign2_1.c
_DS_Store   buffer_mgr.h       test_assign2_1.c.bak
debugging.c buffer_mgr_stat.c  test_assign2_1.c.bak1
dt.c        buffer_mgr_stat.h  test_assign2_1.gdb
Makefile					   test_helper.c

Methods implemented in storage_mgr.c:
*FIFO
We use a circular FIFO, that is we only need to store the index of the last writen frame, and on every new write increment it until the end, in that case fifo_old will be the first element (index 0), and so on...
We update this parameter only when a frame is writen

*LRU
We have an array, sized the number of frames.
This array stores integers from 0 to number of frames
0 is the least accesed frame
Once we have to update this array, we search from max to 0 the index of the frame we are updating. All the indexes above are decremented 1, and the tarjet index is set to max
with this we are saying that all the "newer" frames are one time older, and the last accesed frame is the newest.
Everytime a frame is accesed, we update our LRU array

Excution instructions:
* excute t_assign2_1 to see results for required test cases.
	command: ./t_assign1_1
* all test cases pass 
* to see how the methods are implemented, open buffer_mgr.c
	command: vi buffer_mgr.c
