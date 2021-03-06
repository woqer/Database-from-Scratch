Group Member:
* Wilfrido Vidana
* Matthew Jankowiak
* Zhiqun Nie
* An Shi

Description:
The program is trying to implement a storage manager that is able to read blocks from a file on disk and write blocks from memory to a file on disk.

Files included:
dberror.c  README          test_assign1_1.c     test_pagefile.bin
dberror.h  storage_mgr.c   test_assign_extra
_DS_Store  storage_mgr.h   test_assign_extra.c
Makefile   test_assign1_1  test_helper.h

Methods implemented in storage_mgr.c:
* Create a header page which has the header size of 4, to save the information of the pageFile, the readHeader() method returns the total number of pages, writeHeader()method tries to write the header info and filled the rest of the page with 0's and the method returns the message whether write is successful or not,but when counting total number of pages and current page position, the header page is not included.
* Create a page file and fills the single page with '0\' bytes, then implements openPageFile,closePageFile and destroyPageFile.
* Implement readBlock(), readPreviousBlock(), readCurrentBlock(),readNextBlock() and writeBlock(), writeCurrentBlock() methods.
* Implement appendEmptyBlock() method which increases the number of pages by one and filled with '\0' bytes. Test case is added to test this method.

Additional test case and error check added in test_assign_extra for the following methods:
* createPageFile()
* openPageFile()
* closePageFile()
* destroyPageFile()
* readPreviousBlock()
* readNextBlock()
* readCurrentBlock()
* writeCurrentBlock()
* appendEmptyBlock()
* ensureCapacity()

Excution instructions:
* two excutable files generated from Makefile for test_assign1_1.c and test_assign_extra.c
	(if not seeing them, type command: make to generate)
* excute t_assign1_1 to see results for required test cases.
	command: ./t_assign1_1
* excute test_assign_extra to see results for additional test cases and error check
	command: ./test_assign_extra
* all test cases pass 
* to see how the methods are implemented, open storage_mgr.c
	command: vi storage_mgr.c
