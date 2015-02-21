#include <sys/stat.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

// test name
char *testName;

/* test output files */
#define TESTPF "test_pagefile.bin"

/* prototypes for test functions */
static void testCreateOpenClose(void);
static void testSinglePageContent(void);

/* main function running all tests */
int
main (void)
{
  testName = "";
  
  initStorageManager();

  testCreateOpenClose();
  testSinglePageContent();

  return 0;
}


/* check a return code. If it is not RC_OK then output a message, error description, and exit */
/* Try to create, open, and close a page file */
void
testCreateOpenClose(void)
{
  SM_FileHandle fh;

  testName = "test create open and close methods";

  TEST_CHECK(createPageFile (TESTPF));
  
  TEST_CHECK(openPageFile (TESTPF, &fh));
  ASSERT_TRUE(strcmp(fh.fileName, TESTPF) == 0, "filename correct");
  ASSERT_TRUE((fh.totalNumPages == 1), "expect 1 page in new file");
  ASSERT_TRUE((fh.curPagePos == 0), "freshly opened file's page position should be 0");


  fh.curPagePos = 2;
  TEST_CHECK(closePageFile (&fh));
  ASSERT_TRUE((fh.curPagePos == 0), "closePageFile() should set curPagePos to 0");

  TEST_CHECK(destroyPageFile (TESTPF));
  ASSERT_TRUE((access(TESTPF, R_OK) < 0) && (errno==2), "file no longer exist");

  // after destruction trying to open the file should cause an error
  ASSERT_TRUE((openPageFile(TESTPF, &fh) != RC_OK), "opening non-existing file should return an error.");

  TEST_DONE();
}

/* Try to create, open, and close a page file */
void
testSinglePageContent(void)
{
  SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  testName = "test single page content";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // create a new page file
  TEST_CHECK(createPageFile (TESTPF));
  TEST_CHECK(openPageFile (TESTPF, &fh));
  printf("created and opened file\n");
  
  // read first page into handle
  TEST_CHECK(readFirstBlock (&fh, ph));
  // the page should be empty (zero bytes)
  for (i=0; i < PAGE_SIZE; i++) {
    ASSERT_TRUE((ph[i] == 0), "expected zero byte in first page of freshly initialized page");
  }
  printf("first block was empty\n");

  // change ph to be a string and write that one to disk
  for (i=0; i < PAGE_SIZE; i++)
    ph[i] = (i % 10) + '0';
  TEST_CHECK(writeBlock (0, &fh, ph));
  printf("writing first block\n");

  // read back the page containing the string and check that it is correct
  TEST_CHECK(readFirstBlock (&fh, ph));
  for (i=0; i < PAGE_SIZE; i++)
    ASSERT_TRUE((ph[i] == (i % 10) + '0'), "character in page read from disk is the one we expected.");
  printf("reading first block\n");


  // printf("ph[0] before:\t%c\n", ph[0]);
	TEST_CHECK(readBlock(0, &fh, ph));
	for (i=0; i < 20; i++) {
		// printf("ph[0] after:\t%c\n", ph[i]);
    ASSERT_TRUE((ph[i] == (i % 10) + '0'), "readBlock(0) results the same as readFirstBlock");
  }
  
  // trying to read block 1 should return RC_FILE_NOT_FOUND
  ASSERT_EQUALS_INT(readBlock(1, &fh, ph), RC_FILE_NOT_FOUND, "trying to read block 1 should return RC_FILE_NOT_FOUND");

  fh.curPagePos = 1;
  ASSERT_EQUALS_INT(getBlockPos(&fh), 1 , "testing position returned by getblockPos");
  
  // because we just set curPos to 1, the previous page should be 0, so the reading should be OK
  TEST_CHECK(readPreviousBlock(&fh, ph));
  TEST_CHECK(readCurrentBlock(&fh, ph)); // should take the position that readPreviousBlock just set

  // Additional Tests for appendEmptyBlock and ensureCapacity
  struct stat fileStat;
  TEST_CHECK(appendEmptyBlock(&fh));
  stat(fh.fileName,&fileStat);
  ASSERT_TRUE((fileStat.st_size == 12288), "size after append is what we expected.");
  
  TEST_CHECK(ensureCapacity(5, &fh));
  stat(fh.fileName,&fileStat);
  ASSERT_TRUE((fileStat.st_size == 24576), "size after ensure capacity 5 is what we expected.");

  // destroy new page file
  TEST_CHECK(destroyPageFile (TESTPF));  

  TEST_DONE();
}
