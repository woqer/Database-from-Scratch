#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

char *testName;

#define TESTPF "testbuffer.bin"

int main (void) {
  SM_FileHandle fh;
  SM_PageHandle ph;
  int i;

  testName = "test single page content";

  ph = (SM_PageHandle) malloc(PAGE_SIZE);

  // printf("Header: %d\n", readHeader(fopen(TESTPF, "r")));

  // TEST_CHECK(createPageFile (TESTPF));

  TEST_CHECK(openPageFile (TESTPF, &fh));
  
  printf("opened file\n");
  
  TEST_CHECK(ensureCapacity(10000, &fh));

  // for (i = 0; i < 10000; i++) {
  //   sprintf(ph, "Page-%d", i);
  //   TEST_CHECK(writeBlock(i, &fh, ph))
  // }

  // TEST_CHECK(writeBlock(9999, &fh, "Lololo"));

  // read first page into handle
  TEST_CHECK(readFirstBlock (&fh, ph));
  printf("Readed 1:\t\t %s\n", ph);
  TEST_CHECK(readBlock(9998, &fh, ph));
  printf("Readed 9998:\t\t %s\n", ph);
  TEST_CHECK(readBlock(9999, &fh, ph));
  printf("Readed 9999:\t\t %s\n", ph);

  TEST_DONE();
}