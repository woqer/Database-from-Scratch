#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <stdlib.h>
// #include <string.h>

static int NumReadIO = 0;
static int NumWriteIO = 0;

/* A structure that stores bookkeeping data of buffer manager pool. 
 */
typedef struct BM_PoolInfo {
  int numPages;
  SM_FileHandle *fh;// file handler of page file associated with buffer pool
  bool *dirtys;     // dirty flags array
  int *fixCounter;  // fix counter array
  PageNumber *map;  // mapping between page frames and page numbers
  int fifo_old;     // circular FIFO, index of the oldest added item
  int *lru_stamp;   // used stamp, begins at 0
  char **frames;    // frames pointer array
} BM_PoolInfo;

/************************************************************
 *                    Functions definitions                 *
 ************************************************************/

/* A function to print elements in an integer array.
 * The elements start at index 0 and ends at index length-1.
 */
void printIntArray(const char* objectName, int *i_a, int length) {
  printf("%s:\t\t [", objectName);
  int i;
  for (i = 0; i < (length - 1); i++) {
    printf("%d, ", i_a[i]);
  }
  printf("%d]\n", i_a[length - 1]);
}

/* A function to print elements in a boolean array.
 * The elements start at index 0 and ends at index length-1.
 */
void printBoolArray(const char* objectName, bool *i_a, int length) {
  printf("%s:\t\t [", objectName);
  int i;
  for (i = 0; i < (length - 1); i++) {
    printf("%s, ", i_a[i] ? "true" : "false");
  }
  printf("%s]\n", i_a[length - 1] ? "true" : "false"); 
}

/* A function to linearly search a target integer in an integer array.
 * The index of the target is returned if found, -1 if not found.
 * The search start at index 0 and ends at index length-1.
 */
void printStrArray(const char* objectName, char **i_a, int length) {
  printf("%s:\t\t [", objectName);
  int i;
  for (i = 0; i < (length - 1); i++) {
    printf("\"%s\", ", i_a[i]);
  }
  printf("\"%s\"]\n", i_a[length - 1]); 
}

/* A function to print all pool information. 
 */
void printPoolInfo(BM_PoolInfo *pi) {
  SM_FileHandle *fh = pi->fh;
  printf("numPages:\t\t %d\n", pi->numPages);
  printf("fh->fileName:\t\t %s\n", fh->fileName);
  printf("fh->totalNumPages:\t\t %d\n", fh->totalNumPages);
  printBoolArray("dirtys", pi->dirtys, pi->numPages);
  printIntArray("fixCounter", pi->fixCounter, pi->numPages);
  printIntArray("map", pi->map, pi->numPages);
  printf("fifo_old:\t\t %d\n", pi->fifo_old);
  printIntArray("lru_stamp", pi->lru_stamp, pi->numPages);
  printStrArray("frames", pi->frames, pi->numPages);

}

/* A function to linearly search a target integer in an integer array.
 * The index of the target is returned if found, -1 if not found.
 * The search start at index 0 and ends at index length-1.
 */
int searchArray(int x, int *a, int length) {
  int i;
  for (i = 0; i < length; i++) {
    if (a[i] == x) return i;
  }
  return -1;
}

/* A function to linearly search the lowest value in an integer array.
 * The search start at index 0 and ends at index length-1.
 */
int searchLowest(int *a, int length) {
  int i;
  int x = a[0];
  int j = 0;
  for (i = 1; i < length; i++) {
    if (a[i] < x) {
      x = a[i];
      j = i;
    }
  }
  return j;
}

/* A function to initialize buffer meta data structure, which are kept in BM_PoolInfo.
 */
RC initPoolInfo(unsigned int numPages, SM_FileHandle *fh, BM_PoolInfo *pi) {
  RC rc_code;
  pi->numPages = numPages;
  pi->fh = fh;
  pi->dirtys = (bool *)malloc(sizeof(bool) * numPages);
  pi->fixCounter = (int *)malloc(sizeof(int) * numPages);
  pi->map = (PageNumber *)malloc(sizeof(PageNumber) * numPages);
  pi->fifo_old = 0;
  pi->lru_stamp = (int *)malloc(sizeof(int) * numPages);
  pi->frames = (char **)malloc(sizeof(char *) * numPages);

  int i, j;
  for (i = 0; i < numPages; i++) {
    pi->dirtys[i] = false;
    pi->fixCounter[i] = 0;
    pi->map[i] = -1;
    pi->lru_stamp[i] = i;
    pi->frames[i] = (char *)malloc(sizeof(char)*PAGE_SIZE);

    for (j = 0; j < PAGE_SIZE; j++) {
      pi->frames[i][j] = '\0';
    }
  }

  return rc_code;
}


/* A function to initialize buffer pool handler.
 */
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData) {

  RC rc_code;

  SM_FileHandle *fHandle = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));
  fHandle->fileName = (char *)malloc(sizeof(char)*256);


  if ((rc_code = openPageFile(pageFileName, fHandle)) != RC_OK) return rc_code;


  bm->pageFile = pageFileName;
  bm->numPages = numPages;
  bm->strategy = strategy;
  // bm->mgmtData = malloc(sizeof(BM_PoolInfo));
  
  BM_PoolInfo *pi = (BM_PoolInfo *)malloc(sizeof(BM_PoolInfo));

  if((rc_code = initPoolInfo(numPages, fHandle, pi)) != RC_OK) return rc_code;
  
  bm->mgmtData = pi;

  NumWriteIO = 0;
  NumReadIO = 0;

  return rc_code;
}

/* A function to free buffer pool handler.
 */
void free_pool(BM_BufferPool *bm) {
  BM_PoolInfo *pi = (BM_PoolInfo *)bm->mgmtData;
  SM_FileHandle *fh = (SM_FileHandle *)pi->fh;

  int i;
  for (i = 0; i < bm->numPages; i++) {
    // printf("free(pi->frames[%d])\n", i);
    free(pi->frames[i]);
  }
  // printf("free(pi->frames)\n");
  free(pi->frames);
  // printf("free(pi->map)\n");
  free(pi->map);
  // printf("free(pi->fixCounter)\n");
  free(pi->fixCounter);
  // printf("free(pi->dirtys)\n");
  free(pi->dirtys);

  // printf("free(fh)\n");
  free(fh);

  // printf("free(pi)\n");
  free(pi);

}

/* A function to shut down the buffer pool, it writes all the dirty pages 
 * to the page file in disk.
 */
RC shutdownBufferPool(BM_BufferPool *const bm) {
  // Needs comprobation of dirty and pinned
  RC rc_code;
  bool pinned_free = true;
  BM_PoolInfo *pi = (BM_PoolInfo *)bm->mgmtData;

  rc_code = forceFlushPool(bm);

  int i;
  for (i = 0; i < bm->numPages; i++) {
    if (pi->fixCounter[i] != 0) pinned_free = false;
  }

  if(pinned_free) {
    
    free_pool(bm);
    
    return RC_OK;

  } else {
    return RC_PINNED_PAGES;
  }

}

/* A function to write all the dirty pages with fixed count 0 to the page file in disk.
 */
RC forceFlushPool(BM_BufferPool *const bm) {
  // read from buffer and write to disk only dirty pages with fixed count 0
  RC rc_code;

  BM_PoolInfo *pi = (BM_PoolInfo *)bm->mgmtData;

  SM_FileHandle *fHandle = pi->fh;

  int i;
  for (i = 0; i < bm->numPages; i++) {
    if ( pi->dirtys[i] && (pi->fixCounter[i] == 0) ) {
      // write page to disk
      SM_PageHandle memPage = pi->frames[i];

      rc_code = writeBlock(pi->map[i], pi->fh, memPage);
      NumWriteIO++;
      if (rc_code != RC_OK)
        return rc_code;
      pi->dirtys[i] = false;
    }
  }

  return rc_code;
}

/* A function to update the fifo page information when needed. 
 * The function finds and updates the oldest page and replace it.
 */
RC readPageFIFO(BM_PoolInfo *const pi, BM_PageHandle *const page, 
      const PageNumber pageNum) {
  RC rc_code = RC_OK;

  int max_index = pi->numPages - 1;
  int index = pi->fifo_old;

  while(pi->fixCounter[index] > 0) {
    if (index >= max_index) {
      index = 0;
    } else {
      index++;
    }
  }

  SM_FileHandle *fh = pi->fh;
  SM_PageHandle memPage = pi->frames[index];

  if (pi->dirtys[index]) {
    writeBlock(pi->map[index], fh, memPage);
    NumWriteIO++;
    pi->dirtys[index] = false;
  }

  if (pageNum >= fh->totalNumPages) {
    rc_code = appendEmptyBlock(fh);
    // NumWriteIO++;
    if (rc_code != RC_OK) return rc_code;
  }

  rc_code = readBlock(pageNum, fh, memPage);
  NumReadIO++;

  page->data = memPage;
  // update control variables
  pi->map[index] = pageNum;
  pi->fixCounter[index]++; // should go from 0 to 1...
  if (pi->fifo_old >= max_index) {
    pi->fifo_old = 0;
  } else {
    pi->fifo_old++;
  }

  return rc_code;
}

/* A function to update the lru page information when needed. 
 */
void update_lru(int index, int *ary, int length) {
  int max = length - 1;
  int pointer;

  int *result;
  result =  (int *)malloc(sizeof(int) * length);
  int i;
  for(i = 0; i <length; i++) {
    *(result + i) = *(ary + i);
  }
  
  pointer = searchArray(max, ary, length);
  while (( pointer != index) && max > 0) {
    result[pointer]--;
    max--;
    pointer = searchArray(max, ary, length);
  }

  result[index] = length - 1;

  for(i = 0; i <length; i++) {
    *(ary + i) = *(result + i);
  }

  free(result);
}

/* A function to update the lru page information when needed. 
 */
RC readPageLRU(BM_PoolInfo *const pi, BM_PageHandle *const page, 
      const PageNumber pageNum) {
  RC rc_code;

  int index = searchLowest(pi->lru_stamp, pi->numPages);

  SM_FileHandle *fh = pi->fh;
  SM_PageHandle memPage = pi->frames[index];

  if (pi->fixCounter[index] > 0) return RC_PINNED_LRU;

  if (pi->dirtys[index]) {
    writeBlock(pi->map[index], fh, memPage);
    NumWriteIO++;
    pi->dirtys[index] = false;
  }

  if (pageNum >= fh->totalNumPages) {
    rc_code = appendEmptyBlock(fh);
    // NumWriteIO++;
    if (rc_code != RC_OK) return rc_code;
  }

  rc_code = readBlock(pageNum, fh, memPage);
  NumReadIO++;
  page->data = memPage;
 
  // update fix count and lru array
  pi->map[index] = pageNum;
  pi->fixCounter[index]++;

  update_lru(index, pi->lru_stamp, pi->numPages);

  return rc_code;
  
}

/* A function to label a page as dirty.
 */
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
  // page->pageNum is dirty, mark it in buffer header
  BM_PoolInfo *pi = (BM_PoolInfo *)bm->mgmtData;
  int index = searchArray(page->pageNum, pi->map, pi->numPages);
  pi->dirtys[index] = true;
  return RC_OK;
}

/* A function to unpin a page when a user finishes using it.
 */
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
  // unpin the page, decrement fix count
  BM_PoolInfo *pi = (BM_PoolInfo *)bm->mgmtData;
  int index = searchArray(page->pageNum, pi->map, pi->numPages);
  pi->fixCounter[index]--;

  // if (bm->strategy == RS_LRU) {
  //   pi->lru_stamp = update_lru(index, pi->lru_stamp, pi->numPages);
  // }
  return RC_OK;
}

/* A function to writes a page to the disk.
 */
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {
  // writes page to disk

  // read fHandle pointer from buffer header
  // from buffer reader, get the pointer to page and store it in memPage
  // writeBlock(page->numPage, fHandle, memPage);
  RC rc_code;
  BM_PoolInfo *pi = (BM_PoolInfo *)bm->mgmtData;
  SM_FileHandle *fh = pi->fh;
  int index = searchArray(page->pageNum, pi->map, pi->numPages);
  rc_code = writeBlock(pi->map[index], fh, pi->frames[index]);
  NumWriteIO++;
  if (rc_code != RC_OK)
    return rc_code;

  pi->dirtys[index] = false;

  return RC_OK;
}

/* A function to pin a page when a user starts using it.
 */
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum) {
  // read header from mgmtData
  // if pageNum already in mgmtData retrieve page pointer to page handler (page)
  // else apply strategy

  // of course, pin the Page, that is write the info to the buffer header
  // and increment fix count

  RC rc_code = RC_OK;

  page->pageNum = pageNum;
  BM_PoolInfo *pi = (BM_PoolInfo *)bm->mgmtData;

  int index = searchArray(pageNum, pi->map, bm->numPages);

  if (index < 0) {
    if (searchArray(0, pi->fixCounter, pi->numPages) < 0) {
      return RC_PINNED_PAGES;
    }

    // Apply strategy
    switch (bm->strategy)
    {
    case RS_FIFO:
      rc_code = readPageFIFO(pi, page, pageNum);
      break;
    case RS_LRU:
      rc_code = readPageLRU(pi, page, pageNum);
      break;
    default:
      printf("Strategy not implemented: %i\n", bm->strategy);
      break;
    }
  } else {
    // Page already on buffer frame!
    page->data = pi->frames[index];
    pi->fixCounter[index]++;
    if (bm->strategy == RS_LRU) {
      update_lru(index, pi->lru_stamp, pi->numPages);
    }
  }
  return rc_code;
}

// Statistics Interface
/* A function to get the frame contents.
 */
PageNumber *getFrameContents (BM_BufferPool *const bm) {
  BM_PoolInfo *pi = (BM_PoolInfo *)bm->mgmtData;
  PageNumber *pn = pi->map;

  printIntArray("Frames map", pn, bm->numPages);

  return pn;
}

/* A function to get the dirty flags.
 */
bool *getDirtyFlags (BM_BufferPool *const bm) {
  BM_PoolInfo *pi = (BM_PoolInfo *)bm->mgmtData;
  bool *df = pi->dirtys;
  
  printBoolArray("Dirty pages", df, bm->numPages);

  return df;
}

/* A function to get the fix counts.
 */
int *getFixCounts (BM_BufferPool *const bm) {
  BM_PoolInfo *pi = (BM_PoolInfo *)bm->mgmtData;
  int *fc = pi->fixCounter;
  
  printIntArray("Fix count", fc, bm->numPages);

  return fc;
}

int getNumReadIO (BM_BufferPool *const bm) {
  return NumReadIO;
}
int getNumWriteIO (BM_BufferPool *const bm) {
  return NumWriteIO;
}
