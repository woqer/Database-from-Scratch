#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <stdlib.h>

static int NumReadIO = 0;
static int NumWriteIO = 0;

typedef struct BM_PoolInfo {
  int numPages;
  SM_FileHandle *fh; // file handler of page file associated with buffer pool
  bool *dirtys;     // dirty flags array
  int *fixCounter;  // fix counter array
  PageNumber *map;  // mapping between page frames and page numbers
  int fifo_old;     // circular FIFO, index of the oldest added item
  int *lru_stamp;         // usedstamp, beggins from 0
  char **frames;    // frames pointer array
} BM_PoolInfo;

void printIntArray(const char* objectName, int *i_a, int length) {
  printf("%s: [", objectName);
  int i;
  for (i = 0; i < (length - 1); i++) {
    printf("%d, ", i_a[i]);
  }
  printf("%d]\n", i_a[length - 1]);
}

void printBoolArray(const char* objectName, bool *i_a, int length) {
  printf("%s: [", objectName);
  int i;
  for (i = 0; i < (length - 1); i++) {
    printf("%s, ", i_a[i] ? "true" : "false");
  }
  printf("%s]\n", i_a[length - 1] ? "true" : "false"); 
}

int searchArray(int x, int *a, int length) {
  int i;
  for (i = 0; i < length; i++) {
    if (a[i] == x) return i;
  }
  return -1;
}

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

// Initialize buffer metadate structure
RC initPoolInfo(unsigned int numPages, SM_FileHandle *fh, BM_PoolInfo *pi) {
  RC rc_code;
  pi->numPages = numPages;
  pi->fh = fh;
  pi->dirtys = (bool *)malloc(sizeof(bool) * numPages);
  pi->fixCounter = (int *)malloc(sizeof(int) * numPages);
  pi->map = (PageNumber *)malloc(sizeof(PageNumber) * numPages);
  pi->fifo_old = 0;
  pi->lru_stamp = (int *)malloc(sizeof(int) * numPages);
  pi->frames = malloc(numPages);

  int i, j;
  for (i = 0; i < numPages; i++) {
    pi->dirtys[i] = false;
    pi->fixCounter[i] = 0;
    pi->map[i] = -1;
    pi->lru_stamp[i] = i;
    pi->frames[i] = (char *)malloc(sizeof(char)*PAGE_SIZE);

    // printf("reading page %d from file %s\n", i, fh->fileName);

    // if(rc_code = readCurrentBlock (fh, (SM_PageHandle)pi->frames[i]) != RC_OK)
    //   return rc_code;

    for (j = 0; j < PAGE_SIZE; j++) {
      pi->frames[i][j] = '\0';
    }
  }

  // printf("end of method initPoolInfo\n");
  return rc_code;
}


// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData) {

  RC rc_code;

  SM_FileHandle *fHandle = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));
  fHandle->fileName = (char *)malloc(sizeof(char)*256);

  // printf("opening %s\n", pageFileName);

  if ((rc_code = openPageFile(pageFileName, fHandle)) != RC_OK) return rc_code;

  // printf("opened %s\n", pageFileName);


  bm->pageFile = pageFileName;
  bm->numPages = numPages;
  bm->strategy = strategy;
  // bm->mgmtData = malloc(sizeof(BM_PoolInfo));
  
  BM_PoolInfo *pi = (BM_PoolInfo *)malloc(sizeof(BM_PoolInfo));

  if((rc_code = initPoolInfo(numPages, fHandle, pi)) != RC_OK) return rc_code;
  
  bm->mgmtData = pi;

  NumWriteIO = 0;
  NumReadIO = 0;

  // printf("numPages: %d\n", pi->numPages);
  // printf("dirtys: %s\n", pi->dirtys[0] ? "true" : "false");
  // printf("fixCounter: %d\n", pi->fixCounter[0]);
  // printf("map: %d\n", pi->map[0]);
  // printf("frames[0]: %s\n", pi->frames[0]);

  // getFrameContents(bm);
  // getDirtyFlags(bm);
  // getFixCounts(bm);

  // Needs the real initialization:
  //    - some buffer header for storing info about dirty and pinned pages --> DONE
  //    - the header will store fHandle pointer ?

  return rc_code;
}

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

      rc_code = writeBlock(pi->map[i], pi->fh, (SM_PageHandle)pi->frames[i]);
      NumWriteIO++;
      if (rc_code != RC_OK)
        return rc_code;
      pi->dirtys[i] = false;
    }
  }

  // rc_code = openPageFile(bm->pageFile, fHandle);

  return rc_code;
}

RC readPageFIFO(BM_PoolInfo *const pi, BM_PageHandle *const page, 
      const PageNumber pageNum) {
  RC rc_code = RC_OK;

  // printf("[fido] .... Begin of readPageFIFO(%d)\n", pageNum);

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
    // printf("[fido] ...... dirty page! writting to disk...\n");
    writeBlock(pi->map[index], fh, memPage);
    NumWriteIO++;
    pi->dirtys[index] = false;
  }

  if (pageNum >= fh->totalNumPages) {
    // printf("[fido] ...... non-existing page, appending new one to disk...\n");
    rc_code = appendEmptyBlock(fh);
    // NumWriteIO++;
    if (rc_code != RC_OK) return rc_code;
  }

  // printf("[fido] ...... reading page from disk...\n");
  rc_code = readBlock(pageNum, fh, memPage);
  NumReadIO++;

  page->data = memPage;
  // update control variables
  // printf("[fido] ...... reading done, updating control variables, index = %d\n",
    // index);
  pi->map[index] = pageNum;
  pi->fixCounter[index]++; // should go from 0 to 1...
  if (pi->fifo_old >= max_index) {
    pi->fifo_old = 0;
  } else {
    pi->fifo_old++;
  }

  return rc_code;
}

int *update_lru(int index, int *ary, int length) {
  int max = length - 1;
  int *result = ary;
  int pointer;

  // if (searchArray(max, ary, length) == index) return ary;

  while ((pointer = searchArray(max, ary, length)) != index) {
    result[pointer]--;
    max--;
  }

  return result;
}

RC readPageLRU(BM_PoolInfo *const pi, BM_PageHandle *const page, 
      const PageNumber pageNum) {
  // printf("[fido] .... Begin of readPageLRU(%d)\n", pageNum);
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
    // printf("[fido] ...... non-existing page, appending new one to disk...\n");
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

  pi->lru_stamp = update_lru(index, pi->lru_stamp, pi->numPages);

  return rc_code;
  
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
  // page->pageNum is dirty, mark it in buffer header
  BM_PoolInfo *pi = (BM_PoolInfo *)bm->mgmtData;
  int index = searchArray(page->pageNum, pi->map, pi->numPages);
  pi->dirtys[index] = true;
  return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
  // unpin the page, decrement fix count
  BM_PoolInfo *pi = (BM_PoolInfo *)bm->mgmtData;
  int index = searchArray(page->pageNum, pi->map, pi->numPages);
  pi->fixCounter[index]--;

  if (bm->strategy == RS_LRU) {
    pi->lru_stamp = update_lru(index, pi->lru_stamp, pi->numPages);
  }
  return RC_OK;
}

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

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum) {
  // read header from mgmtData
  // if pageNum already in mgmtData retrieve page pointer to page handler (page)
  // else apply strategy

  // of course, pin the Page, that is write the info to the buffer header
  // and increment fix count

  // printf("[fido] Begin pinPage(%d)\n", pageNum);

  RC rc_code = RC_OK;

  page->pageNum = pageNum;
  BM_PoolInfo *pi = (BM_PoolInfo *)bm->mgmtData;

  int index = searchArray(pageNum, pi->map, bm->numPages);

  if (index < 0) {
    // printf("[fido] .. Page NOT on buffer, applying strategy...\n");
    if (searchArray(0, pi->fixCounter, pi->numPages) < 0) {
      // printf("[fido] .... WARNING!!! all pages pinned\n");
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
    // printf("[fido] .. Yay! Page already on buffer frame\n");
    page->data = pi->frames[index];
    pi->fixCounter[index]++;
    if (bm->strategy == RS_LRU) {
      pi->lru_stamp = update_lru(index, pi->lru_stamp, pi->numPages);
    }
  }
  // printf("[fido] End pinPage(%d)\n", pageNum);
  return rc_code;
}

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm) {
  BM_PoolInfo *pi = (BM_PoolInfo *)bm->mgmtData;
  PageNumber *pn = pi->map;

  printIntArray("Frames map", pn, bm->numPages);

  return pn;
}

bool *getDirtyFlags (BM_BufferPool *const bm) {
  BM_PoolInfo *pi = (BM_PoolInfo *)bm->mgmtData;
  bool *df = pi->dirtys;
  
  printBoolArray("Dirty pages", df, bm->numPages);

  return df;
}

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
