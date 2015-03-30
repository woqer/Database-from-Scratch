#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <stdlib.h>

typedef struct BM_PoolInfo {
  int numPages;
  SM_FileHandle *fh; // file handler of page file associated with buffer pool
  bool *dirtys;     // dirty flags array
  int *fixCounter;  // fix counter array
  PageNumber *map;  // mapping between page frames and page numbers
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

// Initialize buffer metadate structure
BM_PoolInfo *initPoolInfo(unsigned int numPages, SM_FileHandle *fh) {
  BM_PoolInfo *pi = (BM_PoolInfo *)malloc(sizeof(BM_PoolInfo));

  pi->numPages = numPages;
  pi->fh = fh;
  pi->dirtys = (bool *)malloc(sizeof(bool) * numPages);
  pi->fixCounter = (int *)malloc(sizeof(int) * numPages);
  pi->map = (PageNumber *)malloc(sizeof(PageNumber) * numPages);
  pi->frames = malloc(numPages);

  int i, j;
  for (i = 0; i < numPages; i++) {
    pi->dirtys[i] = false;
    pi->fixCounter[i] = 0;
    pi->map[i] = i;
    pi->frames[i] = (char *)malloc(sizeof(char)*PAGE_SIZE);
    for (j = 0; j < PAGE_SIZE; j++) {
      pi->frames[i][j] = '\0';
    }
  }

  return pi;
}

void free_pool(BM_BufferPool *bm) {
  BM_PoolInfo *pi = (BM_PoolInfo *)bm->mgmtData;
  SM_FileHandle *fh = (SM_FileHandle *)pi->fh;

  int i;
  for (i = 0; i < bm->numPages; i++) {
    printf("free(pi->frames[%d])\n", i);
    free(pi->frames[i]);
  }
  printf("free(pi->frames)\n");
  free(pi->frames);
  printf("free(pi->map)\n");
  free(pi->map);
  printf("free(pi->fixCounter)\n");
  free(pi->fixCounter);
  printf("free(pi->dirtys)\n");
  free(pi->dirtys);

  printf("free(fh)\n");
  free(fh);

  printf("free(pi)\n");
  free(pi);

}

// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		  const int numPages, ReplacementStrategy strategy, 
		  void *stratData) {
  
  RC rc_code;
  BM_PoolInfo *pi;

  SM_FileHandle *fHandle = (SM_FileHandle *)malloc(sizeof(SM_FileHandle));
  fHandle->fileName = (char *)malloc(sizeof(char)*256);

  if ((rc_code = openPageFile(pageFileName, fHandle)) != RC_OK) return rc_code;

  bm->pageFile = pageFileName;
  bm->numPages = numPages;
  bm->strategy = strategy;
  // bm->mgmtData = malloc(sizeof(BM_PoolInfo));
  
  bm->mgmtData = initPoolInfo(numPages, fHandle);
  pi = (BM_PoolInfo *)(bm->mgmtData);

  printf("numPages: %d\n", pi->numPages);
  printf("dirtys: %s\n", pi->dirtys[0] ? "true" : "false");
  printf("fixCounter: %d\n", pi->fixCounter[0]);
  printf("map: %d\n", pi->map[0]);
  printf("frames[0]: %s\n", pi->frames[0]);

  // getFrameContents(bm);
  // getDirtyFlags(bm);
  // getFixCounts(bm);

  // Needs the real initialization:
  //    - some buffer header for storing info about dirty and pinned pages --> DONE
  //    - the header will store fHandle pointer ?

  return rc_code;
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
      if (rc_code = writeBlock(i, pi->fh, (SM_PageHandle)pi->frames[i]) != RC_OK) return rc_code;
      pi->dirtys[i] = false;
    }
  }

  // rc_code = openPageFile(bm->pageFile, fHandle);

  return rc_code;
}

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page) {
  // page->pageNum is dirty, mark it in buffer header
  return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page) {
  // unpin the page, decrement fix count
  // strategy will take over the replacement in memory
  // making a decition based on unpinned, dirty and fixcount
  return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page) {
  // writes page to disk

  // read fHandle pointer from buffer header
  // from buffer reader, get the pointer to page and store it in memPage
  // writeBlock(page->numPage, fHandle, memPage);


  return RC_OK;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
	    const PageNumber pageNum) {
  // read header from mgmtData
  // if pageNum already in mgmtData retrieve page pointer to page handler (page)
  // else apply strategy

  // of course, pin the Page, that is write the info to the buffer header
  // and increment fix count

  page->pageNum = pageNum;

  // page data should be retrieved from buffer header
  page->data = (char *)malloc(sizeof(char)*PAGE_SIZE); // Fix in the future!
  
  return RC_OK;
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
  return 1;
}
int getNumWriteIO (BM_BufferPool *const bm) {
  return 1;
}
