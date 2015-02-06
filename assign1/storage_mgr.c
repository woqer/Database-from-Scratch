#include "storage_mgr.h"
#include "stdlib.h"
#include "unistd.h"

#define HEADER_SIZE 4

/************************************************************
 *                    Functions definitions                 *
 ************************************************************/
/* manipulating page files */

void
initStorageManager (void)
{

}

RC
createPageFile (char *fileName)
{
	FILE *fp;
	size_t size_wrote, size_header;
	char zero = '\0';

	if ((fp = fopen(fileName, "w")) == NULL) return RC_WRITE_FAILED;
	
	// First we write the initial file handler information (1 page count)
	size_header = fwrite("0001", sizeof(char)*HEADER_SIZE, 1, fp);

	// We fill in with '\0' a single page
	size_wrote = fwrite(&zero, sizeof(char), PAGE_SIZE-HEADER_SIZE, fp);

	if (((size_header*HEADER_SIZE) + size_wrote) != PAGE_SIZE) {
		return RC_WRITE_FAILED;
	} else {
		fclose(fp);
		return RC_OK;
	}
	
}

RC
openPageFile (char *fileName, SM_FileHandle *fHandle)
{
	FILE *fp;
	char *header;
	
	if (access(fileName, R_OK) < 0) return RC_FILE_NOT_FOUND;
	
	fp = fopen(fileName, "r");

	header = (char*)malloc(sizeof(char)*4);

	if (fread(header, sizeof(char)*HEADER_SIZE, 1, fp) < 1) return -1;

	fHandle->fileName = fileName;
	fHandle->totalNumPages = atoi(header);
	fHandle->curPagePos = 0;
	return RC_OK;
}

RC
closePageFile (SM_FileHandle *fHandle)
{
	fHandle->curPagePos = 0;
	return RC_OK;
}

RC
destroyPageFile (char *fileName)
{
	if (access(fileName, R_OK) < 0) return RC_FILE_NOT_FOUND;
	if (remove(fileName) < 0) {
		return RC_WRITE_FAILED;
	} else {
		return RC_OK;
	}
}

/* reading blocks from disc */

RC
readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return RC_OK;
}

int getBlockPos (SM_FileHandle *fHandle)
{

}

RC
readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return RC_OK;
}

RC
readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return RC_OK;
}

RC
readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return RC_OK;
}

RC
readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return RC_OK;
}

RC
readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return RC_OK;
}

/* writing blocks to a page file */

RC
writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return RC_OK;
}

RC
writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return RC_OK;
}

RC
appendEmptyBlock (SM_FileHandle *fHandle)
{
	return RC_OK;
}

RC
ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
	return RC_OK;
}
