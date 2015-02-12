#include "storage_mgr.h"
#include "stdlib.h"
#include "unistd.h"
#include "string.h"

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
	size_wrote = fwrite(&zero, sizeof(char), PAGE_SIZE, fp);

	if (size_wrote != PAGE_SIZE) {
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

	free(header);

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
	FILE *fp;
	int totalNumPages;
	char *header, *buff;

	if (access(fHandle->fileName, R_OK) < 0) return RC_FILE_NOT_FOUND;

	fp = fopen(fHandle->fileName, "r");

	header = (char*)malloc(sizeof(char)*4);
	buff = (char*)malloc(sizeof(char)*PAGE_SIZE);

	if (fread(header, sizeof(char)*HEADER_SIZE, 1, fp) < 1) return -1;

	totalNumPages = atoi(header);

	if ((pageNum > totalNumPages) || (pageNum < 0)) return RC_READ_NON_EXISTING_PAGE;

	int i;
	for (i = 0; i < pageNum; i++) {
		fread(buff, sizeof(char)*PAGE_SIZE, 1, fp);
	}

	strncpy(memPage, buff, PAGE_SIZE);

	free(header);
	free(buff);

	return RC_OK;
}

int getBlockPos (SM_FileHandle *fHandle)
{
 return fHandle->curPagePos;
}

RC
readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(1,fHandle,memPage);
}

RC
readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(getBlockPos(fHandle)-1,fHandle,memPage);;
}

RC
readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(getBlockPos(fHandle),fHandle,memPage);;
}

RC
readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(getBlockPos(fHandle)+1,fHandle,memPage);;
}

RC
readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(fHandle->totalNumPages,fHandle,memPage);;
}

/* writing blocks to a page file */

RC
writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	FILE *fp;
	int totalNumPages;
	char *header, *buff;

	if (access(fHandle->fileName, R_OK) < 0) return RC_FILE_NOT_FOUND;

	fp = fopen(fHandle->fileName, "r+");

	header = (char*)malloc(sizeof(char)*4);
	buff = (char*)malloc(sizeof(char)*PAGE_SIZE);

	if (fread(header, sizeof(char)*HEADER_SIZE, 1, fp) < 1) return -1;

	totalNumPages = atoi(header);

	if ((pageNum > totalNumPages) || (pageNum < 0)) return RC_READ_NON_EXISTING_PAGE;

	int i;
	for (i = 0; i < pageNum-1; i++) {
		fread(buff, sizeof(char)*PAGE_SIZE, 1, fp);
	}

	fwrite(memPage, sizeof(char)*PAGE_SIZE, 1, fp);

	free(header);
	free(buff);

	return RC_OK;
}

RC
writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return writeBlock(fHandle->curPagePos, fHandle, memPage);
}


// NOT TESTED, NOT COMPLETED, NOT SURE!!!
RC
appendEmptyBlock (SM_FileHandle *fHandle)
{
	FILE *fp;
	int totalNumPages, i, h_size;
	char *header, *buff;

	if (access(fHandle->fileName, R_OK) < 0) return RC_FILE_NOT_FOUND;

	fp = fopen(fHandle->fileName, "r+");

	header = (char*)malloc(sizeof(char)*4);
	buff = (char*)malloc(sizeof(char)*PAGE_SIZE);

	if (fread(header, sizeof(char)*HEADER_SIZE, 1, fp) < 1) return -1;

	totalNumPages = atoi(header);

	h_size = snprintf(header,4,"%d",totalNumPages+1);

	for (i = h_size; i < HEADER_SIZE; i++) {
		snprintf(header, 4, "0%s", header);
	}

	fseek(fp, 0, SEEK_SET);

	fwrite(header, sizeof(char)*HEADER_SIZE, 1, fp);

	fseek(fp, ftell(fp), SEEK_CUR);
	for (i = 0; i < totalNumPages; i++) {
		fread(buff, sizeof(char)*PAGE_SIZE, 1, fp);
	}
	fseek(fp, ftell(fp), SEEK_CUR);
	fwrite("\0", sizeof(char)*PAGE_SIZE, 1, fp);

	free(header);
	free(buff);

	return RC_OK;
}

RC
ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
	return RC_OK;
}
