#include <sys/stat.h>

#include "storage_mgr.h"
#include "stdlib.h"
#include "unistd.h"
#include "string.h"

#define HEADER_SIZE 4

/************************************************************
 *                    Functions definitions                 *
 ************************************************************/
/* manipulating page files */

int
readHeader (FILE *fp)
{
	char *header, *buff;
	
	header = (char*)malloc(sizeof(char)*HEADER_SIZE);
	buff = (char*)malloc(sizeof(char)*PAGE_SIZE);

	fread(header, sizeof(char)*HEADER_SIZE, 1, fp);
	fread(buff, sizeof(char)*(PAGE_SIZE - HEADER_SIZE), 1, fp);

	free(buff);

	return atoi(header);
}

int
writeHeader (FILE *fp, int totalNumPages)
{
	int size_header;
	char *header;

	header = (char*)malloc(sizeof(char)*HEADER_SIZE);

	sprintf(header, "%04d", totalNumPages);

	size_header = fwrite(header, sizeof(char)*HEADER_SIZE, 1, fp);
	size_header = fwrite("\0", sizeof(char), PAGE_SIZE - HEADER_SIZE, fp);

	free(header);

	return size_header;
}

void
initStorageManager (void)
{

}

RC
createPageFile (char *fileName)
{
	FILE *fp;
	size_t size_wrote;

	if ((fp = fopen(fileName, "w")) == NULL) return RC_WRITE_FAILED;
	
	// First we write the initial file handler information (1 page count)
	if (writeHeader(fp, 1) < 1) return RC_WRITE_FAILED;

	// We fill in with '\0' a single page
	size_wrote = fwrite("\0", sizeof(char), PAGE_SIZE, fp);

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
	int totalNumPages;
	
	if (access(fileName, R_OK) < 0) return RC_FILE_NOT_FOUND;
	
	fp = fopen(fileName, "r");

	if ((totalNumPages = readHeader(fp)) < 1) return -1;

	fHandle->fileName = fileName;
	fHandle->totalNumPages = totalNumPages;
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
	FILE *fp;
	int totalNumPages;
	char *buff;

	if (access(fHandle->fileName, R_OK) < 0) return RC_FILE_NOT_FOUND;

	fp = fopen(fHandle->fileName, "r");

	buff = (char*)malloc(sizeof(char)*PAGE_SIZE);

	if ( (totalNumPages = readHeader(fp)) < 1) return -1;

	if ((pageNum > totalNumPages) || (pageNum < 0)) return RC_READ_NON_EXISTING_PAGE;

	int i;
	for (i = 0; i < pageNum; i++) {
		fread(buff, sizeof(char)*PAGE_SIZE, 1, fp);
	}

	strncpy(memPage, buff, PAGE_SIZE);

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
	char *buff;

	if (access(fHandle->fileName, R_OK) < 0) return RC_FILE_NOT_FOUND;

	fp = fopen(fHandle->fileName, "r+");

	buff = (char*)malloc(sizeof(char)*PAGE_SIZE);

	if ( (totalNumPages = readHeader(fp)) < 1) return -1;

	if ((pageNum > totalNumPages) || (pageNum < 0)) return RC_READ_NON_EXISTING_PAGE;

	int i;
	for (i = 0; i < pageNum-1; i++) {
		fread(buff, sizeof(char)*PAGE_SIZE, 1, fp);
	}

	fwrite(memPage, sizeof(char)*PAGE_SIZE, 1, fp);

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
	char *buff;

	if (access(fHandle->fileName, R_OK) < 0) return RC_FILE_NOT_FOUND;

	fp = fopen(fHandle->fileName, "r+");

	buff = (char*)malloc(sizeof(char)*PAGE_SIZE);

	if ((totalNumPages = readHeader(fp)) < 1) return -1;

	fseek(fp, 0, SEEK_SET);

	writeHeader(fp,totalNumPages+1);

	fseek(fp, ftell(fp), SEEK_CUR);
	for (i = 0; i < totalNumPages; i++) {
		fread(buff, sizeof(char)*PAGE_SIZE, 1, fp);
	}
	fseek(fp, ftell(fp), SEEK_CUR);
	fwrite("\0", sizeof(char)*PAGE_SIZE, 1, fp);
    
    // log file info to console
    struct stat fileStat;
    if(stat(fHandle->fileName,&fileStat) < 0)
        return -1;
    printf("\n\n**APPEND LOG**\n");
    printf("************************************\n");
    printf("File Name: \t\t%s\n",fHandle->fileName);
    printf("File Size: \t\t%lld bytes\n",fileStat.st_size);
    printf("Total # Pages: \t%d\n",totalNumPages);
    

	free(buff);

	return RC_OK;
}

RC
ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
    
    FILE *fp;
    int totalNumPages, i;
    char *buff;
    
    if (access(fHandle->fileName, R_OK) < 0) return RC_FILE_NOT_FOUND;
    
    fp = fopen(fHandle->fileName, "r+");
    
    buff = (char*)malloc(sizeof(char)*PAGE_SIZE);
    
    if ((totalNumPages = readHeader(fp)) < 1) return -1;
    
    if (numberOfPages > totalNumPages) {
        for (i = 0; i < (numberOfPages - totalNumPages); i++) {
            appendEmptyBlock(fHandle);
        }
    }
    
    // log file info to console
    struct stat fileStat;
    if(stat(fHandle->fileName,&fileStat) < 0)
        return -1;
    printf("\n\n**ENSURE LOG**\n");
    printf("************************************\n");
    printf("File Name: \t\t%s\n",fHandle->fileName);
    printf("File Size: \t\t%lld bytes\n",fileStat.st_size);
    printf("Total # Pages: \t%d\n",totalNumPages);
    
    return RC_OK;
}
