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
	char zero = '\0';

	header = (char*)malloc(sizeof(char)*HEADER_SIZE);

	sprintf(header, "%04d", totalNumPages);

	fseek(fp, 0L, SEEK_SET);
	if ((size_header = fwrite(header, sizeof(char)*HEADER_SIZE, 1, fp)) < 1) {
		// printf("\nWRITEHEADER(header): size_header %d\n", size_header);
		return RC_FILE_R_W_ERROR;
	}
	if ((size_header = fwrite(&zero, sizeof(char), PAGE_SIZE - HEADER_SIZE, fp)) < 1) {
		// printf("\nWRITEHEADER(zeros): size_header %d\n", size_header);
		return RC_FILE_R_W_ERROR;
	}

	free(header);
	// printf("\nWRITEHEADER(return): size_header %d\n", size_header);

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
	int size_header;
	char zero = '\0';

	if ((fp = fopen(fileName, "w")) == NULL) return RC_FILE_R_W_ERROR;
	
	// First we write the initial file handler information (1 page count)
	if ((size_header = writeHeader(fp, 1)) < 1) {
		// printf("CREATEPAGEFILE(writeHeader): size_header %d\n", size_header);
		return RC_FILE_R_W_ERROR;
	}

	// We fill in with '\0' a single page
	size_wrote = fwrite(&zero, sizeof(char), PAGE_SIZE, fp);
	// printf("CREATEPAGEFILE(fwrite): size_wrote %d\n", size_wrote);

	if (size_wrote != PAGE_SIZE) {
		return RC_FILE_R_W_ERROR;
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

	if ((totalNumPages = readHeader(fp)) < 1) return RC_FILE_R_W_ERROR;

	fHandle->fileName = fileName;
	fHandle->totalNumPages = totalNumPages;
	fHandle->curPagePos = 0;

	fclose(fp);

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
		return RC_FILE_R_W_ERROR;
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

	if ( (totalNumPages = readHeader(fp)) < 1) return RC_FILE_R_W_ERROR;

	if ((pageNum > totalNumPages) || (pageNum < 0)) return RC_READ_NON_EXISTING_PAGE;

	int i;
	for (i = 0; i < pageNum; i++) {
		fread(buff, sizeof(char)*PAGE_SIZE, 1, fp);
	}

	strncpy(memPage, buff, PAGE_SIZE);

	fHandle->curPagePos = pageNum;

	free(buff);
	fclose(fp);

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

	if ((totalNumPages = readHeader(fp)) < 1) return RC_FILE_R_W_ERROR;

	if ((pageNum >= totalNumPages) || (pageNum < 0)) return RC_READ_NON_EXISTING_PAGE;

	int i;
	for (i = 0; i < pageNum; i++) {
		fread(buff, sizeof(char)*PAGE_SIZE, 1, fp);
	}

	if (fwrite(memPage, sizeof(char)*PAGE_SIZE, 1, fp) < 1) return RC_WRITE_FAILED;

	fHandle->curPagePos = pageNum;

	free(buff);
	fclose(fp);

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
	char zero = '\0';

	if (access(fHandle->fileName, R_OK) < 0) return RC_FILE_NOT_FOUND;

	fp = fopen(fHandle->fileName, "r+");

	buff = (char*)malloc(sizeof(char)*PAGE_SIZE);

	if ((totalNumPages = readHeader(fp)) < 1) return RC_FILE_R_W_ERROR;

	rewind(fp);

	if (writeHeader(fp,totalNumPages+1) < 1) return RC_FILE_R_W_ERROR;

	printf("\n");
	printf("APPEND: ftell_pre-fseek %ld\n", ftell(fp));
	fseek(fp, 0L, SEEK_CUR);
	printf("APPEND: ftell_post-fseek %ld\n", ftell(fp));

	for (i = 0; i < totalNumPages; i++) {
		fread(buff, sizeof(char)*PAGE_SIZE, 1, fp);
		printf("APPEND: totalNumPages_header %d\n", totalNumPages);
		printf("APPEND: iteration %d\n", i);
	}
	fseek(fp, 0L, SEEK_CUR);
	fwrite(&zero, sizeof(char), PAGE_SIZE, fp);

	fHandle->totalNumPages++;
	fHandle->curPagePos;

	free(buff);
	fclose(fp);

	// log file info to console
  struct stat fileStat;
  if(stat(fHandle->fileName,&fileStat) < 0)
      return RC_FILE_R_W_ERROR;
  printf("\n\n**APPEND LOG**\n");
  printf("************************************\n");
  printf("File Name: \t\t%s\n",fHandle->fileName);
  printf("File Size: \t\t%lld bytes\n",fileStat.st_size);
  printf("Total # Pages: \t%d\n",fHandle->totalNumPages);
  
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
    
    if ((totalNumPages = readHeader(fp)) < 1) return RC_FILE_R_W_ERROR;
    
    if (numberOfPages > totalNumPages) {
        for (i = 0; i < (numberOfPages - totalNumPages); i++) {
            appendEmptyBlock(fHandle);
        }
    }
    
    // log file info to console
    struct stat fileStat;
    if(stat(fHandle->fileName,&fileStat) < 0)
        return RC_FILE_R_W_ERROR;
    printf("\n\n**ENSURE LOG**\n");
    printf("************************************\n");
    printf("File Name: \t\t%s\n",fHandle->fileName);
    printf("File Size: \t\t%lld bytes\n",fileStat.st_size);
    printf("Total # Pages: \t%d\n",fHandle->totalNumPages);
    
    free(buff);
    fclose(fp);

    return RC_OK;
}
