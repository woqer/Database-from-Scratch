#include <sys/stat.h>

#include "storage_mgr.h"
#include "stdlib.h"
#include "unistd.h"
#include "string.h"

#define HEADER_SIZE 4

/************************************************************
 *                    Functions definitions                 *
 ************************************************************/

/* Read the header page and place the file pointer to the next page, where the actual data starts. 
 * Return the value in header as an integer, which is the number of pages in the file.
 */
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

/* Write the value of totalNumPages to the header page, appending '\0' after it in header page.
 * If there's error during writing header or '\0', return RC_FILE_R_W_ERROR.
 */
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

/* Create a new page file with fileName. The initial file size is one page,
 * fill this single page with '\0' bytes. 
 * Write the initial number of pages to the header, which is 1.
 * If there's error during writing header or '\0', return RC_FILE_R_W_ERROR.
 */
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

/* Opens an existing page file, if it does not exist, return RC_FILE_NOT_FOUND.
 * Then read header first, and the fields of this file handle are initialized 
 * with the information about the opened file.
 */
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

/* Close page file by changing the curPagePos to 0.
 */
RC
closePageFile (SM_FileHandle *fHandle)
{
	fHandle->curPagePos = 0;
	return RC_OK;
}

/* Destroy page file by removing the file from disc.
 */
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

/* Reads the pageNumth block from a file and stores its content in the memory 
 * pointed to by the memPage page handle.
 * Check if the pageNum is valid (should be >= 0 and <= totalNumPages)
 * The method reads all pages from the first one to the pageNumth to the buffer
 * then copy the last page from buffer to memPage. Set curPagePos = pageNum.
 */
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

	//check if pageNum is valid
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

/* Return the current page position in a file. */
int getBlockPos (SM_FileHandle *fHandle)
{
	return fHandle->curPagePos;
}

/* Read the first page in the file by calling readBlock() with pageNum = 1. */
RC
readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(1,fHandle,memPage);
}

/* Read previous page relative to the curPagePos of the file, 
 * by calling readBlock() with pageNum = curPagePos - 1.
 * The value of curPagePos was set to curPagePos - 1 after reading.
 */
RC
readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(getBlockPos(fHandle)-1,fHandle,memPage);;
}

/* Read current page according to the curPagePos of the file.
 * by calling readBlock() with pageNum = curPagePos.
 */
RC
readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(getBlockPos(fHandle),fHandle,memPage);;
}

/* Read next page relative to the curPagePos of the file
 * by calling readBlock() with pageNum = curPagePos + 1.
 * The value of curPagePos was set to curPagePos + 1 after reading.
 */
RC
readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(getBlockPos(fHandle)+1,fHandle,memPage);;
}

/* Read last page relative to the curPagePos of the file
 * by calling readBlock() with pageNum = totalNumPages.
 * The value of curPagePos was set to totalNumPages after reading.
 */
RC
readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return readBlock(fHandle->totalNumPages-1,fHandle,memPage);;
}

/* Write what is in the memPage to the pageNumth block.
 * The value of curPagePos was set to pageNum after writing.
 */
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
	
	//check if pageNum is valid 
	if ((pageNum >= totalNumPages) || (pageNum < 0)) return RC_READ_NON_EXISTING_PAGE;

	//read till the (pageNum - 1)th page
	int i;
	for (i = 0; i < pageNum; i++) {
		fread(buff, sizeof(char)*PAGE_SIZE, 1, fp);
	}
	
	//write memPage to the pageNum
	if (fwrite(memPage, sizeof(char)*PAGE_SIZE, 1, fp) < 1) return RC_WRITE_FAILED;

	fHandle->curPagePos = pageNum;

	free(buff);
	fclose(fp);

	return RC_OK;
}

/* Write the current block in the file by calling writeBlock() with pageNum = curPagePos. */
RC
writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return writeBlock(fHandle->curPagePos, fHandle, memPage);
}


/* Increase the number of pages in the file by one. 
 * The new last page is filled with zero bytes.
 */
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

	//read to the end of the page file
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

/* If the file has less than numberOfPages pages, then increase the size to numberOfPages. 
 * The increasing is done by appending (numberOfPages - totalNumPages) empty blocks 
 * at the end of file.
 */
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
