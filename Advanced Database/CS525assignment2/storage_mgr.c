//
//  storage_mgr.c
//  CS525assignment
//
//  Created by Jie Huang on 9/4/15.
//  Copyright (c) 2015 Jie Huang. All rights reserved.
//

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <string.h>
#include <fcntl.h>
#include "storage_mgr.h"
#include "dberror.h"

/************************************************************
 *                    interface                             *
 ************************************************************/
/* manipulating page files */

#define MAX_FILE_HANDLE 256 //define the most files can be handled
#define BYTES_TO_PAGE(bytes) ((bytes-1) / PAGE_SIZE)
#define PAGE_OFFSET(pageNo)  (pageNo * PAGE_SIZE)


// Storage manager
typedef struct sManager {
    SM_FileHandle* openHandles[MAX_FILE_HANDLE];
    int handleCount;
}sManager;
// Get the last page number based on file size.
// We can alternatively store last page number within
// the page file, but it is not necessary for now.
static int getLastPageNo(char* fileName)
{
    struct stat st;
    stat(fileName, &st);
    return BYTES_TO_PAGE(st.st_size);
}

static sManager storageManager; // It will be initialized late

// Check if the handle list open or not
static RC checkFileHandle(SM_FileHandle *fHandle)
{
    int i;
    int handleCount= storageManager.handleCount;
    for(i=0; i<MAX_FILE_HANDLE && handleCount; i++)
    {
        if (storageManager.openHandles[i] == 0)
            continue;
        handleCount--;
        if (storageManager.openHandles[i] == fHandle)
            return RC_OK;
    }
    
    return RC_FILE_HANDLE_NOT_INIT;
}

// Add fHandle to list
static RC addFileHandle(SM_FileHandle *fHandle)
{
    int i;
    for(i=0; i<MAX_FILE_HANDLE; i++)
        if (storageManager.openHandles[i] == 0)
        {
            storageManager.openHandles[i]= fHandle;
            storageManager.handleCount++;
            return RC_OK;
        }
    
    return RC_FILE_HANDLE_NOT_INIT;
}

// Delete the fHandle from the list
static RC deleteFileHandle(SM_FileHandle *fHandle)
{
    int i;
    int handleCount= storageManager.handleCount;
    for(i=0; i<MAX_FILE_HANDLE && handleCount; i++)
    {
        if (!storageManager.openHandles[i])
            continue;
        handleCount--;
        if (storageManager.openHandles[i] == fHandle)
        {
            storageManager.openHandles[i]= 0;
            return RC_OK;
        }
    }
    
    return RC_FILE_HANDLE_NOT_INIT;
}


void initStorageManager (void){
}


/* Create page file */
RC createPageFile (char *fileName)
{
    int fd;
    // Create file if not exists
    fd = open(fileName, O_CREAT|O_EXCL|O_RDWR, S_IRWXU);
        if (fd > 0 )
    {
        // Add 1 page with zerobytes of page size
        char initPage[PAGE_SIZE];
        memset(initPage, 0, PAGE_SIZE);
        if (write(fd, initPage, PAGE_SIZE) < PAGE_SIZE)
        {
            close(fd);
            return RC_WRITE_FAILED;
        }
        
        return RC_OK;
    }
    return RC_FILE_CREATE_FAILED;
}

/* Open the page file and add it to the handle list*/
RC openPageFile (char *fileName, SM_FileHandle *fHandle)
{
    
    
    // Check if the file handle has been open or not
    if (checkFileHandle(fHandle) == RC_OK)
        return RC_FILE_OPENED;
    FILE *fd;
    fd = fopen(fileName, "r+");
    
    if (fd)
    {
        // Initialize the fHandle
        fHandle->fileName= (char*) malloc(strlen(fileName)+1);
        strcpy(fHandle->fileName, fileName);
        fHandle->curPagePos= 0;
        fHandle->totalNumPages= getLastPageNo(fHandle->fileName)+1;
        fHandle->mgmtInfo= fd;
        
        // add the fHandle to the handle list
        addFileHandle(fHandle);
        
        return RC_OK;
    }
    return RC_FILE_NOT_FOUND;
}

/* Close the page file and delete it from the handle list*/
RC closePageFile (SM_FileHandle *fHandle)
{
    
    // Check if the file handle has been open or not
    if (checkFileHandle(fHandle) != RC_OK)
        return RC_FILE_HANDLE_NOT_INIT;
    
    // Close the file
    int check = fclose(fHandle->mgmtInfo);
    if (check < 0)
        return RC_FILE_CLOSE_FAILED;
    
    // delete the fHandle to the handle list
    deleteFileHandle(fHandle);
    fHandle->mgmtInfo= NULL;
    return RC_OK;
}


/* Destroy the page file  */
RC destroyPageFile (char *fileName)
{
    // Remove the file
    if (unlink(fileName) < 0)
        return RC_FILE_NOT_FOUND;
    
    return RC_OK;
}




/* read page from opened file */

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    
    int seekPage;
    size_t result;
    if (checkFileHandle(fHandle) != RC_OK)
        return RC_FILE_NOT_FOUND;
    
    /* checks if the page number is valid */
    if (pageNum > fHandle->totalNumPages ){
        return RC_READ_NON_EXISTING_PAGE;
    }
    
    if (pageNum <0 ){
        return RC_READ_NON_EXISTING_PAGE;
    }
    /* checks if file is open */
    if (fHandle->mgmtInfo == NULL){
        return RC_FILE_NOT_FOUND;
    }
    
    seekPage = fseek(fHandle->mgmtInfo, pageNum*PAGE_SIZE, 0);
    
    /* checks if the file seek was successful. If yes, reads the file page into mempage. */
    if (seekPage == 0){
        result = fread(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
        fHandle->curPagePos = pageNum;
        return RC_OK;
    }
    else{
        return RC_READ_NON_EXISTING_PAGE;
    }
}

int getBlockPos (SM_FileHandle *fHandle){
    return fHandle->curPagePos;
}

/*
 * Read the first block by providing the pageNum argument as 0 to the readBlocklock function.
 */
RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(0, fHandle, memPage);
}

/*
 * Read the previous block by providing the pageNum argument as curPagePos-1 to the readBlock function.
 */
RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->curPagePos-1, fHandle, memPage);
}

/*
 * Read the current block by providing the pageNum argument as curPagePos to the readBlock function.
 */
RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

/*
 *Read the next block by providing the pageNum argument as curPagePos+1 to the readBlock function.
 */
RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->curPagePos+1, fHandle, memPage);
}

/*
 *Read the last block by providing the pageNum argument as totalNumPages to the readBlock function.
 */
RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage){
    return readBlock(fHandle->totalNumPages, fHandle, memPage);
}





/* write blocks to a opened page file */
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage){
    int s;
    int w;
    if (checkFileHandle(fHandle) != RC_OK)
        return RC_FILE_NOT_FOUND;
    
    /* Checks if the page number given by the user is less than total number of pages in a file i.e page number is valid 
       If the page number entered is greater than the total number return an error message
     */
    if ((fHandle->totalNumPages) < pageNum || pageNum < 0 ){
        return RC_WRITE_FAILED;
    }
    
    s = fseek(fHandle->mgmtInfo, pageNum*PAGE_SIZE, 0);
    
    if (s == 0){
        w = fwrite(memPage, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
        if (w != PAGE_SIZE){
            return RC_WRITE_ERROR;
        }
        
    }
    return RC_OK;
}
/* Write the current block by passing the cuPagePos into the writeBlock function.*/
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage){
        return writeBlock(fHandle->curPagePos, fHandle, memPage);
}
    
/*Find the end of the page file and attach one more page after it*/
RC appendEmptyBlock(SM_FileHandle *fHandle){
        int s;
        int w;
        char New[PAGE_SIZE] = { 0 };
        if (checkFileHandle(fHandle) != RC_OK)
            return RC_FILE_NOT_FOUND;
        
        s = fseek(fHandle->mgmtInfo, (fHandle->totalNumPages*PAGE_SIZE), 0);
        
        if (s == 0){
            w = fwrite(New, sizeof(char), PAGE_SIZE, fHandle->mgmtInfo);
            if (w != PAGE_SIZE){
                return RC_WRITE_ERROR;
            }
            
            fHandle -> curPagePos = fHandle -> totalNumPages + 1;                //move curPagePos to the last new block
            fHandle -> totalNumPages = fHandle -> totalNumPages + 1;
        }
    return RC_OK;

}
 /*Check if the page file still has enough capacity,if not use appendEmptyBlock function to create more pages*/
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle){
        if (checkFileHandle(fHandle) != RC_OK)
            return RC_FILE_NOT_FOUND;
        if (numberOfPages > fHandle->totalNumPages)
        {
            int MorePages = numberOfPages - (fHandle->totalNumPages);
            int a;
            for (a = 0; a < MorePages; a++){
                appendEmptyBlock(fHandle);
                
            }
        }
        
        return RC_OK;
}
