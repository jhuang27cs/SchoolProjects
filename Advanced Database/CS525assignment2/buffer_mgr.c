

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "buffer_mgr.h"
#include "dberror.h"
#include "storage_mgr.h"

#define MAX_PAGES 20000
#define MAX_FRAMES 200
#define MAX_K 10
/* A frame node consisting one page/frame of a buffer pool*/

typedef struct Node{
    
    int pageNum;            /* the pageNum of page in the pageFile*/
    int frameNum;           /* the number of the frame in the frame list*/
    int dirty;              /* dirty = 1, not dirty = 0*/
    int fixCount;           /* fixCound of the page based on the pinning/un-pinning request*/
    int rf;                 /* reference bit per node to be used by clock*/
    int pageFrequency;     /* frequency of the page per client request for LFU*/
    char *data;             /* actual data pointed bu the framenode.*/
    struct Node *next;
    struct Node *previous;
    
}Node;

/* A frame list with a pointer to head and tail node of tyoe pageFrameNode.*/
typedef struct frameList{
    
    Node *head;    /* head node of the frame list. Should add new/updated/recently used node to head */
    Node *tail;    /* tail node of the frame list. Should be the first/start to remove as per strategy*/
    
}frameList;

/* Required data per buffer pool. */
typedef struct poolInfo{
    
    int numFrames;          /* filled number of frames in the frame list */
    int numRead;            /* number of read done on the buffer pool */
    int numWrite;           /* number of write done on the buffer pool */
    int countPinning;       /* total number of pinning done for the bm */
    void *stratData;
    int pageToFrame[MAX_PAGES];         /* an array from pageNumber to frameNumber. The size should be same as the size of the pageFile.*/
    int frameToPage[MAX_FRAMES];        /* an array from frameNumber to pageNumber. The size should be same as the size of the frame list.*/
    int pageToFrequency[MAX_PAGES];     /* an array mappping pageNumber to pageFrequency. The size should be same as the size of the pageFile.*/
    bool dirtyFlags[MAX_FRAMES];        /* dirtyflags of all the frames.*/
    int fixedCounts[MAX_FRAMES];        /* fixed count of all the frames.*/
    int khist[MAX_PAGES][MAX_K];        /* history of reference of each page in memory*/
    frameList *frames;      /* a pointer to the frame list of current buffer pool*/
    
}poolInfo;

/*Frame list operations*/


/* To make the given node the head of the list.*/
void updateHead(frameList **list, Node *updateNode){
    
    Node *head = (*list)->head;
    
    if(updateNode == (*list)->head || head == NULL || updateNode == NULL){
        return;
    }
    else if(updateNode == (*list)->tail){
        Node *temp = ((*list)->tail)->previous;
        temp->next = NULL;
        (*list)->tail = temp;
    }
    else{
        updateNode->previous->next = updateNode->next;
        updateNode->next->previous = updateNode->previous;
    }
    
    updateNode->next = head;
    head->previous = updateNode;
    updateNode->previous = NULL;
    
    (*list)->head = updateNode;
    (*list)->head->previous = NULL;
    return;
}

/* To find the node with given page number. update rf and fixCount*/
Node *pageInMemory(BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum){
    
    Node *found;
    poolInfo *info = (poolInfo *)bm->mgmtData;
    Node *current = info->frames->head;
    if((info->pageToFrame)[pageNum] != NO_PAGE){
        while(current != NULL){
            if(current->pageNum == pageNum){
                found=current;
                /* provide the client with the data and details of page*/
                page->pageNum = pageNum;
                page->data = found->data;
                
                /* pinned, so increase the fix count and the read-count*/
                found->fixCount++;
                found->rf = 1;
                return found;
                
            }
            current = current->next;
        }
        
    }
    return NULL;
}

/* find node without change the value of rf and fixCount */
Node *findNode(frameList *list, const PageNumber pageNum) {
    Node *current = list -> head;
    while (current != NULL) {
        if (current -> pageNum == pageNum){
            return current;
        }
        current = current -> next;
    }
    return NULL;
}

/* update the frame list*/
RC updateNewFrame(BM_BufferPool *const bm, Node *found, BM_PageHandle *const page, const PageNumber pageNum){
    
    SM_FileHandle fh;
    poolInfo *info = (poolInfo *)bm->mgmtData;
    RC status;
    
    if ((status = openPageFile ((char *)(bm->pageFile), &fh)) != RC_OK){
        return status;
    }
    
    /* If the frame to be replaced is dirty, write it back to the disk.*/
    if(found->dirty == 1){
        if((status = ensureCapacity(pageNum, &fh)) != RC_OK){
            return status;
        }
        
        if((status = writeBlock(found->pageNum,&fh, found->data)) != RC_OK){
            return status;
        }
        (info->numWrite)++;
    }
    
    /* Update the pageToFrame lookup, set the replaceable page's value to NO_PAGE.*/
    (info->pageToFrame)[found->pageNum] = NO_PAGE;
    
    /* Read the data into new frame.*/
    
    if((status = ensureCapacity(pageNum, &fh)) != RC_OK){
        return status;
    }
    
    if((status = readBlock(pageNum, &fh, found->data)) != RC_OK){
        return status;
    }
    
    /* provide the client with the data and details of page*/
    page->pageNum = pageNum;
    page->data = found->data;
    
    (info->numRead)++;
    
    /* Set all the parameters of the new frame, and update the lookup arrays.*/
    found->dirty = 0;
    found->fixCount = 1;
    found->pageNum = pageNum;
    found->rf = 1;
    
    (info->pageToFrame)[found->pageNum] = found->frameNum;
    (info->frameToPage)[found->frameNum] = found->pageNum;
    
    closePageFile(&fh);
    
    return RC_OK;
    
}


/* The page replacement strategies.*/
/* pinPage by FIFO*/
RC pinPage_FIFO (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
    Node *found;
    poolInfo *info = (poolInfo *)bm->mgmtData;
    
    /* Check if page already in memory.*/
    if((found = pageInMemory(bm, page, pageNum)) != NULL){
        return RC_OK;
    }
    
    /* If need to load the page*/
    
    /* If the frames in the memory are less than the total available frames, find out the first free frame from the head. */
    if((info->numFrames) < bm->numPages){
        found = info->frames->head;
        int i = 0;
        
        while(i < info->numFrames){
            found = found->next;
            ++i;
        }
        
        /*increase the frame count*/
        (info->numFrames)++;
        updateHead(&(info->frames), found);
    }
    else{
        /* For new page, if all the frames are filled, find the oldest frame with fix count 0 */
        found = info->frames->tail;
        
        while(found != NULL && found->fixCount != 0){
            found = found->previous;
        }
        
        if (found == NULL){
            return RC_NO_MORE_SPACE_IN_BUFFER;
        }
        
        updateHead(&(info->frames), found);
    }
    
    RC status;
    /*Updata the frame list*/
    if((status = updateNewFrame(bm, found, page, pageNum)) != RC_OK){
        return status;
    }
    
    return RC_OK;
}

/*pinPage with Least Recently Used strategy*/
RC pinPage_LRU (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
    Node *found;
    poolInfo *info = (poolInfo *)bm->mgmtData;
    
    /* Check if page already in memory. */
    if((found = pageInMemory(bm, page, pageNum)) != NULL){
        /* Put this frame to the head of the frame list, because it is the latest used frame. */
        updateHead(&(info->frames), found);
        return RC_OK;
    }
    
    /* If need to load the page*/
    
    /* If the frames in the memory are less than the total available frames, find out the first free frame from the head. */
    if((info->numFrames) < bm->numPages){
        found = info->frames->head;
        
        int i = 0;
        while(i < info->numFrames){
            found = found->next;
            ++i;
        }
        /*increase the frame count*/
        (info->numFrames)++;
    }
    else{
        /* For new page, if all the frames are filled, then try to find a frame with fixed count 0
         * starting from the tail. Because we always put the LRU page in the tail*/
        found = info->frames->tail;
        
        while(found != NULL && found->fixCount != 0){
            found = found->previous;
        }
        
        /* If reached to head, it means no frames with fixed count 0 available.*/
        if (found == NULL){
            return RC_NO_MORE_SPACE_IN_BUFFER;
        }
    }
    
    RC status;
    
    if((status = updateNewFrame(bm, found, page, pageNum)) != RC_OK){
        return status;
    }
    
    /* Put this frame to the head of the frame list, because it is the latest used frame. */
    updateHead(&(info->frames), found);
    
    return RC_OK;
}

/* pinPage with LRU_K, The basic idea of LRU-K is to keep track of the times of the last K references
 to popular database pages*/
RC pinPage_LRU_K (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
    Node *found;
    poolInfo *info = (poolInfo *)bm->mgmtData;
    int K = (int)(info->stratData);
    int i;
    (info->countPinning)++;
    
    /* Check if page already in memory.*/
    if((found = pageInMemory(bm, page, pageNum)) != NULL){
        
        for(i = K-1; i>0; i--){
            info->khist[found->pageNum][i] = info->khist[found->pageNum][i-1];
        }
        
        info->khist[found->pageNum][0] = info->countPinning;
        
        return RC_OK;
    }
    /* If need to load the page*/
    
    /* If the frames in the memory are less than the total available frames, find out the first free frame from the head. */
    if((info->numFrames) < bm->numPages){
        found = info->frames->head;
        
        int i = 0;
        while(i < info->numFrames){
            found = found->next;
            ++i;
        }
        
        /*This frame will be filled up now, so increase the frame count*/
        (info->numFrames)++;
        updateHead(&(info->frames), found);
    }
    else{
        /* For new page, if all the frames are filled, then try to find a frame with fixed count 0
         * starting from the tail.*/
        Node *current;
        Node *result;
        
        current = info->frames->head;
        bool flag=FALSE;
        while (current !=NULL)
        {
            /* if there are some page with the level K equal to 0 and fixCount == 0, we can simple replace the last one*/
            if ((info->khist[current->pageNum][K-1])==0 && current->fixCount == 0)
            {
                flag=TRUE;
                result=current;
            }
            current=current->next;
        }
        /*flag == FALSE means we have to remove the page with level K not equal to 0 and fixCount == 0*/
        if (flag ==FALSE ) {
            current = info->frames->head;
            result=current;
            while(current != NULL){
                if(current->fixCount == 0 && info->khist[current->pageNum][0]<=info->khist[result->pageNum][0] ){
                    if (info->khist[current->pageNum][K-1]!=0)
                    {
                        result = current;
                    }
                }
                current = current->next;
            }
        }
        if (result == NULL) {
            return RC_NO_MORE_SPACE_IN_BUFFER;
            
        }
        found=result;
    }
    RC status;
    
    if((status = updateNewFrame(bm, found, page, pageNum)) != RC_OK){
        return status;
    }
    
    for(i = K-1; i>0; i--){
        info->khist[found->pageNum][i] = info->khist[found->pageNum][i-1];
    }
    info->khist[found->pageNum][0] = info->countPinning;
    
    return RC_OK;
}

/*pinPage with Least Frequency Used*/
RC pinPage_LFU (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
    Node *found;
    Node *result;
    poolInfo *info = (poolInfo *)bm->mgmtData;
    page->data = (SM_PageHandle) malloc(PAGE_SIZE);
    
    /* Check if page already in memory. */
    if ((found = pageInMemory(bm, page, pageNum)) != NULL) {
        found->pageFrequency++;
        return RC_OK;
    }
    /* If need to load the page*/
    
    /* For new page, if the frames in the memory is less than the total available frames, just find out the first free frame from the head. */
    
    if((info->numFrames) < bm->numPages){
        found = info->frames->head;
        
        int i = 0;
        while(i < info->numFrames){
            found = found->next;
            ++i;
        }
        /*This frame will be filled up now, so increase the frame count*/
        (info->numFrames)++;
        updateHead(&(info->frames), found);
    }
    
    
    else{
        /* For new page, if all the frames are filled, find the oldest frame with fix count 0 */
        found = info->frames->tail;
        
        /*Check if there is a page with fixCount == 0*/
        while(found != NULL && found->fixCount!=0) {
            
            found = found->previous;
        }
        
        result = found;
        if (result==NULL)
        {
            return RC_NO_MORE_SPACE_IN_BUFFER_LFU;
        }
        /*find the page with lowest pageFrequency and fixCount == 0*/
        while (found != NULL) {
            
            if (found->pageFrequency<result->pageFrequency){
                if(found ->fixCount == 0){
                    result=found;
                }
            }
            found = found -> previous;
        }
        found = result;
        updateHead(&(info->frames), found);
        
    }
    
    
    int status;
    if((status = updateNewFrame(bm, found, page, pageNum)) != RC_OK){
        return status;
    }
    
    
    return RC_OK;
}

/*pinPage with CLOCK*/
RC pinPage_CLOCK (BM_BufferPool *const bm, BM_PageHandle *const page,const PageNumber pageNum)
{
    Node *found;
    poolInfo *info = (poolInfo *)bm->mgmtData;
    
    /*Page is already in memory*/
    if((found = pageInMemory(bm, page, pageNum)) != NULL){
        return RC_OK;
    }
    
    /* Page is not in memory */
    
    
    else{
        /* retrieve first frame with rf = 0 and set all bits to zero along the way*/
        Node *result=info->frames->head;
        while (result != NULL && result->rf == 1){
            if (result->fixCount == 0){
                result->rf = 0;
            }
            result = result->next;
        }
        /*result == NULL means the we already in the end, and all page in the frame were rf == 1
         if we retrieval the list again, we can find the rf == 0 and fixCount == 0. 
         if we can't, then return no more space*/
        if (result == NULL){
            result=info->frames->head;
            while (result != NULL && result->rf == 1){
                if (result->fixCount == 0) {
                    result->rf = 0;
                }
                result = result->next;
                }
            }
        if (result == NULL) {
            return RC_NO_MORE_SPACE_IN_BUFFER;
        }
    
        found = result;
    }
    
    RC status;
    
    /*call updateNewFrame with the new value of found*/
    if((status = updateNewFrame(bm, found, page, pageNum)) != RC_OK){
        return status;
    }
    
    return RC_OK;
}

/* To create a new node for frame list*/
Node *initNode(){
    
    Node *node = malloc(sizeof(Node));
    node->pageNum = NO_PAGE;
    node->frameNum = 0;
    node->dirty = 0;
    node->fixCount = 0;
    node->rf = 0;
    node->data =  calloc(PAGE_SIZE, sizeof(SM_PageHandle));
    node->next = NULL;
    node->previous = NULL;
    node->pageFrequency = 0;
    
    return node;
}

/*Buffer Pool Functions*/

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData)
{
    int i;
    SM_FileHandle fh;
    
    if(numPages <= 0){
        return RC_INVALID_BM;
    }
    
    if (openPageFile ((char *)pageFileName, &fh) != RC_OK){
        return RC_FILE_NOT_FOUND;
    }
    
    /* Initialize the data for mgmtInfo.*/
    poolInfo *info = malloc(sizeof(poolInfo));
    
    info->numFrames = 0;
    info->numRead = 0;
    info->numWrite = 0;
    info->stratData = stratData;
    info->countPinning = 0;
    
    /* Initialize the lookup arrays with 0 values.*/
    memset(info->frameToPage,NO_PAGE,MAX_FRAMES*sizeof(int));
    memset(info->pageToFrame,NO_PAGE,MAX_PAGES*sizeof(int));
    memset(info->dirtyFlags,NO_PAGE,MAX_FRAMES*sizeof(bool));
    memset(info->fixedCounts,NO_PAGE,MAX_FRAMES*sizeof(int));
    memset(info->khist, -1, sizeof(&(info->khist)));
    memset(info->pageToFrequency,0,MAX_PAGES*sizeof(int));
    
    /* Creating the linked list of empty frames. */
    info->frames = malloc(sizeof(frameList));
    
    info->frames->head = info->frames->tail = initNode();
    
    for(i = 1; i<numPages; ++i){
        info->frames->tail->next = initNode();
        info->frames->tail->next->previous = info->frames->tail;
        info->frames->tail = info->frames->tail->next;
        info->frames->tail->frameNum = i;
    }
    
    bm->numPages = numPages;
    bm->pageFile = (char*) pageFileName;
    bm->strategy = strategy;
    bm->mgmtData = info;
    
    closePageFile(&fh);
    
    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_BM;
    }
    RC status;
    
    if((status = forceFlushPool(bm)) != RC_OK){
        return status;
    }
    
    poolInfo *info = (poolInfo *)bm->mgmtData;
    Node *current = info->frames->head;
    
    while(current != NULL){
        current = current->next;
        free(info->frames->head->data);
        free(info->frames->head);
        info->frames->head = current;
    }
    
    info->frames->head = info->frames->tail = NULL;
    free(info->frames);
    free(info);
    
    bm->numPages = 0;
    
    return RC_OK;
}

RC forceFlushPool(BM_BufferPool *const bm)
{
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_BM;
    }
    
    poolInfo *info = (poolInfo *)bm->mgmtData;
    Node *current = info->frames->head;
    
    SM_FileHandle fh;
    
    if (openPageFile ((char *)(bm->pageFile), &fh) != RC_OK){
        return RC_FILE_NOT_FOUND;
    }
    
    while(current != NULL){
        if(current->dirty == 1){
            if(writeBlock(current->pageNum, &fh, current->data) != RC_OK){
                return RC_WRITE_FAILED;
            }
            current->dirty = 0;
            (info->numWrite)++;
        }
        current = current->next;
    }
    
    closePageFile(&fh);
    
    return RC_OK;
}

/*Page Management Functions*/

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_BM;
    }
    
    poolInfo *info = (poolInfo *)bm->mgmtData;
    Node *found;
    
    /* Locate the page to be marked as dirty.*/
    if((found = findNode(info->frames, page->pageNum))== NULL){
        return RC_NON_EXISTING_PAGE_IN_FRAME;
    }
    
    /* Mark the page as dirty */
    found->dirty = 1;
    
    return RC_OK;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_BM;
    }
    
    poolInfo *info = (poolInfo *)bm->mgmtData;
    Node *found;
    
    /* Locate the page to be unpinned */
    if((found = findNode(info->frames, page->pageNum)) == NULL){
        return RC_NON_EXISTING_PAGE_IN_FRAME;
    }
    
    /* Unpinned so decrease the fixCount by 1 */
    if(found->fixCount > 0){
        found->fixCount--;
    }
    else{
        return RC_NON_EXISTING_PAGE_IN_FRAME;
    }
    
    return RC_OK;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)

{
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_BM;
    }
    
    poolInfo *info = (poolInfo *)bm->mgmtData;
    Node *found;
    SM_FileHandle fh;
    
    if (openPageFile ((char *)(bm->pageFile), &fh) != RC_OK){
        return RC_FILE_NOT_FOUND;
    }
    
    /* Locate the page to be forced on the disk */
    if((found = findNode(info->frames, page->pageNum)) == NULL){
        closePageFile(&fh);
        return RC_NON_EXISTING_PAGE_IN_FRAME;
        
    }
    
    /* write the current content of the page back to the page file on disk */
    if(writeBlock(found->pageNum, &fh, found->data) != RC_OK){
        closePageFile(&fh);
        return RC_WRITE_FAILED;
    }
    
    (info->numWrite)++;
    
    closePageFile(&fh);
    
    return  RC_OK;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page,
            const PageNumber pageNum)
{
    if (!bm || bm->numPages <= 0){
        return RC_INVALID_BM;
    }
    if(pageNum < 0){
        return RC_READ_NON_EXISTING_PAGE;
    }
    
    switch (bm->strategy)
    {
        case RS_FIFO:
            return pinPage_FIFO(bm,page,pageNum);
            break;
        case RS_LRU:
            return pinPage_LRU(bm,page,pageNum);
            break;
        case RS_CLOCK:
            return pinPage_CLOCK(bm,page,pageNum);
            break;
        case RS_LFU:
            return pinPage_LFU(bm,page,pageNum);
            break;
        case RS_LRU_K:
            return pinPage_LRU_K(bm,page,pageNum);
            break;
        default:
            return RC_UNKNOWN_STRATEGY;
            break;
    }
    return RC_OK;
}

/*Statistics Functions*/


PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    /*return the value of frameToPage*/
    return ((poolInfo *)bm->mgmtData)->frameToPage;
}

bool *getDirtyFlags (BM_BufferPool *const bm)
{
    /*goes through the list of frames and updates the value of dirtyFlags accordingly*/
    poolInfo *info = (poolInfo *)bm->mgmtData;
    Node *cur = info->frames->head;
    
    while (cur != NULL){
        (info->dirtyFlags)[cur->frameNum] = cur->dirty;
        cur = cur->next;
    }
    
    return info->dirtyFlags;
}

int *getFixCounts (BM_BufferPool *const bm)
{
    /*goes through the list of frames and updates the value of fixedCounts accordingly*/
    poolInfo *info = (poolInfo *)bm->mgmtData;
    Node *cur = info->frames->head;
    
    while (cur != NULL){
        (info->fixedCounts)[cur->frameNum] = cur->fixCount;
        cur = cur->next;
    }
    
    return info->fixedCounts;
}

int getNumReadIO (BM_BufferPool *const bm)
{
    /*returns the value of numRead*/
    return ((poolInfo *)bm->mgmtData)->numRead;
}

int getNumWriteIO (BM_BufferPool *const bm)
{
    /*returns the value of numWrite*/
    return ((poolInfo *)bm->mgmtData)->numWrite;
}