#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <math.h>
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include "tables.h"

/* Copy from the re_serializer file */

typedef struct VarString {
    char *buf;
    int size;
    int bufsize;
} VarString;

#define MAKE_VARSTRING(var)				\
do {							\
var = (VarString *) malloc(sizeof(VarString));	\
var->size = 0;					\
var->bufsize = 100;					\
var->buf = malloc(100);				\
} while (0)

#define FREE_VARSTRING(var)			\
do {						\
free(var->buf);				\
free(var);					\
} while (0)

#define GET_STRING(result, var)			\
do {						\
result = malloc((var->size) + 1);		\
memcpy(result, var->buf, var->size);	\
result[var->size] = '\0';			\
} while (0)

#define RETURN_STRING(var)			\
do {						\
char *resultStr;				\
GET_STRING(resultStr, var);			\
FREE_VARSTRING(var);			\
return resultStr;				\
} while (0)

#define ENSURE_SIZE(var,newsize)				\
do {								\
if (var->bufsize < newsize)					\
{								\
int newbufsize = var->bufsize;				\
while((newbufsize *= 2) < newsize);			\
var->buf = realloc(var->buf, newbufsize);			\
}								\
} while (0)

#define APPEND_STRING(var,string)					\
do {									\
ENSURE_SIZE(var, var->size + strlen(string));			\
memcpy(var->buf + var->size, string, strlen(string));		\
var->size += strlen(string);					\
} while(0)

#define APPEND(var, ...)			\
do {						\
char *tmp = malloc(10000);			\
sprintf(tmp, __VA_ARGS__);			\
APPEND_STRING(var,tmp);			\
free(tmp);					\
} while(0)
RC
attrOffset (Schema *schema, int attrNum, int *result)
{
    int offset = 0;
    int attrPos = 0;
    
    for(attrPos = 0; attrPos < attrNum; attrPos++)
        switch (schema->dataTypes[attrPos])
    {
        case DT_STRING:
            offset += schema->typeLength[attrPos];
            break;
        case DT_INT:
            offset += sizeof(int);
            break;
        case DT_FLOAT:
            offset += sizeof(float);
            break;
        case DT_BOOL:
            offset += sizeof(bool);
            break;
    }
    
    *result = offset;
    return RC_OK;
}

/* Copy from the re_serializer file */

/* record_mgr starts. */

typedef struct tableInfo{
    
    int schemaLength;
    int schemaPages;
    int StartPage;
    int LastPage;
    int numTuples;
    int recordSize;
    int maxRecords;
    BM_BufferPool *bm;
}tableInfo;

// table and manager
typedef struct scanInfo{
    Expr *cond;
    int scanID;
    int totalrecord_num;
}scanInfo;

extern RC initRecordManager (void *mgmtData)
{
    return RC_OK;
}
extern RC shutdownRecordManager (){
    return RC_OK;
}
extern RC createTable (char *name, Schema *schema){
    
    /* Check if table already exists*/
    
//    if( access(name, F_OK) != -1 ) {
//        return RC_TABLE_ALREADY_EXIST;
//    }
    
    int status;
    SM_FileHandle fh;
    /* Create a file with the given name and create pages for info and schema*/
    if ((status=createPageFile(name)) != RC_OK)
        return status;
    
    int schema_size = sizeof(int)*2+(schema->numAttr)*sizeof(int)*2+(schema->keySize)*sizeof(int);
    for (int i=0;i<schema->numAttr;i++){
        schema_size+=strlen(schema->attrNames[i]);
    }
    int record_size=getRecordSize(schema);
    int schemaPages = (int) ceil((float)schema_size/PAGE_SIZE);
    int maxRecords = (int) floor((float)PAGE_SIZE/(float)record_size);
    int startPage=schemaPages+1;
    int lastPage = startPage;
    if ((status=openPageFile(name, &fh)) != RC_OK)
        return status;
    
    if ((status=ensureCapacity((schemaPages + 1), &fh)) != RC_OK){
        return status;
    }
    /* First page with file info*/
    tableInfo *info = (tableInfo *) malloc(sizeof(tableInfo));
    info->numTuples = 0;
    info->schemaLength = schema_size;
    info->schemaPages = schemaPages;
    info->StartPage = startPage;
    info->recordSize = record_size;
    info->LastPage = lastPage;
    info->maxRecords = maxRecords;
    
    VarString *result;
    MAKE_VARSTRING(result);
    APPEND(result, "schemaLength <%i> schemaPages <%i> StartPage <%i> LastPage<%i> numTuples<%i> recordSize <%i> maxRecords <%i>", info->schemaLength, info->schemaPages, info->StartPage, info->LastPage, info->numTuples, info->recordSize, info->maxRecords);
    APPEND_STRING(result, "\n");
    char *finalResult;
    GET_STRING(finalResult, result);
    
    if ((status=writeBlock(0, &fh, finalResult)) != RC_OK)
        return status;
    
    /* From next page, write the schema*/
    char *schema_str = serializeSchema(schema);
    if ((status=writeBlock(1, &fh, schema_str)) != RC_OK)
        return status;
    if ((status=closePageFile(&fh)) != RC_OK)
        return status;
    free(result);
    free(finalResult);
    return RC_OK;
}

extern RC openTable (RM_TableData *rel, char *name){
    
    
    if(access(name, F_OK) == -1) {
        return RC_TABLE_NOT_FOUND;
    }
    
    BM_BufferPool *bm = (BM_BufferPool *)malloc(sizeof(BM_BufferPool));
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    
    initBufferPool(bm, name, 3, RS_FIFO, NULL);
    pinPage(bm, page, 0);
    
    tableInfo *info = (tableInfo*) malloc(sizeof(tableInfo));
    char info_data[strlen(page->data)];
    strcpy(info_data, page->data);
    
    char *tFirst, *tNext;
    tFirst = strtok(info_data, "<");
    tFirst = strtok(NULL, ">");
    info->schemaLength = (int) strtol(tFirst, &tNext ,10);
    
    tFirst = strtok(NULL, "<");
    tFirst = strtok(NULL, ">");
    info->schemaPages = (int)strtol(tFirst, &tNext ,10);
    
    tFirst = strtok(NULL, "<");
    tFirst = strtok(NULL, ">");
    info->StartPage = (int) strtol(tFirst, &tNext ,10);
    
    tFirst = strtok(NULL, "<");
    tFirst = strtok(NULL, ">");
    info->LastPage = (int) strtol(tFirst, &tNext ,10);
    
    tFirst = strtok(NULL, "<");
    tFirst = strtok(NULL, ">");
    info->numTuples = (int) strtol(tFirst, &tNext ,10);
    
    tFirst = strtok(NULL, "<");
    tFirst = strtok(NULL, ">");
    info->recordSize = (int) strtol(tFirst, &tNext ,10);
    
    tFirst = strtok(NULL, "<");
    tFirst = strtok(NULL, ">");
    info->maxRecords = (int) strtol(tFirst, &tNext ,10);
    
    if(info->schemaLength < PAGE_SIZE){
        pinPage(bm, page, 1);
    }
    
    
    Schema *schema = (Schema *) malloc(sizeof(Schema));
    char schema_data[strlen(page->data)];
    strcpy(schema_data, page->data);
    
    char *sFirst, *sNext;
    
    sFirst = strtok(schema_data, "<");
    sFirst = strtok(NULL, ">");
    int numAttr;
    numAttr = (int)strtol(sFirst, &sNext, 10);
    schema->numAttr = numAttr;
    
    schema->attrNames=(char **)malloc(sizeof(char*)*numAttr);
    schema->dataTypes=(DataType *)malloc(sizeof(DataType)*numAttr);
    schema->typeLength=(int *)malloc(sizeof(int)*numAttr);
    
    char* str_ref[numAttr];
    
    sFirst = strtok(NULL, "(");
    int i, j;
    for (i = 0; i<numAttr; i++) {
        sFirst = strtok(NULL, ": ");
        schema->attrNames[i]=(char *)calloc(strlen(sFirst), sizeof(char));
        strcpy(schema->attrNames[i], sFirst);
        
        if(i == numAttr - 1){
            sFirst = strtok(NULL, ") ");
        } else {
            sFirst = strtok(NULL, ", ");
        }
        str_ref[i] = (char *)calloc(strlen(sFirst), sizeof(char));
        
        if (strcmp(sFirst, "INT") == 0){
            schema->dataTypes[i] = DT_INT;
            schema->typeLength[i] = 0;
        }
        else if (strcmp(sFirst, "FLOAT") == 0){
            schema->dataTypes[i] = DT_FLOAT;
            schema->typeLength[i] = 0;
        }
        else if (strcmp(sFirst, "BOOL") == 0){
            schema->dataTypes[i] = DT_BOOL;
            schema->typeLength[i] = 0;
        }
        else{
            strcpy(str_ref[i], sFirst);
        }
    }
    
    int keyFlag = 0, keySize = 0;
    char* keyAttrs[numAttr];
    
    if((sFirst = strtok (NULL,"(")) != NULL){
        sFirst = strtok (NULL,")");
        char *key = strtok (sFirst,", ");
        
        while(key != NULL){
            keyAttrs[keySize] = (char *)malloc(strlen(key)*sizeof(char));
            strcpy(keyAttrs[keySize], key);
            keySize++;
            key = strtok (NULL,", ");
        }
        keyFlag = 1;
    }
    
    char *sLast;
    for(i=0; i<numAttr; ++i){
        if(strlen(str_ref[i]) > 0){
            sLast = (char *) malloc(sizeof(char)*strlen(str_ref[i]));
            memcpy(sLast, str_ref[i], strlen(str_ref[i]));
            schema->dataTypes[i] = DT_STRING;
            sFirst = strtok (sLast,"[");
            sFirst = strtok (NULL,"]");
            schema->typeLength[i] = (int) strtol(sFirst, &sNext, 10);
            free(sLast);
            free(str_ref[i]);
        }
    }
    
    if(keyFlag == 1){
        schema->keyAttrs=(int *)malloc(sizeof(int)*keySize);
        schema->keySize = keySize;
        for(i=0; i<keySize; ++i){
            for(j=0; j<numAttr; ++j){
                if(strcmp(keyAttrs[i], schema->attrNames[j]) == 0){
                    schema->keyAttrs[i] = j;
                    free(keyAttrs[i]);
                }
            }
        }
    }
    
    rel->schema = schema;
    rel->name = name;
    
    info->bm = bm;
    rel->mgmtData = info;
    
    free(page);
    
    return RC_OK;
}

extern RC closeTable (RM_TableData *rel){
    
    shutdownBufferPool(((tableInfo *)rel->mgmtData)->bm);
    free(rel->mgmtData);
    free(rel->schema->dataTypes);
    free(rel->schema->attrNames);
    free(rel->schema->keyAttrs);
    free(rel->schema->typeLength);
    free(rel->schema);
    
    return RC_OK;
}

extern RC deleteTable (char *name){
    if(access(name, F_OK) == -1) {
        return RC_TABLE_NOT_FOUND;
    }
    
    if(remove(name) != 0){
        return RC_TABLE_NOT_FOUND;
    }
    return RC_OK;
}
extern int getNumTuples (RM_TableData *rel){
    return ((tableInfo *)rel->mgmtData)->numTuples;
}

// handling records in a table
extern RC insertRecord (RM_TableData *rel, Record *record){
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    tableInfo *info = (tableInfo *) (rel->mgmtData);
    int page_num, slot;
    page_num = info->LastPage;
    slot = info->numTuples - ((page_num - info->StartPage)*info->maxRecords) ;
    if (slot == info->maxRecords){
        slot = 0;
        page_num++;
    }
    info->LastPage = page_num;
    record->id.page = page_num;
    record->id.slot = slot;
    //printf("page:%i, slot:%i\n",record->id.page,record->id.slot);
    char *record_str = record->data;
//    printf("Record %i\n",*(record->data));
//    printf("Record %s\n",(record->data + 4));
//    printf("Record %i\n",*(record->data + 8));
    pinPage(info->bm, page, page_num);
    memcpy(page->data + (slot*info->recordSize), record_str, getRecordSize(rel->schema));
//    printf("InsertedRecord %i\n",*(page->data));
//    printf("InsertedRecord %s\n",(page->data + 4));
//    printf("InsertedRecord %i\n",*(page->data + 8));
    free(record_str);
    markDirty(info->bm, page);
    unpinPage(info->bm, page);
    forcePage(info->bm, page);
   
    (info->numTuples)++;
    VarString *result;
    MAKE_VARSTRING(result);
    APPEND(result, "schemaLength <%i> schemaPages <%i> StartPage <%i> LastPage<%i> numTuples<%i> recordSize <%i> maxRecords <%i>", info->schemaLength, info->schemaPages, info->StartPage, info->LastPage, info->numTuples, info->recordSize, info->maxRecords);
    APPEND_STRING(result, "\n");
    char *finalResult;
    GET_STRING(finalResult, result);
    int status;
    SM_FileHandle fh;
    if ((status=openPageFile(rel->name,&fh))!=RC_OK){
        return status;
    }
    if ((status=writeBlock(0, &fh, finalResult)) != RC_OK){
        free(finalResult);
        return status;
    }
    if ((status=closePageFile(&fh)) != RC_OK) {
        free(finalResult);
        return status;
    }
    free(result);
    free(finalResult);
    free(page);
    return RC_OK;
}

extern RC deleteRecord (RM_TableData *rel, RID id){
    tableInfo *info = (tableInfo *) (rel->mgmtData);
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    int pageNum, slot;
    pageNum = id.page;
    slot = id.slot;
    char *record_str = "@@";//tombstone flag
    
    pinPage(info->bm, page, pageNum);
    memcpy(page->data + (slot*info->recordSize), record_str, getRecordSize(rel->schema));
    markDirty(info->bm, page);
    unpinPage(info->bm, page);
    forcePage(info->bm, page);
    (info->numTuples)--;
    
    VarString *result;
    MAKE_VARSTRING(result);
    APPEND(result, "schemaLength <%i> schemaPages <%i> StartPage <%i> LastPage<%i> numTuples<%i> recordSize <%i> maxRecords <%i>", info->schemaLength, info->schemaPages, info->StartPage, info->LastPage, info->numTuples, info->recordSize, info->maxRecords);
    APPEND_STRING(result, ">\n");
    char *finalResult;
    GET_STRING(finalResult, result);
    int status;
    SM_FileHandle fh;
    if ((status=openPageFile(rel->name,&fh))!=RC_OK){
        return status;
    }
    if ((status=writeBlock(0, &fh, finalResult)) != RC_OK){
        free(finalResult);
        return status;
    }
    if ((status=closePageFile(&fh)) != RC_OK) {
        free(finalResult);
        return status;
    }
    free(result);
    free(finalResult);
    free(page);
    return RC_OK;
    
}
extern RC updateRecord (RM_TableData *rel, Record *record){
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    tableInfo *info = (tableInfo *) (rel->mgmtData);
    int page_num, slot;
    
    page_num = record->id.page;
    slot = record->id.slot;
    
    char *record_str = record->data;
    
    pinPage(info->bm, page, page_num);
    memcpy(page->data + (slot*info->recordSize), record_str, getRecordSize(rel->schema));
    free(record_str);
    
    markDirty(info->bm, page);
    unpinPage(info->bm, page);
    forcePage(info->bm, page);
    
    VarString *result;
    MAKE_VARSTRING(result);
    APPEND(result, "schemaLength <%i> schemaPages <%i> StartPage <%i> LastPage<%i> numTuples<%i> recordSize <%i> maxRecords <%i>", info->schemaLength, info->schemaPages, info->StartPage, info->LastPage, info->numTuples, info->recordSize, info->maxRecords);
    APPEND_STRING(result, ">\n");
    char *finalResult;
    GET_STRING(finalResult, result);
    int status;
    SM_FileHandle fh;
    if ((status=openPageFile(rel->name,&fh))!=RC_OK){
        return status;
    }
    if ((status=writeBlock(0, &fh, finalResult)) != RC_OK){
        free(finalResult);
        return status;
    }
    if ((status=closePageFile(&fh)) != RC_OK) {
        free(finalResult);
        return status;
    }
    
    free(result);
    free(finalResult);
    free(page);
    return RC_OK;
}
extern RC getRecord (RM_TableData *rel, RID id, Record *record){
    tableInfo *info = (tableInfo *) (rel->mgmtData);
    BM_PageHandle *page = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));
    
    int page_num, slot;
    page_num = id.page;
    slot = id.slot;
    
    pinPage(info->bm, page, page_num);
    char *record_str = (char *) malloc(sizeof(char) * info->recordSize);
    memcpy(record_str, page->data + ((slot)*info->recordSize), sizeof(char)*info->recordSize);
//    printf("slot offset %i", (slot)*info->recordSize);
//    printf("RECORD_STR %i\n",*(record_str));
//    printf("RECORD_STR %s\n",(record_str + 4));
//    printf("RECORD_STR %i\n",*(record_str + 8));
    unpinPage(info->bm, page);
    
    if (record_str[0]== '@' && record_str[1] == '@') {
        return RC_RECORD_HAS_BEEN_REMOVED;
    } else {
        record->id.page = page_num;
        record->id.slot = slot;
        memcpy(record->data,record_str,info->recordSize);
    }
//    printf("record page  %i record slot %i", record->id.page, record->id.slot);
    free(record_str);
    free(page);
    return RC_OK;
}

// scans
extern RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond){
    
    /* initialize the RM_ScanHandle data structure */
    scanInfo *sinfo=(scanInfo *)malloc(sizeof(scanInfo));
    sinfo->scanID=0;
    sinfo->totalrecord_num=getNumTuples(rel);
    sinfo->cond=cond;
    scan->rel=rel;
    scan->mgmtData=(void *)sinfo;
    return RC_OK;
}
extern RC next (RM_ScanHandle *scan, Record *record){
    Value *value;
    scanInfo *info;
    tableInfo *tinfo;
    Record *temp;
    temp=malloc(sizeof(Record));
    tinfo=(tableInfo *)scan->rel->mgmtData;
    temp->data = (char *)malloc(getRecordSize(scan->rel->schema));
    info=scan->mgmtData;
    if (info->totalrecord_num==info->scanID){
        return RC_RM_NO_MORE_TUPLES;
    }
    RC status;
    temp->id.page=(int)floor((float)(info->scanID/tinfo->maxRecords))+tinfo->StartPage;
    temp->id.slot=info->scanID-(tinfo->maxRecords)*(temp->id.page-tinfo->StartPage);
    /* fetch the record as per the page and slot id */
    status = getRecord(scan->rel, temp->id, temp);
    info=scan->mgmtData;
    if(status == RC_RECORD_HAS_BEEN_REMOVED){
        (info->scanID)++;
        return next(scan,record);
    }
//    printf("scanrecord %i    ",*(temp->data));
//    printf("%s      ",(temp->data + 4));
//    printf(" %i\n",*(temp->data + 8));
    
    /* evaluate the given expression to check if the record is the one required by the client */
    evalExpr(temp, scan->rel->schema, info->cond, &value);
    /* If the record fetched is not the required one then call the next function with the updated record id parameters. */
    if(value->v.boolV != 1){
        (info->scanID)++;
        return next(scan,record);
    }
    else{
        (info->scanID)++;
        record->id.page = temp->id.page;
        record->id.slot = temp->id.slot;
        memcpy(record->data,temp->data,getRecordSize(scan->rel->schema));
        return RC_OK;
    };
}
extern RC closeScan (RM_ScanHandle *scan){
//    free(scan);
    return RC_OK;
}

// dealing with schemas
extern int getRecordSize (Schema *schema){
    int size = 0, tempSize = 0, i;
    
    for(i=0; i<schema->numAttr; ++i){
        switch (schema->dataTypes[i]){
            case DT_STRING:
                tempSize = schema->typeLength[i];
                break;
            case DT_INT:
                tempSize = sizeof(int);
                break;
            case DT_FLOAT:
                tempSize = sizeof(float);
                break;
            case DT_BOOL:
                tempSize = sizeof(bool);
                break;
            default:
                break;
        }
        size += tempSize;
    }
    return size;
}
extern Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys){
    
    Schema *schema = (Schema *) malloc(sizeof(Schema));
    
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->keyAttrs = keys;
    
    return schema;
}
extern RC freeSchema (Schema *schema){
    free(schema);
    return RC_OK;
}

// dealing with records and attribute values
extern RC createRecord (Record **record, Schema *schema){
    /* allocate memory for a new record and record data as per the schema. */
    *record = (Record *)  malloc(sizeof(Record));
    (*record)->data = (char *)malloc((getRecordSize(schema)));
    
    return RC_OK;
}
extern RC freeRecord (Record *record ){
    /* free the memory space allocated to record and its data */
    free(record);
    
    return RC_OK;
}
extern RC getAttr (Record *record, Schema *schema, int attrNum, Value **value){
    
    /* allocate the space to the value data structre where the attribute values are to be fetched */
    *value = (Value *)  malloc(sizeof(Value));
    int offset; char *attrData;
    
    /* get the offset value of different attributes as per the attribute numbers */
    attrOffset(schema, attrNum, &offset);
    attrData = (record->data + offset);
    (*value)->dt = schema->dataTypes[attrNum];
    
    /* attribute data is assigned to the value pointer as per the different data types */
    switch(schema->dataTypes[attrNum])
    {
        case DT_INT:
        {
            memcpy(&((*value)->v.intV),attrData, sizeof(int));
        }
            break;
        case DT_STRING:
        {
            char *strV;
            int len = schema->typeLength[attrNum];
            strV=(char*) malloc(len);
            //printf("getattr:%s\n len:%i\n",attrData,len);
            strncpy(strV,attrData,len);
            //printf("coppyattr:%s\n len:%i\n",attrData,len);
            (*value)->v.stringV=strV;        }
            break;
        case DT_FLOAT:
        {
            memcpy(&((*value)->v.floatV),attrData, sizeof(float));
        }
            break;
        case DT_BOOL:
        {
            memcpy(&((*value)->v.boolV),attrData, sizeof(bool));
        }
            break;
    }
    
    return RC_OK;
    
}

extern RC setAttr (Record *record, Schema *schema, int attrNum, Value *value){
    int offset;
    
    /* get the offset value of different attributes as  per the attribute numbers */
    attrOffset(schema, attrNum, &offset);
    
    /* set attribute values as per the attributes datatype */
    switch(schema->dataTypes[attrNum])
    {
        case DT_INT:
        {
            memcpy((record->data + offset),&(value->v.intV), sizeof(int));
//            printf("int in record:%i\n",*(record->data + offset));
        }
            break;
        case DT_STRING:
        {
            memcpy((record->data + offset),(value->v.stringV), schema->typeLength[attrNum]);
//            printf("string in record:%s\n",(record->data));
//            printf("len:%i\n",len);
        }
            break;
        case DT_FLOAT:
        {
            memcpy((record->data + offset),&((value->v.floatV)), sizeof(float));
        }
            break;
        case DT_BOOL:
        {
            memcpy((record->data + offset),&((value->v.boolV)), sizeof(bool));
        }
            break;
    }
    
    return RC_OK;
}