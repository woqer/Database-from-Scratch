#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "buffer_mgr.h"
#include "rm_serializer.c"

typedef struct DB_header
{
  int numTables;
  char **tableNames;
  int *tableHeaders;
} DB_header;

static BM_BufferPool *buffer_manager;
static BM_PageHandle *page_handler;

DB_header *createDB_header() {
  DB_header *header = (DB_header *)malloc(sizeof(DB_header));
  header->tableHeaders = (int *)malloc(sizeof(int) * MAX_N_TABLES);
  header->tableNames = (char **)malloc(sizeof(char*) * MAX_N_TABLES);
  header->numTables = 0;
  return header;
}

int readDB_Header(DB_header *header) {
  return 0;
}

int writeDB_Header(DB_header *header, char *tableName) {
  return 0;
}

int getDB_HeaderSize() {
  int size = sizeof(int); // header->numTables 
  size += sizeof(DB_header);
  size += sizeof(int) * MAX_N_TABLES;
  size += sizeof(char*) * MAX_N_TABLES;
  return size;
}

void addTableToDB_Header (DB_header *header, RM_TableData *rel) {
  // header->tableHeaders[numTables] = 
}

// table and manager
RC initRecordManager (void *mgmtData) {
  buffer_manager = MAKE_POOL();
  page_handler = MAKE_PAGE_HANDLE();

  char *pageFileName = "testrecord.bin"; 
  createPageFile(pageFileName);
  initBufferPool(buffer_manager, pageFileName, 6, RS_FIFO, NULL);

  DB_header *db_header = createDB_header();

  pinPage(buffer_manager, page_handler, 0);
  
  if (page_handler->data == '\0') {
    // write DB_header to page
  } else {
    // read DB_header from page, not needed?!?!?!
  }

  return RC_OK;
}

RC shutdownRecordManager () {
  return RC_OK;
}

RC createTable (char *name, Schema *schema) {
  return RC_OK;
}

RC openTable (RM_TableData *rel, char *name) {
  return RC_OK;
}

RC closeTable (RM_TableData *rel) {
  return RC_OK;
}

RC deleteTable (char *name) {
  return RC_OK;
}

int getNumTuples (RM_TableData *rel) {
  return 0;
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record) {
  return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id) {
  return RC_OK;
}

RC updateRecord (RM_TableData *rel, Record *record) {
  return RC_OK;
}

RC getRecord (RM_TableData *rel, RID id, Record *record) {
  return RC_OK;
}


// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
  return RC_OK;
}

RC next (RM_ScanHandle *scan, Record *record) {
  return RC_OK;
}

RC closeScan (RM_ScanHandle *scan) {
  return RC_OK;
}


// dealing with schemas
int getRecordSize (Schema *schema) {
  int recordSize = 0;
  int i;
  for(i=0; i<schema->numAttr; i++)
  {
    switch(schema->dataTypes[i])
    {
    case DT_INT: recordSize += sizeof(int);
     break;
    case DT_STRING: recordSize += schema->typeLength[i];
     break;
    case DT_FLOAT: recordSize += sizeof(float);
     break;
    case DT_BOOL: recordSize += sizeof(bool);
     break;
    default: break;
    }
  }
  return recordSize;
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {
  Schema *schema = (Schema *)malloc(sizeof(Schema));

  schema->numAttr = numAttr;
  schema->attrNames = (char **)malloc(sizeof(char *) * numAttr);
  schema->dataTypes = (DataType *)malloc(sizeof(DataType) * numAttr);
  schema->typeLength = (int *)malloc(sizeof(int) * numAttr);
  schema->keyAttrs = (int *)malloc(sizeof(int) * keySize);
  schema->keySize = keySize;

  int i;
  for (i = 0; i < numAttr; i++) {
    DataType x = dataTypes[i];
    (schema->dataTypes)[i] = x;
    (schema->typeLength)[i] = typeLength[i];
    (schema->attrNames)[i] = (char *)malloc(sizeof(char) * ATTR_SIZE);
    strncpy((schema->attrNames)[i], attrNames[i], ATTR_SIZE);
  }

  for (i = 0; i < keySize; i++) {
    (schema->keyAttrs)[i] = keys[i];
  }

  return schema;
}

RC freeSchema (Schema *schema) {
  return RC_OK;
}


// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema) {
  Record *rec = (Record *)malloc(sizeof(Record));
  
  RID *rid_ptr = (RID *)malloc(sizeof(RID));
  rid_ptr->page = 0;
  rid_ptr->slot = 0;
  
  rec->id = *rid_ptr;
  rec->data = (char *)malloc(sizeof(char) * getRecordSize(schema));

  *record = rec;  

  return RC_OK;
}

RC freeRecord (Record *record) {

  free(&(record->id));
  free(record->data);
  printf("Trying to free record struct...\n");
  free(record);

  return RC_OK;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value) {
  Value *val;
  
  int offset;
  char *attrData;
  
  attrOffset(schema, attrNum, &offset); //get the offset of attrNum in record->data
  attrData = record->data + offset;     
  
  switch((schema->dataTypes)[attrNum])
  {
	case DT_INT: 
	{
		int intVal;
		memcpy(&intVal,attrData, sizeof(int));
		MAKE_VALUE(val, DT_INT, intVal);
	}
	break;
	case DT_STRING:
	{
		char *buf;
		int len = schema->typeLength[attrNum];
		buf = (char *) malloc(len);
		strncpy(buf, attrData, len);
		MAKE_STRING_VALUE(val, buf);
		(val->v.stringV)[len] = '\0';
		free(buf);
    }
	break;
	case DT_FLOAT:
    {
		float floatVal;
		memcpy(&floatVal,attrData, sizeof(float));
		MAKE_VALUE(val, DT_FLOAT, floatVal);
    }
    break;
	case DT_BOOL:
    {
		bool boolVal;
		memcpy(&boolVal,attrData, sizeof(bool));
		MAKE_VALUE(val, DT_BOOL, boolVal);
    }
    break;
    default: 
	break;
  }
  
  *value = val;
  
  return RC_OK;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value) {

  int offset;
  char *attrData;
  
  attrOffset(schema, attrNum, &offset); //get the offset of attrNum in record->data
  attrData = record->data + offset; 
  
  switch((schema->dataTypes)[attrNum])
  {
	case DT_INT: memcpy(attrData, &(value->v.intV), sizeof(int));
	break;
	case DT_STRING: memcpy(attrData, value->v.stringV, schema->typeLength[attrNum]);
	break;
	case DT_FLOAT: memcpy(attrData, &(value->v.floatV), sizeof(float));
    break;
	case DT_BOOL: memcpy(attrData, &(value->v.boolV), sizeof(bool));
    break;
    default: 
	break;
  }
  
  return RC_OK;
}

