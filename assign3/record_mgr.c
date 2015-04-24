#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

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
  int i;
  for (i = 0; i < MAX_N_TABLES; i++) {
    header->tableNames[i] = (char *)malloc(sizeof(char) * ATTR_SIZE);
  }
  header->numTables = 0;
  return header;
}

void write_to_header(DB_header *header, int tablePage, char* tableName) {
  header->tableHeaders[header->numTables] = tablePage;
  header->tableNames[header->numTables] = tableName;
  header->numTables++;
}

int getDB_HeaderSize() {
  int size = sizeof(int); // header->numTables 
  size += sizeof(DB_header);
  size += sizeof(int) * MAX_N_TABLES;
  size += sizeof(char *) * MAX_N_TABLES;
  size += sizeof(char) * ATTR_SIZE *  MAX_N_TABLES;;
  return size;
}

void printDB_Header(DB_header *header) {
  printf("Printing structure...!!!\n");
  printf("\tnumTables:\t\t%i\n", header->numTables);
  int i;
  for (i = 0; i < header->numTables; i++) {
    printf("\ttableNames[%i]\t\t%s\n", i, header->tableNames[i]);
    printf("\ttableHeaders[%i]\t\t%i\n", i, header->tableHeaders[i]);
  }
}

char *write_serializer(DB_header *header) {
  int size = getDB_HeaderSize();
  char *out = (char *)malloc(sizeof(char) * size);

  int int_size = sizeof(int);
  int str_size = sizeof(char) * ATTR_SIZE;

  memcpy(out, &(header->numTables), int_size);
  int offset = int_size;

  int i;
  for (i = 0; i < header->numTables; i++) {
    memcpy(out + offset, header->tableNames[i], str_size);
    offset += str_size;
    memcpy(out + offset, &(header->tableHeaders[i]), int_size);
    offset += int_size;
  }

  return out;
}

DB_header *read_serializer(char *data) {
  DB_header *header = createDB_header();

  int int_size = sizeof(int);
  int str_size = sizeof(char) * ATTR_SIZE;
  int offset = 0;

  memcpy(&(header->numTables), data, int_size);
  offset += int_size;

  int i;
  for (i = 0; i < header->numTables; i++) {
    memcpy(header->tableNames[i], data + offset, str_size);
    offset += str_size;

    memcpy(&(header->tableHeaders[i]), data + offset, int_size);
    offset += int_size;
  }

  return header;
}

void addTableToDB_Header (DB_header *header) {
  write_to_header(header, 456, "HolaMundo");
  // header->numTables++;
}

void testWritingPage() {
  CHECK(pinPage(buffer_manager, page_handler, 1));
  sprintf(page_handler->data, "BOQUEPASA");
  CHECK(markDirty(buffer_manager, page_handler));
  CHECK(unpinPage(buffer_manager, page_handler));

}

void testReadingPage() {
  CHECK(pinPage(buffer_manager, page_handler, 1));
  printf("Page-1: %s\n", page_handler->data);
  CHECK(unpinPage(buffer_manager, page_handler));
}

// table and manager
RC initRecordManager (void *mgmtData) {
  buffer_manager = MAKE_POOL();
  page_handler = MAKE_PAGE_HANDLE();

  char *pageFileName = "testrecord.bin";

  if (access(pageFileName, R_OK) < 0) {
    printf("Creating PageFile...\n");
    createPageFile(pageFileName);
  }
  
  initBufferPool(buffer_manager, pageFileName, 6, RS_FIFO, NULL);

  DB_header *db_header = createDB_header();

  CHECK(pinPage(buffer_manager, page_handler, 0));
  
  printf("Reading DB_header from disk...\n");
  DB_header *db_read = read_serializer(page_handler->data);

  printDB_Header(db_read);


  if (db_read->numTables <= 0) {
    // write DB_header to page
    printf("No DB_Header in disk, writing a new one...\n");
    
    // printDB_Header(db_header);

    // printf("Adding some table to DB_Header before initializing...\n");
    // addTableToDB_Header(db_header);
    // printDB_Header(db_header);

    char *data = write_serializer(db_header);

    memcpy(page_handler->data, data, getDB_HeaderSize());

    printf("Checking header serializers in init...\n");
    DB_header *helper = read_serializer(page_handler->data);
    printDB_Header(helper);

    CHECK(markDirty(buffer_manager, page_handler));

  }

  else {
    // read DB_header from page, not needed?!?!?!
    printf("************* GOOD ****************\n");
    printf("************* GOOD ****************\n");
    printf("************* GOOD ****************\n");
    printf("Reading already existent DB_Header...\n");
    DB_header *db_read = read_serializer(page_handler->data);

    printDB_Header(db_read);

  }
  
  // char *data = write_serializer(db_header);
  // memcpy(page_handler->data, data, getDB_HeaderSize());
  
  // markDirty(buffer_manager, page_handler);  

  CHECK(unpinPage(buffer_manager, page_handler));

  return RC_OK;
}

RC shutdownRecordManager () {
  CHECK(pinPage(buffer_manager, page_handler, 0));

  printf("Reading DB_Header on buffer before shutdown...\n");
  DB_header *db_read = read_serializer(page_handler->data);
  printDB_Header(db_read);

  // printf("Adding some table to DB_Header before shutdown...\n");
  // addTableToDB_Header(db_read);
  // printDB_Header(db_read);
  
  CHECK(unpinPage(buffer_manager, page_handler));
  CHECK(shutdownBufferPool(buffer_manager));
  return RC_OK;
}

int searchStringArray(char *target, char **strArray, int length)
{
  int i;
  for(i=0; i<length; i++)
  {
    if(strcmp(strArray[i], target) == 0)
      return i;
  }
  return -1;
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
  char *tableName = rel->name;

  CHECK(pinPage(buffer_manager, page_handler_db, 0));
  DB_header *db_header = read_db_serializer(page_handler_db->data);

  int table_pos_in_array = searchStringArray(tableName, db_header->tableNames, db_header->numTables);
  if(table_pos_in_array < 0){
    return RC_FILE_NOT_FOUND;
  }

  int table_page_num = db_header->tableHeaders[table_pos_in_array];
  
  BM_PageHandle *page_handler_table = MAKE_PAGE_HANDLE();
  CHECK(pinPage(buffer_manager, page_handler_table, table_page_num));

  Table_Header *th_header = read_th_serializer(page_handler_table->data);
  

  CHECK(unpinPage(buffer_manager, page_handler_db));
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

