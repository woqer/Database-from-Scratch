#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "buffer_mgr.h"
#include "rm_serializer.c"

#define PAGES_LIST 100

typedef struct DB_header
{
  int numTables;
  char **tableNames;
  int *tableHeaders;
  int nextAvailPage;
} DB_header;

typedef struct Table_Header
{
  int numPages;
  Schema *schema;
  int *pagesList;
  int nextSlot;
  int slots_per_page;
  bool *active; // size is numPages*slots_per_page
} Table_Header;

static BM_BufferPool *buffer_manager;
static BM_PageHandle *page_handler_db;



/* A function to linearly search a target integer in an integer array.
 * The index of the target is returned if found, -1 if not found.
 * The search start at index 0 and ends at index length-1.
 */
int searchIntArray(int x, int *a, int length) {
  int i;
  for (i = 0; i < length; i++) {
    if (a[i] == x) return i;
  }
  return -1;
}

// searchs string in the given array, returns the index
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

Table_Header *createTable_Header(Schema *schema) {
  Table_Header *th = (Table_Header *)malloc(sizeof(Table_Header));
  th->pagesList = (int *)malloc(sizeof(int) * PAGES_LIST);
  th->schema = schema;
  th->nextSlot = 0;
  th->numPages = 0;
  float slots_per_page_decimal = PAGE_SIZE / getRecordSize(schema);
  th->slots_per_page = (int)slots_per_page_decimal;
  th->active = (bool *)malloc(sizeof(bool) * th->slots_per_page);
  
  int i;
  for (i = 0; i < th->slots_per_page; i++) {
    th->active[i] = false;
  }
  return th;
}

DB_header *createDB_header() {
  DB_header *header = (DB_header *)malloc(sizeof(DB_header));
  header->tableHeaders = (int *)malloc(sizeof(int) * MAX_N_TABLES);
  header->tableNames = (char **)malloc(sizeof(char*) * MAX_N_TABLES);
  int i;
  for (i = 0; i < MAX_N_TABLES; i++) {
    header->tableNames[i] = (char *)malloc(sizeof(char) * ATTR_SIZE);
  }
  header->numTables = 0;
  header->nextAvailPage = 1;
  return header;
}

void free_db_header(DB_header *head) {
  int i;
  for (i = 0; i < MAX_N_TABLES; i++) {
    free(head->tableNames[i]);
  }
  free(head->tableNames);
  free(head->tableHeaders);
  free(head);
}

void free_table_header(Table_Header *th) {
  free(th->pagesList);
  free(th->active);
  free(th);
}

void add_table_to_header(DB_header *header, int tablePage, char* tableName) {
  header->tableHeaders[header->numTables] = tablePage;
  header->tableNames[header->numTables] = tableName;
  header->numTables++;
  header->nextAvailPage = tablePage + 2;
}

// returns the current available slot
void add_record_to_header(Table_Header *th, DB_header *db_header) {

  int active_index = ((th->numPages -1) * th->slots_per_page) + th->nextSlot;
  th->active[active_index] = true;

  if (th->nextSlot + 1 > th->slots_per_page) {
    // need a new page
    th->nextSlot = 0;
    
    if (th->numPages % PAGES_LIST) {
      // realloc pagesList
      th->pagesList = (int *)realloc(th->pagesList, sizeof(int) * th->numPages);
    }

    th->active = (bool *)realloc(th->active, sizeof(bool) * th->slots_per_page);
    th->pagesList[th->numPages++] = db_header->nextAvailPage++;

  } else {
    th->nextSlot++;
  }
}

int getDB_HeaderSize() {
  int size = sizeof(int) * 2; // numTables & nextAvailPage
  size += sizeof(DB_header);
  size += sizeof(int) * MAX_N_TABLES;
  size += sizeof(char *) * MAX_N_TABLES;
  size += sizeof(char) * ATTR_SIZE *  MAX_N_TABLES;
  return size;
}

int getSchemaSize(Schema *schema) {
  int size = sizeof(int) * 2;                 // keySize & numAttr
  size += ATTR_SIZE * schema->numAttr;        // **attrNames;
  size += sizeof(DataType) * schema->numAttr; // *dataTypes
  size += sizeof(int) * schema->numAttr;      // *typeLength size;
  size += sizeof(int) * schema->keySize;      // int *keyAttrs;
}


int getTable_Header_Size(Table_Header *th) {
  int size = sizeof(int) * 3; // numpages & nextSlot & slots_per_page
  
  // *pagesList
  size += sizeof(int) * PAGES_LIST;
  int i, realloc_times;
  realloc_times = (int)(th->numPages / PAGES_LIST);
  for (i = 0; i < realloc_times; i++) {
    size += sizeof(int) * PAGES_LIST;
  }
  
  size += getSchemaSize(th->schema); // *schema
  size += sizeof(bool) * th->numPages * th->slots_per_page; // *active
  return size;
}

void printDB_Header(DB_header *header) {
  printf("Printing DB_header...!!!\n");
  printf("\tnumTables:\t\t%i\n", header->numTables);
  printf("\tnextAvailPage:\t\t%i\n", header->nextAvailPage);
  int i;
  for (i = 0; i < header->numTables; i++) {
    printf("\ttableNames[%i]\t\t%s\n", i, header->tableNames[i]);
    printf("\ttableHeaders[%i]\t\t%i\n", i, header->tableHeaders[i]);
  }
}

void printSchema(Schema *schema) {
  printf("Printing Schema...!!!\n");
  printf("numAttr\t\t%d\n", schema->numAttr);
  int i;
  for (i = 0; i < schema->numAttr; i++) {
    printf("attrNames[%i]\t\t%s\n", i, schema->attrNames[i]);
    printf("dataTypes[%i]\t\t%d\n", i, schema->dataTypes[i]);
    printf("typeLength[%i]\t\t%d\n", i, schema->typeLength[i]);
  }
  printf("keySize\t\t%i\n", schema->keySize);
  for (i = 0; i < schema->keySize; i++) {
    printf("keyAttrs[%i]\t\t%d\n", i, schema->keyAttrs[i]);
  }
}

void printTable_Header(Table_Header *th) {
  printf("******************************\n");
  printf("Printing Table_Header...!!!\n");
  printf("numPages\t\t%d\n", th->numPages);
  printf("nextSlot\t\t%d\n", th->nextSlot);
  printf("slots_per_page\t\t%d\n", th->slots_per_page);
  int active_size = th->numPages * th->slots_per_page;
  int i;
  for (i = 0; i < th->numPages; i++) {
    printf("pagesList[%i]\t\t%d\n", i, th->pagesList[i]);
  }
  for (i = 0; i < active_size; i++) {
    printf("active[%i]\t\t%s\n", i, th->active[i] ? "true" : "false");
  }
  printSchema(th->schema);
  printf("******************************\n");
}

char *write_db_serializer(DB_header *header) {
  int size = NULL;
  size = getDB_HeaderSize();
  char *out = NULL;
  out = (char *)malloc(sizeof(char) * size);

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

  memcpy(out + offset, &(header->nextAvailPage), int_size);

  return out;
}

char *write_schema_serializer(Schema *schema) {
  int size = getSchemaSize(schema);
  char *out = (char *)malloc(sizeof(char) * size);

  int int_size = sizeof(int);
  int str_size = sizeof(char) * ATTR_SIZE;
  int dt_size = sizeof(DataType);

  memcpy(out, &(schema->numAttr), int_size);
  int offset = int_size;
  memcpy(out + offset, &(schema->keySize), int_size);
  offset += int_size;

  int i;
  for(i = 0; i < schema->numAttr; i++) {
    memcpy(out + offset, schema->attrNames[i], str_size);
    offset += str_size;
    memcpy(out + offset, &(schema->dataTypes[i]), dt_size);
    offset += dt_size;
    memcpy(out + offset, &(schema->typeLength[i]), int_size);
    offset += int_size;
  }

  for(i = 0; i < schema->keySize; i++) {
    memcpy(out + offset, &(schema->keyAttrs[i]), int_size);
    offset += int_size;
  }

  return out;
}

char *write_table_serializer(Table_Header *th) {
  int size = getTable_Header_Size(th);
  char *out = (char *)malloc(sizeof(char) * size);

  int int_size = sizeof(int);
  int str_size = sizeof(int);
  int bool_size = sizeof(bool);
  int active_size = th->numPages * th->slots_per_page;

  memcpy(out, &(th->numPages), int_size);
  int offset = int_size;
  memcpy(out + offset, &(th->nextSlot), int_size);
  offset += int_size;
  memcpy(out + offset, &(th->slots_per_page), int_size);
  offset += int_size;

  int i;
  for (i = 0; i < th->numPages; i++) {
      memcpy(out + offset, &(th->pagesList[i]), int_size);
      offset += int_size;
  }

  for (i = 0; i < active_size; i++) {
    memcpy(out + offset, &(th->active[i]), bool_size);
    offset += bool_size;
    // if (th->active[i])
      // printf("WWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWW_table_serializer: memcpy a value of true on position %i\n", i);
  }

  char *sch_data = write_schema_serializer(th->schema);
  memcpy(out + offset, sch_data, getSchemaSize(th->schema));
  free(sch_data); //already copied

  return out;
}

DB_header *read_db_serializer(char *data) {
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

  memcpy(&(header->nextAvailPage), data + offset, int_size);

  return header;
}

Schema *read_schema_serializer(char *data) {
  Schema *schema = (Schema *)malloc(sizeof(Schema));
  schema->numAttr = 0;
  schema->keySize = 0;

  int int_size = sizeof(int);
  int str_size = sizeof(char) * ATTR_SIZE;
  int dt_size = sizeof(DataType);

  memcpy(&(schema->numAttr), data, int_size);
  int offset = int_size;
  memcpy(&(schema->keySize), data + offset, int_size);
  offset += int_size;

  schema->attrNames = (char **)malloc(sizeof(char *) * schema->numAttr);  
  schema->dataTypes = (DataType *)malloc(sizeof(DataType) * schema->numAttr);
  schema->typeLength = (int *)malloc(sizeof(int) * schema->numAttr);
  schema->keyAttrs = (int *)malloc(sizeof(int) * schema->keySize);

  int i;
  for (i = 0; i < schema->numAttr; i++) {
    schema->attrNames[i] = (char *)malloc(sizeof(char) * ATTR_SIZE);
    memcpy(schema->attrNames[i], data + offset, str_size);
    offset += str_size;
    memcpy(&(schema->dataTypes[i]), data + offset, dt_size);
    offset += dt_size;
    memcpy(&(schema->typeLength[i]), data + offset, int_size);
    offset += int_size;
  }

  for (i = 0; i < schema->keySize; i++) {
    memcpy(&(schema->keyAttrs[i]), data + offset, int_size);
    offset += int_size;
  }

  return schema;
}

Table_Header *read_table_serializer(char *data) {
  Table_Header *th = (Table_Header *)malloc(sizeof(Table_Header));
  th->numPages = 0;
  th->nextSlot = 0;
  th->slots_per_page = 0;

  int int_size = sizeof(int);
  int str_size = sizeof(int);
  int bool_size = sizeof(bool);

  memcpy(&(th->numPages), data, int_size);
  int offset = int_size;
  memcpy(&(th->nextSlot), data + offset, int_size);
  offset += int_size;
  memcpy(&(th->slots_per_page), data + offset, int_size);
  offset += int_size;

  int active_size = th->numPages * th->slots_per_page;
  th->pagesList = (int *)malloc(sizeof(int) * th->numPages);
  th->active = (bool *)malloc(sizeof(bool) * active_size);

  int i;
  for(i = 0; i < th->numPages; i++) {
    memcpy(&(th->pagesList[i]), data + offset, int_size);
    offset += int_size;
  }

  for(i = 0; i < active_size; i++) {
    memcpy(&(th->active[i]), data + offset, bool_size);
    offset += bool_size;
    // if (th->active[i])
      // printf("RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR_table_serializer: memcpy a value of true on position %i\n", i);
  }

  Schema *schema_aux = read_schema_serializer(data + offset);

/*  printf("r_table_serilizer: printing schema from r_schema_serializer\n");
  printSchema(schema_aux);
*/
  th->schema = schema_aux;

  return th;
}

// void testWritingPage() {
//   CHECK(pinPage(buffer_manager, page_handler_db, 1));
//   sprintf(page_handler_db->data, "BOQUEPASA");
//   CHECK(markDirty(buffer_manager, page_handler_db));
//   CHECK(unpinPage(buffer_manager, page_handler_db));

// }

// void testReadingPage() {
//   CHECK(pinPage(buffer_manager, page_handler_db, 1));
//   printf("Page-1: %s\n", page_handler_db->data);
//   CHECK(unpinPage(buffer_manager, page_handler_db));
// }

// table and manager
RC initRecordManager (void *mgmtData) {
  buffer_manager = MAKE_POOL();
  page_handler_db = MAKE_PAGE_HANDLE();

  char *pageFileName = "testrecord.bin";

  if (access(pageFileName, R_OK) < 0) {
    printf("Creating PageFile...\n");
    createPageFile(pageFileName);
  }
  
  initBufferPool(buffer_manager, pageFileName, 6, RS_FIFO, NULL);


  CHECK(pinPage(buffer_manager, page_handler_db, 0));
  
  printf("Reading DB_header from disk...\n");
  DB_header *db_read = read_db_serializer(page_handler_db->data);

  if (db_read->numTables <= 0) {
    printf("No DB_Header in disk, writing a new one...\n");

    DB_header *db_header = createDB_header();
    char *data = write_db_serializer(db_header);

    memcpy(page_handler_db->data, data, getDB_HeaderSize());

    CHECK(markDirty(buffer_manager, page_handler_db));

    free(data);
    free_db_header(db_header);

  } else {
    printf("****** Printing already existent DB_Header... ******\n");
    printDB_Header(db_read);
  }

  CHECK(unpinPage(buffer_manager, page_handler_db));

  printf("InitRecord printing page_handler_db...\n");
  printf("page_handler_db->pageNum\t\t%d\n", page_handler_db->pageNum);

  free_db_header(db_read);

  return RC_OK;
}

RC shutdownRecordManager () {
  // CHECK(pinPage(buffer_manager, page_handler_db, 0));

  // printf("Reading DB_Header on buffer before shutdown...\n");
  // DB_header *db_read = read_db_serializer(page_handler_db->data);
  // printDB_Header(db_read);

  // printf("Adding some table to DB_Header before shutdown...\n");
  // addTableToDB_Header(db_read);
  // printDB_Header(db_read);
  
  // CHECK(unpinPage(buffer_manager, page_handler_db));
  printf("Shuting down buffer pool...\n");
  CHECK(shutdownBufferPool(buffer_manager));
  printf("Returned from shuting down buffer pool\n");

  free(page_handler_db);
  free(buffer_manager);

  return RC_OK;
}

RC createTable (char *name, Schema *schema) {
  BM_PageHandle *page_handler_table = MAKE_PAGE_HANDLE();
  BM_PageHandle *page_handler_empty = MAKE_PAGE_HANDLE();

  Table_Header *th = createTable_Header(schema);

  CHECK(pinPage(buffer_manager, page_handler_db, 0));  
  DB_header *db_header = read_db_serializer(page_handler_db->data);

  // printf("createTable: db_header pinned and serialized, using info for setting up table...\n");
  printDB_Header(db_header);
  // nextAvailPage is for table header and the next one will be the first page
  // to write records on this table
  int table_page_num = db_header->nextAvailPage;
  CHECK(pinPage(buffer_manager, page_handler_table, table_page_num));
  th->pagesList[th->numPages++] = table_page_num + 1;
  
  // creating empty page for first records
  CHECK(pinPage(buffer_manager, page_handler_empty, table_page_num + 1));
  CHECK(unpinPage(buffer_manager, page_handler_empty));

  // printf("createTable: pined and unpinned empty page, proceeding to add_table_to_header\n");
  // two pages used, one for table header, other for first records on new table
  db_header->nextAvailPage += 2;
  add_table_to_header(db_header, table_page_num, name);

  char *table_data = write_table_serializer(th);
  memcpy(page_handler_table->data, table_data, getTable_Header_Size(th));
  free(table_data);
  markDirty(buffer_manager, page_handler_table);
  unpinPage(buffer_manager, page_handler_table);

  char *db_data = write_db_serializer(db_header);
  memcpy(page_handler_db->data, db_data, getDB_HeaderSize());
  free(db_data);
  markDirty(buffer_manager, page_handler_db);
  unpinPage(buffer_manager, page_handler_db);

  // printf("Returning from createTable\n");

  return RC_OK;
}

RC openTable (RM_TableData *rel, char *name) {
  if(sizeof(name) > ATTR_SIZE) return RC_TABLE_NAME_TOO_LONG;

  CHECK(pinPage(buffer_manager, page_handler_db, 0)); //page handler for database header
  DB_header *db_header = read_db_serializer(page_handler_db->data); //DB_hearder structure for the data in database header

  //index of the table in the array
  int table_pos_in_array = searchStringArray(name, db_header->tableNames, db_header->numTables);
  if(table_pos_in_array < 0) return RC_TABLE_NOT_FOUND;

  int table_page_num = db_header->tableHeaders[table_pos_in_array];
  
  BM_PageHandle *page_handler_table = MAKE_PAGE_HANDLE(); //page handler for the table header
  CHECK(pinPage(buffer_manager, page_handler_table, table_page_num));
  Table_Header *th_header = read_table_serializer(page_handler_table->data); //Table_Header strudture for the data in table header

  rel->name = (char *)malloc(sizeof(char *) * ATTR_SIZE);
  strcpy(rel->name, name);

  Schema *schema = th_header->schema;
  rel->schema = createSchema (schema->numAttr, schema->attrNames, schema->dataTypes, schema->typeLength, schema->keySize, schema->keyAttrs);

  free_table_header(th_header);
  free_db_header(db_header);
  return RC_OK;
}

RC closeTable (RM_TableData *rel) {
  return RC_OK;
}

RC deleteTable (char *name) {
  if(sizeof(name) > ATTR_SIZE) return RC_TABLE_NAME_TOO_LONG;

  CHECK(pinPage(buffer_manager, page_handler_db, 0)); //page handler for database header
  DB_header *db_header = read_db_serializer(page_handler_db->data); //DB_hearder structure for the data in database header

  //index of the table in the array
  int table_pos_in_array = searchStringArray(name, db_header->tableNames, db_header->numTables);
  if(table_pos_in_array < 0) return RC_TABLE_NOT_FOUND;

  int i;
  for(i=table_pos_in_array; i<db_header->numTables-1; i++)
  {
    db_header->tableNames[i] = db_header->tableNames[i+1];
    db_header->tableHeaders[i] = db_header->tableHeaders[i+1];
  }
  db_header->numTables--;

  char *db_header_data = write_db_serializer(db_header);
  memcpy(page_handler_db->data, db_header_data, getDB_HeaderSize());
  free(db_header_data);
  free_db_header(db_header);
  CHECK(unpinPage(buffer_manager, page_handler_db));
  CHECK(markDirty(buffer_manager, page_handler_db));

  return RC_OK;
}

int getNumTuples (RM_TableData *rel) {
  char *tableName = rel->name; //name of the table

  CHECK(pinPage(buffer_manager, page_handler_db, 0)); //page handler for database header
  DB_header *db_header = read_db_serializer(page_handler_db->data); //DB_hearder structure for the data in database header

  //index of the table in the array
  int table_pos_in_array = searchStringArray(tableName, db_header->tableNames, db_header->numTables);
  if(table_pos_in_array < 0) return RC_TABLE_NOT_FOUND;

  int table_page_num = db_header->tableHeaders[table_pos_in_array];
  
  BM_PageHandle *page_handler_table = MAKE_PAGE_HANDLE(); //page handler for the table header
  CHECK(pinPage(buffer_manager, page_handler_table, table_page_num));
  Table_Header *th_header = read_table_serializer(page_handler_table->data); //Table_Header strudture for the data in table header

  int i, numTuples = 0;
  int totalRecordInDisk = (th_header->slots_per_page) * (th_header->numPages-1) + th_header->nextSlot;
  for(i=0; i<totalRecordInDisk; i++)
  {
    if(th_header->active[i])
      numTuples++;
  }
  
  //free headers
  free_table_header(th_header);
  free_db_header(db_header);
  //unpin all the pages
  CHECK(unpinPage(buffer_manager, page_handler_table));
  CHECK(unpinPage(buffer_manager, page_handler_db));

  return numTuples;
}

// handling records in a table
RC insertRecord (RM_TableData *rel, Record *record) {
  char *tableName = rel->name; //name of the table

  CHECK(pinPage(buffer_manager, page_handler_db, 0)); //page handler for database header
  DB_header *db_header = read_db_serializer(page_handler_db->data); //DB_hearder structure for the data in database header

  //index of the table in the array
  int table_pos_in_array = searchStringArray(tableName, db_header->tableNames, db_header->numTables);
  if(table_pos_in_array < 0) return RC_TABLE_NOT_FOUND;

  int table_page_num = db_header->tableHeaders[table_pos_in_array];
  
  BM_PageHandle *page_handler_table = MAKE_PAGE_HANDLE(); //page handler for the table header
  CHECK(pinPage(buffer_manager, page_handler_table, table_page_num));
  Table_Header *th_header = read_table_serializer(page_handler_table->data); //Table_Header strudture for the data in table header

  //page handler for the page to be written, it will always be the last page of the table
  BM_PageHandle *page_handler_writing_page = MAKE_PAGE_HANDLE(); 
  CHECK(pinPage(buffer_manager, page_handler_writing_page, th_header->pagesList[th_header->numPages-1]));

  int recordSize = getRecordSize(th_header->schema);
  int offset = recordSize * th_header->nextSlot;

  char *insertOffset = page_handler_writing_page->data + offset;

  memcpy(insertOffset, record->data, recordSize);

  //for testing
/*  Record *testrecord;
  createRecord(&testrecord, rel->schema);
  memcpy(testrecord->data, insertOffset, recordSize);
  char *recordData = serializeRecord(testrecord, rel->schema);
  printf("\nThe record now have the following data: \n%s\n", recordData);
  free(recordData);*/

  //end of testing

  //makeDirty and unpin the writing page 
  CHECK(markDirty(buffer_manager, page_handler_writing_page));
  CHECK(unpinPage(buffer_manager, page_handler_writing_page));

  //update the handler for this table and the database
  add_record_to_header(th_header, db_header);

  //update table header
  char *table_header_data = write_table_serializer(th_header);
  memcpy(page_handler_table->data, table_header_data, getTable_Header_Size(th_header));
  free(table_header_data);
  free_table_header(th_header);
  CHECK(unpinPage(buffer_manager, page_handler_table));
  CHECK(markDirty(buffer_manager, page_handler_table));

  //update database header
  char *db_header_data = write_db_serializer(db_header);
  memcpy(page_handler_db->data, db_header_data, getDB_HeaderSize());
  free(db_header_data);
  free_db_header(db_header);
  CHECK(unpinPage(buffer_manager, page_handler_db));
  CHECK(markDirty(buffer_manager, page_handler_db));

  return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id) {
  char *tableName = rel->name;

  CHECK(pinPage(buffer_manager, page_handler_db, 0)); 
  DB_header *db_header = read_db_serializer(page_handler_db->data); 
  
  int table_pos_in_array = searchStringArray(tableName, db_header->tableNames, db_header->numTables);
  if(table_pos_in_array < 0) return RC_TABLE_NOT_FOUND;

  int table_page_num = db_header->tableHeaders[table_pos_in_array];
  
  BM_PageHandle *page_handler_table = MAKE_PAGE_HANDLE(); 
  CHECK(pinPage(buffer_manager, page_handler_table, table_page_num));
  
  Table_Header *th_header = read_table_serializer(page_handler_table->data); 
   
  int active_index = (id.page) * (th_header->slots_per_page) + id.slot;
  if(!(th_header->active[active_index])) return RC_RECORD_NOT_ACTIVE;
  
  if(th_header->active[active_index]) {
    th_header->active[active_index] = false;
  } else {
    return RC_RECORD_NOT_ACTIVE;
  }
  
  char *t_header_data = write_table_serializer(th_header);  
  memcpy(page_handler_table->data, t_header_data, getTable_Header_Size(th_header));
  
  
  CHECK(markDirty(buffer_manager, page_handler_table));
  
  CHECK(unpinPage(buffer_manager, page_handler_table));
  CHECK(unpinPage(buffer_manager, page_handler_db));

  free(t_header_data);
  free_db_header(db_header);
  free_table_header(th_header);

  return RC_OK;
}

RC updateRecord (RM_TableData *rel, Record *record) {
  char *tableName = rel->name; //name of the table

  CHECK(pinPage(buffer_manager, page_handler_db, 0)); //page handler for database header
  DB_header *db_header = read_db_serializer(page_handler_db->data); //DB_hearder structure for the data in database header

  RID id = record->id;

  //index of the table in the array
  int table_pos_in_array = searchStringArray(tableName, db_header->tableNames, db_header->numTables);
  if(table_pos_in_array < 0) return RC_TABLE_NOT_FOUND;

  int table_page_num = db_header->tableHeaders[table_pos_in_array];
  
  BM_PageHandle *page_handler_table = MAKE_PAGE_HANDLE(); //page handler for the table header
  CHECK(pinPage(buffer_manager, page_handler_table, table_page_num));
  Table_Header *th_header = read_table_serializer(page_handler_table->data); //Table_Header strudture for the data in table header

  //check if the slot is available for updating
  int active_index = (id.page) * (th_header->slots_per_page) + id.slot;
  if(!th_header->active[active_index]) return RC_RECORD_NOT_ACTIVE;

  //page handler for the page to be written, it will always be the last page of the table
  BM_PageHandle *page_handler_writing_page = MAKE_PAGE_HANDLE(); 
  CHECK(pinPage(buffer_manager, page_handler_writing_page, th_header->pagesList[id.page]));

  int recordSize = getRecordSize(th_header->schema);
  int offset = (id.slot) * recordSize;

  char *updateOffset = page_handler_writing_page->data + offset;

  memcpy(updateOffset, record->data, recordSize);

  //makeDirty and unpin the writing page 
  CHECK(markDirty(buffer_manager, page_handler_writing_page));
  CHECK(unpinPage(buffer_manager, page_handler_writing_page));

  //free both headers
  free_table_header(th_header);
  free_db_header(db_header);

  //unpin the two header pages
  CHECK(unpinPage(buffer_manager, page_handler_table));
  CHECK(unpinPage(buffer_manager, page_handler_db));

  return RC_OK;
}

RC getRecord (RM_TableData *rel, RID id, Record *record) {
  char *tableName = rel->name; //name of the table

  CHECK(pinPage(buffer_manager, page_handler_db, 0)); //page handler for database header
  DB_header *db_header = read_db_serializer(page_handler_db->data); //DB_hearder structure for the data in database header

  //index of the table in the array
  int table_pos_in_array = searchStringArray(tableName, db_header->tableNames, db_header->numTables);
  if(table_pos_in_array < 0) return RC_TABLE_NOT_FOUND;

  int table_page_num = db_header->tableHeaders[table_pos_in_array];
  
  BM_PageHandle *page_handler_table = MAKE_PAGE_HANDLE(); //page handler for the table header
  CHECK(pinPage(buffer_manager, page_handler_table, table_page_num));
  Table_Header *th_header = read_table_serializer(page_handler_table->data); //Table_Header strudture for the data in table header

  //check if the slot is available for updating
  int max_active_index = (th_header->numPages-1) * (th_header->slots_per_page) + th_header->nextSlot;
  int active_index = (id.page) * (th_header->slots_per_page) + id.slot;
  if((active_index < 0) || (active_index >= max_active_index)) return RC_RECORD_OUT_OF_RANGE;
  if(!th_header->active[active_index]) return RC_RECORD_NOT_ACTIVE;

  //page handler for the page to be written, it will always be the last page of the table
  BM_PageHandle *page_handler_reading_page = MAKE_PAGE_HANDLE(); 
  CHECK(pinPage(buffer_manager, page_handler_reading_page, th_header->pagesList[id.page]));

  int recordSize = getRecordSize(th_header->schema);
  int offset = (id.slot) * recordSize;

  char *readOffset = page_handler_reading_page->data + offset;
 
  createRecord(&record, th_header->schema); //create the record
  record->id.page = id.page;
  record->id.slot = id.slot;
  printf("\nRID of the returned record is page: %d, slot: %d\n", record->id.page, record->id.slot);

  memcpy(record->data, readOffset, recordSize);

  //free headers
  free_table_header(th_header);
  free_db_header(db_header);
  //unpin all the pages
  CHECK(unpinPage(buffer_manager, page_handler_reading_page));
  CHECK(unpinPage(buffer_manager, page_handler_table));
  CHECK(unpinPage(buffer_manager, page_handler_db));
  
  //for testing, to print recond info
  char *recordData = serializeRecord(record, rel->schema);
  printf("\nThe record now have the following data: \n%s\n", recordData);
  free(recordData);
  
  return RC_OK;
}

typedef struct Scan_Helper
{
  int nextRecord;
  Table_Header *th_header;
  Expr *cond;
} Scan_Helper;

// scans
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
  scan->rel = rel;

  Scan_Helper *sp = (Scan_Helper *)malloc(sizeof(Scan_Helper *));
  sp->nextRecord = 0;
  sp->cond = cond; // ?? or make a copy of cond ??

  char *tableName = rel->name; //name of the table

  CHECK(pinPage(buffer_manager, page_handler_db, 0)); //page handler for database header
  DB_header *db_header = read_db_serializer(page_handler_db->data); //DB_hearder structure for the data in database header

  //index of the table in the array
  int table_pos_in_array = searchStringArray(tableName, db_header->tableNames, db_header->numTables);
  if(table_pos_in_array < 0) return RC_TABLE_NOT_FOUND;

  int table_page_num = db_header->tableHeaders[table_pos_in_array];
  
  BM_PageHandle *page_handler_table = MAKE_PAGE_HANDLE(); //page handler for the table header
  CHECK(pinPage(buffer_manager, page_handler_table, table_page_num));
  Table_Header *th_header = read_table_serializer(page_handler_table->data); //Table_Header strudture for the data in table header

  sp->th_header = th_header;
  
  scan->mgmtData = sp;

  free_db_header(db_header);
  //unpin all the pages
  CHECK(unpinPage(buffer_manager, page_handler_table));
  CHECK(unpinPage(buffer_manager, page_handler_db));

  return RC_OK;
}

RC next (RM_ScanHandle *scan, Record *record) {
  RID *id = (RID *)malloc(sizeof(RID *));
  RC returnCode;
  Record *currentRecord = (Record *)malloc(sizeof(Record *));
  Value *result = (Value *)malloc(sizeof(Value *));
  Scan_Helper *sp = (Scan_Helper *)(scan->mgmtData);

  //initialize id value
  id->page = (int)(sp->nextRecord / sp->th_header->slots_per_page);
  id->slot = sp->nextRecord % sp->th_header->slots_per_page;

  if(sp->cond == NULL)
    MAKE_VALUE(result, DT_BOOL, true);
  // printf("%s", result->v.boolV ? "true" : "false");
  while((returnCode = getRecord(scan->rel, *id, currentRecord)) != RC_RECORD_OUT_OF_RANGE)
  {
    if(returnCode == RC_RECORD_NOT_ACTIVE) continue;
    
    if(sp->cond != NULL)
      evalExpr(currentRecord, scan->rel->schema, sp->cond, &result);
    
    if(result->v.boolV) {
      record = currentRecord; //?? or make a copy of current record??
      sp->nextRecord++;
      return RC_OK;
    } else {
      sp->nextRecord++; // update nextRecord value
      
      //update id value, and search again
      id->page = (int)(sp->nextRecord / sp->th_header->slots_per_page);
      id->slot = sp->nextRecord % sp->th_header->slots_per_page;
    }
  }

  return RC_RM_NO_MORE_TUPLES;
}

RC closeScan (RM_ScanHandle *scan) {
  Scan_Helper *sp = (Scan_Helper *)(scan->mgmtData);
  //free(sp->nextRecord);
  free_table_header(sp->th_header);
  free(sp);
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
  int i;
  for(i = 0; i < schema->numAttr; i++) {
    free(schema->attrNames[i]);
  }
  free(schema->attrNames);
  free(schema->dataTypes);
  free(schema->typeLength);
  free(schema->keyAttrs);

  free(schema);

  return RC_OK;
}


// dealing with records and attribute values
RC createRecord (Record **record, Schema *schema) {
  Record *rec = NULL;
  rec = (Record *)malloc(sizeof(Record));
  
  rec->id.page = 0;
  rec->id.slot = 0;
  
  rec->data = (char *)malloc(sizeof(char) * getRecordSize(schema));

  *record = rec;

  return RC_OK;
}

RC freeRecord (Record *record) {
  free(record->data);
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

