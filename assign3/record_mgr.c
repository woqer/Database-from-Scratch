#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "buffer_mgr.h"
#include "rm_serializer.c"

#define PAGES_LIST 1000

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
  int nextSlot;
  int slots_per_page;
  int headerNumPages;
  int pagesList[PAGES_LIST];
  int headerPagesList[PAGES_LIST];
  bool *active; // size is numPages*slots_per_page
} Table_Header;

static BM_BufferPool *buffer_manager;
static BM_PageHandle *page_handler_db;

char *write_db_serializer(DB_header *header);
char *write_schema_serializer(Schema *schema);
char *write_table_serializer(Table_Header *th, DB_header *header);
DB_header *read_db_serializer(char *data);
Schema *read_schema_serializer(char *data);
Table_Header *read_table_serializer(char *data, DB_header *db_header);


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
  Table_Header *th;
  th = malloc(sizeof(*th));
  
  th->nextSlot = 0;
  th->numPages = 0;
  th->headerNumPages = 0;

  // first element is table header itself, unknown value yet
  // initialized to -1 just in case it will not override any existent page
  th->headerPagesList[0] = -1;

  th->schema = schema;
  
  float slots_per_page_decimal = PAGE_SIZE / getRecordSize(schema);
  th->slots_per_page = (int)slots_per_page_decimal;
  
  th->active = malloc(sizeof(*(th->active)) * th->slots_per_page);
  
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
    // printf("Freeing tableNames[%i]: %s\n", i, head->tableNames[i]);
    free(head->tableNames[i]);
  }
  // printf("Freeing tableNames");
  free(head->tableNames);
  free(head->tableHeaders);
  free(head);
}

void free_table_header(Table_Header *th) {
  free(th->active);
  free(th);
}


// returns the current available slot
RID add_record_to_header(Table_Header *th, DB_header *db_header) {
  int active_index = ((th->numPages -1) * th->slots_per_page) + th->nextSlot;
  th->active[active_index] = true;
  
  if (th->nextSlot + 1 > th->slots_per_page) {
    // need a new page
    th->nextSlot = 0;
    
    if (th->numPages > PAGES_LIST) {
      // realloc pagesList
      printf("\n############ E R R O R ############\n");
      printf("Reallocing memory!!! numPages = %d\n", th->numPages);
      printf("############ E R R O R ############\n\n");
      // th->pagesList = (int *)realloc(th->pagesList, sizeof(int) * th->numPages);
    }

    th->pagesList[th->numPages++] = db_header->nextAvailPage++;
    
    // printf("New page (%d)\n", th->pagesList[th->numPages-1]);
    int new_length = th->slots_per_page * th->numPages;
    printf("Reallocing th->active to length %d\n", new_length);
    void* returned = realloc(th->active, sizeof(bool) * new_length);
    
    if (returned == NULL) {
      printf("\n-------------- E R R O R --------------3\n");
      printf("realloc returned NULL!!!");
      exit(-1);
    } else {
      printf("realloc returned address: %p\n", returned);
    }

    th->active = (bool *)returned;
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
  int size = sizeof(int) * 4; // numpages & nextSlot & slots_per_page & headerNumPages
  int max = PAGE_SIZE;
  // *pagesList and *headerPagesList
  size += sizeof(int) * 2 * PAGES_LIST;

  // for now, we are only accepting 100 pages per table, no reallocation
  int i, realloc_times;
  realloc_times = (int)(th->numPages / PAGES_LIST);
  for (i = 0; i < realloc_times; i++) {
    size += sizeof(int) * PAGES_LIST;
  }
  
  size += getSchemaSize(th->schema); // *schema
  size += sizeof(bool) * th->numPages * th->slots_per_page; // *active
  
  // printf("--- Size is (%d), ternary got (%d)\n", size, size > max ? max : size);

  return size > max ? max : size;
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
  printf("keySize\t\t%i\n", schema->keySize);
  int i;
  for (i = 0; i < schema->numAttr; i++) {
    printf("attrNames[%i]\t\t%s\n", i, schema->attrNames[i]);
    printf("dataTypes[%i]\t\t%d\n", i, schema->dataTypes[i]);
    printf("typeLength[%i]\t\t%d\n", i, schema->typeLength[i]);
  }
  for (i = 0; i < schema->keySize; i++) {
    printf("keyAttrs[%i]\t\t%d\n", i, schema->keyAttrs[i]);
  }
}

void printTable_Header(Table_Header *th) {
  printf("******************************\n");
  printf("Printing Table_Header...!!!\n");
  printf("numPages\t\t%d\n", th->numPages);
  printf("headerNumPages\t\t%d\n", th->headerNumPages);
  printf("nextSlot\t\t%d\n", th->nextSlot);
  printf("slots_per_page\t\t%d\n", th->slots_per_page);
  int active_size = th->numPages * th->slots_per_page;
  int i;
  for (i = 0; i < th->numPages; i++) {
    printf("pagesList[%i]\t\t%d\n", i, th->pagesList[i]);
  }
  for (i = 0; i < th->headerNumPages; i++) {
    printf("headerPagesList[%i]\t\t%d\n", i, th->headerPagesList[i]);
  }
  // for (i = 0; i < active_size; i++) {
  //   printf("active[%i]\t\t%s\n", i, th->active[i] ? "true" : "false");
  // }
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

char *write_table_serializer(Table_Header *th, DB_header *db_header) {
  int size = getTable_Header_Size(th);
  char *out = (char *)malloc(sizeof(char) * size);

  int int_size = sizeof(int);
  int str_size = sizeof(int);
  int bool_size = sizeof(bool);
  int active_length = th->numPages * th->slots_per_page;
  int active_size = active_length * bool_size;

  int schema_size = getSchemaSize(th->schema);

  memcpy(out, &(th->numPages), int_size);
  int offset = int_size;
  memcpy(out + offset, &(th->nextSlot), int_size);
  offset += int_size;
  memcpy(out + offset, &(th->slots_per_page), int_size);
  offset += int_size;
  memcpy(out + offset, &(th->headerNumPages), int_size);
  offset += int_size;

  int i;
  for (i = 0; i < th->numPages; i++) {
    memcpy(out + offset, &(th->pagesList[i]), int_size);
    offset += int_size;
  }

  for (i = 0; i < th->headerNumPages; i++) {
    memcpy(out + offset, &(th->headerPagesList[i]), int_size);
    offset += int_size;
  }

  char *sch_data = write_schema_serializer(th->schema);
  memcpy(out + offset, sch_data, schema_size);
  offset += schema_size;


  /////////////////////////////////
  // Dealing with active[]...
  /////////////////////////////////
  int max_size = PAGE_SIZE - offset;

  // We make sure we can fit booleans (2 bytes)
  int active_offset = (int)(max_size / bool_size);
  int remaining_size = active_size;
  int active_to_end = active_offset * bool_size;
  int write_size;
  if (active_size < active_to_end) {
    write_size = active_size;
  } else {
    write_size = active_to_end;
  }

  // copy to first page active[] content that fits
  memcpy(out + offset, th->active, write_size);
  remaining_size -= write_size;
  active_offset = (write_size / bool_size);

  // printf("First memcpy of active . . .  O K ! ! !\n");
  // Need more pages!!!
  bool changed = false;
  if ((active_size + offset) > PAGE_SIZE) {
    printf("W_table_header_serializer... Needing more pages!!!\n");
    // use pages already used for active[]
    int j;
    for (j = 1; j < th->headerNumPages; j++) {
      printf("W_table_header_serializer... using already used pages\n");
      write_size = remaining_size > PAGE_SIZE ? PAGE_SIZE : remaining_size;

      BM_PageHandle *page_handler = MAKE_PAGE_HANDLE();
      pinPage(buffer_manager, page_handler, th->headerPagesList[i]);
    
      memcpy(page_handler->data, th->active + active_offset, write_size);
      active_offset += (write_size / bool_size);
      remaining_size -= write_size;

      markDirty(buffer_manager, page_handler);
      unpinPage(buffer_manager, page_handler);
    }

    while (remaining_size > 0) { // Need to allocate more pages
      BM_PageHandle *page_handler = MAKE_PAGE_HANDLE();
      write_size = remaining_size > PAGE_SIZE ? PAGE_SIZE : remaining_size;
      
      printDB_Header(db_header);
      printTable_Header(th);
      printf("W_table_header_serializer... creating new page\n");

      th->headerPagesList[th->headerNumPages++] = db_header->nextAvailPage;
      CHECK(pinPage(buffer_manager, page_handler, db_header->nextAvailPage++));

      memcpy(page_handler->data, th->active + active_offset, write_size);
      active_offset += (write_size / bool_size);
      remaining_size -= write_size;

      markDirty(buffer_manager, page_handler);
      unpinPage(buffer_manager, page_handler);

      changed = true;
      // MAX TABLE SIZE REACHED!!!
      if (th->headerNumPages >= PAGES_LIST) exit(RC_RM_MAX_TABLE_SIZE_REACHED);
    }

  }

  if (changed) {
    offset = 3 * int_size; // numPages & nextSlot & slots_per_page
    memcpy(out + offset, &(th->headerNumPages), int_size);
    
    offset += (th->numPages + 1) * int_size; // headerNumPages & pagesList[]
    for (i = 0; i < th->headerNumPages; i++) {
      memcpy(out + offset, &(th->headerPagesList[i]), int_size);
      offset += int_size;
    }
  }

  free(sch_data); //already copied

  return out;
}

// char[] *write_table_serializer(Table_Header *th) {
//   int size = getTable_Header_Size(th);
//   char *raw = w_table_serializer(th);

//   int max_size = PAGE_SIZE * th->headerNumPages;

//   char *out[th->headerNumPages];
//   int page_size = sizeof(*raw) * PAGE_SIZE;
//   int offset = 0;

//   int i;
//   for (i = 0; i < th->headerNumPages; i++) {
//     out[i] = raw + offset;
//     offset += page_size;
//   }

//   return out;
// }

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

Table_Header *read_table_serializer(char *data, DB_header *db_header) {
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
  memcpy(&(th->headerNumPages), data + offset, int_size);
  offset += int_size;

  int active_length = th->numPages * th->slots_per_page;
  
  th->active = malloc(sizeof(*(th->active)) * active_length);

  int i;
  for(i = 0; i < th->numPages; i++) {
    memcpy(&(th->pagesList[i]), data + offset, int_size);
    offset += int_size;
  }

  for(i = 0; i < th->headerNumPages; i++) {
    memcpy(&(th->headerPagesList[i]), data + offset, int_size);
    offset += int_size;
  }

  Schema *schema_aux = read_schema_serializer(data + offset);
  offset += getSchemaSize(schema_aux);  

  th->schema = schema_aux;

  // printSchema(th->schema);
  // printf("\n########## Schema printed!!!!!! size (%d) offset (%d)##########\n", getSchemaSize(th->schema), offset);


  /////////////////////////////////
  // Dealing with active[]...
  /////////////////////////////////
  int active_size = active_length * bool_size;
  int max_size = PAGE_SIZE - offset;

  // We make sure we can fit booleans (2 bytes)
  int active_offset = (int)(max_size / bool_size);
  int remaining_size = active_size;
  int active_to_end = active_offset * bool_size;
  int write_size;
  if (active_size < active_to_end) {
    write_size = active_size;
  } else {
    write_size = active_to_end;
  }

  // copy contents of first page to active[]
  memcpy(th->active, data + offset, write_size);
  remaining_size -= write_size;
  active_offset = (write_size / bool_size);

  // Need to read from more pages!!!
  if (th->headerNumPages > 1) {
    printf("R_table_header_serializer... reading from more pages!!!\n");

    // use pages already used for active[]
    int j;
    for (j = 1; j < th->headerNumPages; j++) {
      printf("R_table_header_serializer... reading page (%d)!!!\n", th->headerPagesList[i]);
      write_size = remaining_size > PAGE_SIZE ? PAGE_SIZE : remaining_size;

      BM_PageHandle *page_handler = MAKE_PAGE_HANDLE();
      pinPage(buffer_manager, page_handler, th->headerPagesList[i]);
    
      memcpy(th->active + active_offset, page_handler->data, write_size);
      active_offset += (write_size / bool_size);
      remaining_size -= write_size;

      markDirty(buffer_manager, page_handler);
      unpinPage(buffer_manager, page_handler);
    }

  }

  return th;
}

// // user (caller method) sh
// Table_Header *read_table_serializer(char[] *data) {
//   // read header information
//   Table_Header *th = r_table_serializer(data[0]);

//   // append active contents from other pages

// }

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
  
  // CHANGE THIS after testing!!!!
  // to 100 pages (4M in memory, not too much) and RS_LRU !
  initBufferPool(buffer_manager, pageFileName, 100, RS_LRU, NULL);


  CHECK(pinPage(buffer_manager, page_handler_db, 0));
  
  printf("Reading DB_header from disk...\n");
  DB_header *db_header = read_db_serializer(page_handler_db->data);

  if (db_header->numTables <= 0) {
    printf("No DB_Header in disk (numTables = %d), writing a new one...\n", db_header->numTables);

    DB_header *db_read = createDB_header();
    char *data = write_db_serializer(db_read);

    memcpy(page_handler_db->data, data, getDB_HeaderSize());

    CHECK(markDirty(buffer_manager, page_handler_db));

    free(data);
    free_db_header(db_read);

  } else {
    printf("****** Printing already existent DB_Header... ******\n");
    printDB_Header(db_header);
  }

  CHECK(unpinPage(buffer_manager, page_handler_db));

  printf("InitRecord printing page_handler_db...\n");
  printf("page_handler_db->pageNum\t\t%d\n", page_handler_db->pageNum);

  free_db_header(db_header);

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
  // printDB_Header(db_header);
  // nextAvailPage is for table header and the next one will be the first page
  // to write records on this table
  // printDB_Header(db_header);

  int table_page_num = db_header->nextAvailPage;
  CHECK(pinPage(buffer_manager, page_handler_table, table_page_num));
  th->headerPagesList[th->headerNumPages++] = table_page_num;
  th->pagesList[th->numPages++] = table_page_num + 1;
  
  // creating empty page for first records
  CHECK(pinPage(buffer_manager, page_handler_empty, table_page_num + 1));
  memset(page_handler_empty->data, 0, PAGE_SIZE);
  CHECK(markDirty(buffer_manager, page_handler_empty));
  CHECK(unpinPage(buffer_manager, page_handler_empty));

  // update db_header
  db_header->tableHeaders[db_header->numTables] = table_page_num;
  strcpy(db_header->tableNames[db_header->numTables], name);
  db_header->numTables++;
  db_header->nextAvailPage += 2;

  // printDB_Header(db_header);

  char *table_data = write_table_serializer(th, db_header);
  memcpy(page_handler_table->data, table_data, getTable_Header_Size(th));
  free(table_data);
  markDirty(buffer_manager, page_handler_table);
  unpinPage(buffer_manager, page_handler_table);

  char *db_data = write_db_serializer(db_header);
  memcpy(page_handler_db->data, db_data, getDB_HeaderSize());
  free(db_data);
  markDirty(buffer_manager, page_handler_db);
  unpinPage(buffer_manager, page_handler_db);

  // free(page_handler_table);
  // free(page_handler_empty);

  printf("Freeing table header...\n");
  free_table_header(th);
  printf("Freeing db header...\n");
  free_db_header(db_header);

  // printf("Returning from createTable\n");

  return RC_OK;
}

RC openTable (RM_TableData *rel, char *name) {
  if(sizeof(name) > ATTR_SIZE) return RC_TABLE_NAME_TOO_LONG;

  CHECK(pinPage(buffer_manager, page_handler_db, 0)); //page handler for database header
  DB_header *db_header = read_db_serializer(page_handler_db->data); //DB_hearder structure for the data in database header

  //index of the table in the array
  int table_pos_in_array = searchStringArray(name, db_header->tableNames, db_header->numTables);
  if(table_pos_in_array < 0) {
    CHECK(unpinPage(buffer_manager, page_handler_db));
    return RC_TABLE_NOT_FOUND;
  }

  int table_page_num = db_header->tableHeaders[table_pos_in_array];
  
  BM_PageHandle *page_handler_table = MAKE_PAGE_HANDLE(); //page handler for the table header
  CHECK(pinPage(buffer_manager, page_handler_table, table_page_num));
  Table_Header *th_header = read_table_serializer(page_handler_table->data, db_header); //Table_Header strudture for the data in table header

  rel->name = (char *)malloc(sizeof(char *) * ATTR_SIZE);
  strcpy(rel->name, name);

  Schema *schema = th_header->schema;
  rel->schema = createSchema (schema->numAttr, schema->attrNames, schema->dataTypes, schema->typeLength, schema->keySize, schema->keyAttrs);

  CHECK(markDirty(buffer_manager, page_handler_table));

  CHECK(unpinPage(buffer_manager, page_handler_table));
  CHECK(unpinPage(buffer_manager, page_handler_db));

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
  if(table_pos_in_array < 0) {
    CHECK(unpinPage(buffer_manager, page_handler_db));
    return RC_TABLE_NOT_FOUND;
  }

  int i;
  for(i=table_pos_in_array; i<db_header->numTables-1; i++)
  {
    db_header->tableNames[i] = db_header->tableNames[i+1];
    db_header->tableHeaders[i] = db_header->tableHeaders[i+1];
  }
  db_header->numTables--;

  char *db_header_data = write_db_serializer(db_header);
  CHECK(markDirty(buffer_manager, page_handler_db));
  memcpy(page_handler_db->data, db_header_data, getDB_HeaderSize());
  free(db_header_data);
  free_db_header(db_header);
  

  CHECK(unpinPage(buffer_manager, page_handler_db));

  return RC_OK;
}

int getNumTuples (RM_TableData *rel) {
  char *tableName = rel->name; //name of the table

  CHECK(pinPage(buffer_manager, page_handler_db, 0)); //page handler for database header
  DB_header *db_header = read_db_serializer(page_handler_db->data); //DB_hearder structure for the data in database header

  //index of the table in the array
  int table_pos_in_array = searchStringArray(tableName, db_header->tableNames, db_header->numTables);
  if(table_pos_in_array < 0){
    CHECK(unpinPage(buffer_manager, page_handler_db));
    return RC_TABLE_NOT_FOUND;
  }

  int table_page_num = db_header->tableHeaders[table_pos_in_array];
  
  BM_PageHandle *page_handler_table = MAKE_PAGE_HANDLE(); //page handler for the table header
  CHECK(pinPage(buffer_manager, page_handler_table, table_page_num));
  Table_Header *th_header = read_table_serializer(page_handler_table->data, db_header); //Table_Header strudture for the data in table header

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
  if(table_pos_in_array < 0){
    CHECK(unpinPage(buffer_manager, page_handler_db));
    return RC_TABLE_NOT_FOUND;
  }

  int table_page_num = db_header->tableHeaders[table_pos_in_array];
  
  BM_PageHandle *page_handler_table = MAKE_PAGE_HANDLE(); //page handler for the table header
  CHECK(pinPage(buffer_manager, page_handler_table, table_page_num));
  Table_Header *th_header = read_table_serializer(page_handler_table->data, db_header); //Table_Header strudture for the data in table header

  add_record_to_header(th_header, db_header);

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
  //  WRONG !!! FIRST ADD RECORD TO HEADER AND THEN DO THE REAL THING
  // add_record_to_header(th_header, db_header);

  //update table header
  char *table_header_data = write_table_serializer(th_header, db_header);
  memcpy(page_handler_table->data, table_header_data, getTable_Header_Size(th_header));
  free(table_header_data);
  free_table_header(th_header);
  CHECK(markDirty(buffer_manager, page_handler_table));
  CHECK(unpinPage(buffer_manager, page_handler_table));

  //update database header
  char *db_header_data = write_db_serializer(db_header);
  memcpy(page_handler_db->data, db_header_data, getDB_HeaderSize());
  free(db_header_data);
  free_db_header(db_header);
  CHECK(markDirty(buffer_manager, page_handler_db));
  CHECK(unpinPage(buffer_manager, page_handler_db));

  return RC_OK;
}

RC deleteRecord (RM_TableData *rel, RID id) {
  char *tableName = rel->name;

  CHECK(pinPage(buffer_manager, page_handler_db, 0)); 
  DB_header *db_header = read_db_serializer(page_handler_db->data); 
  
  int table_pos_in_array = searchStringArray(tableName, db_header->tableNames, db_header->numTables);
  if(table_pos_in_array < 0){
    CHECK(unpinPage(buffer_manager, page_handler_db));
    return RC_TABLE_NOT_FOUND;
  }

  int table_page_num = db_header->tableHeaders[table_pos_in_array];
  
  BM_PageHandle *page_handler_table = MAKE_PAGE_HANDLE(); 
  CHECK(pinPage(buffer_manager, page_handler_table, table_page_num));
  
  Table_Header *th_header = read_table_serializer(page_handler_table->data, db_header); 
   
  int active_index = (id.page) * (th_header->slots_per_page) + id.slot;
  if(!(th_header->active[active_index])){
    CHECK(unpinPage(buffer_manager, page_handler_table));
    CHECK(unpinPage(buffer_manager, page_handler_db));
    return RC_RECORD_NOT_ACTIVE;
  }
  
  if(th_header->active[active_index]) {
    th_header->active[active_index] = false;
  } else {
    CHECK(unpinPage(buffer_manager, page_handler_table));
    CHECK(unpinPage(buffer_manager, page_handler_db));
    return RC_RECORD_NOT_ACTIVE;
  }

  CHECK(markDirty(buffer_manager, page_handler_table));  
  char *t_header_data = write_table_serializer(th_header, db_header);  
  memcpy(page_handler_table->data, t_header_data, getTable_Header_Size(th_header));
  
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
  if(table_pos_in_array < 0){
    CHECK(unpinPage(buffer_manager, page_handler_db));
    return RC_TABLE_NOT_FOUND;
  }

  int table_page_num = db_header->tableHeaders[table_pos_in_array];
  
  BM_PageHandle *page_handler_table = MAKE_PAGE_HANDLE(); //page handler for the table header
  CHECK(pinPage(buffer_manager, page_handler_table, table_page_num));
  Table_Header *th_header = read_table_serializer(page_handler_table->data, db_header); //Table_Header strudture for the data in table header

  //check if the slot is available for updating
  int active_index = (id.page) * (th_header->slots_per_page) + id.slot;
  if(!th_header->active[active_index]){
    CHECK(unpinPage(buffer_manager, page_handler_table));
    CHECK(unpinPage(buffer_manager, page_handler_db));
    return RC_RECORD_NOT_ACTIVE;
  }

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
  if(table_pos_in_array < 0) {
    CHECK(unpinPage(buffer_manager, page_handler_db));
    return RC_TABLE_NOT_FOUND;
  }

  int table_page_num = db_header->tableHeaders[table_pos_in_array];
  
  BM_PageHandle *page_handler_table = MAKE_PAGE_HANDLE(); //page handler for the table header
  CHECK(pinPage(buffer_manager, page_handler_table, table_page_num));
  Table_Header *th_header = read_table_serializer(page_handler_table->data, db_header); //Table_Header strudture for the data in table header

  //check if the slot is available for updating
  int max_active_index = (th_header->numPages-1) * (th_header->slots_per_page) + th_header->nextSlot;
  int active_index = (id.page) * (th_header->slots_per_page) + id.slot;
  if((active_index < 0) || (active_index >= max_active_index)) {
    CHECK(unpinPage(buffer_manager, page_handler_table));
    CHECK(unpinPage(buffer_manager, page_handler_db));
    return RC_RECORD_OUT_OF_RANGE;
  }
  if(!th_header->active[active_index]) {
    CHECK(unpinPage(buffer_manager, page_handler_table));
    CHECK(unpinPage(buffer_manager, page_handler_db));
    return RC_RECORD_NOT_ACTIVE;
  }

  //page handler for the page to be written, it will always be the last page of the table
  BM_PageHandle *page_handler_reading_page = MAKE_PAGE_HANDLE(); 
  CHECK(pinPage(buffer_manager, page_handler_reading_page, th_header->pagesList[id.page]));

  int recordSize = getRecordSize(th_header->schema);
  int offset = (id.slot) * recordSize;

  char *readOffset = page_handler_reading_page->data + offset;

  // createRecord(&record, rel->schema); //create the record
  record->id.page = id.page;
  record->id.slot = id.slot;

  memcpy(record->data, readOffset, recordSize);

  //free headers
  free_table_header(th_header);
  free_db_header(db_header);
  //unpin all the pages
  CHECK(unpinPage(buffer_manager, page_handler_reading_page));
  CHECK(unpinPage(buffer_manager, page_handler_table));
  CHECK(unpinPage(buffer_manager, page_handler_db));
  
  //for testing, to print recond info
  // printf("\nRID of the returned record is page: %d, slot: %d\n", record->id.page, record->id.slot);
  // char *recordData = serializeRecord(record, rel->schema);
  // printf("\nThe record now have the following data: \n%s\n", recordData);
  // free(recordData);
  
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
  if(table_pos_in_array < 0) {
    CHECK(unpinPage(buffer_manager, page_handler_db));
    return RC_TABLE_NOT_FOUND;
  }

  int table_page_num = db_header->tableHeaders[table_pos_in_array];
  
  BM_PageHandle *page_handler_table = MAKE_PAGE_HANDLE(); //page handler for the table header
  CHECK(pinPage(buffer_manager, page_handler_table, table_page_num));
  Table_Header *th_header = read_table_serializer(page_handler_table->data,db_header); //Table_Header strudture for the data in table header

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
  
  Value *result;
  Scan_Helper *sp = (Scan_Helper *)(scan->mgmtData);

  //initialize id value
  id->page = (int)(sp->nextRecord / sp->th_header->slots_per_page);
  id->slot = sp->nextRecord % sp->th_header->slots_per_page;

  if(sp->cond == NULL)
  {
    MAKE_VALUE(result, DT_BOOL, true);
  }
  
  while((returnCode = getRecord(scan->rel, *id, record)) != RC_RECORD_OUT_OF_RANGE)
  {
    printf("record_mgr.next: .......... inside while\n");
    if(returnCode == RC_RECORD_NOT_ACTIVE)
    {
      sp->nextRecord++; // update nextRecord value
      
      //update id value, and search again
      id->page = (int)(sp->nextRecord / sp->th_header->slots_per_page);
      id->slot = sp->nextRecord % sp->th_header->slots_per_page;
      continue;
    } 
    
    if(sp->cond != NULL)
      evalExpr(record, scan->rel->schema, sp->cond, &result);
    
    if(result->v.boolV) {
      sp->nextRecord++;
      
      return RC_OK;
    } else {
      sp->nextRecord++; // update nextRecord value
      
      //update id value, and search again
      id->page = (int)(sp->nextRecord / sp->th_header->slots_per_page);
      id->slot = sp->nextRecord % sp->th_header->slots_per_page;
    }
  }
  
  printf("record_mgr.next: .......... after while\n");

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

