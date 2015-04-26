#include <stdlib.h>
#include <unistd.h>
#include "dberror.h"
#include "expr.h"
#include "record_mgr.h"
#include "tables.h"
#include "test_helper.h"


Schema  *testCreateSchema() {
  int numAttr = 5;
  char **attrNames = (char *[]) {"id", "name", "last_name", "weight", "active"};
  DataType *dataTypes = (DataType []) { DT_INT, DT_STRING, DT_STRING, DT_FLOAT, DT_BOOL};
  int *typeLength = (int []) { sizeof(int)       ,
                      sizeof(char) * 20 ,
                      sizeof(char) * 32 ,
                      sizeof(float)     ,
                      sizeof(bool) };
  int keySize = 3;
  int *keys   = (int []) {0, 3, 4};

  Schema *schema = createSchema(numAttr, attrNames, dataTypes, typeLength, keySize, keys);

  numAttr = 123;
  attrNames = (char *[]) {"asdf"};
  dataTypes = (DataType []) {DT_BOOL};
  typeLength = (int []) {123};
  keySize = 123;
  keys = (int []) {123};

  char *schemaData = serializeSchema(schema);
  printf("\nThe schema now has the following data:\n%s\n", schemaData);
  free(schemaData);

  /*printf("numAttr:\t\t%i\n", schema->numAttr);
  printf("keySize:\t\t%i\n", schema->keySize);

  int i;
  for (i = 0; i < schema->numAttr; i ++) {
    printf("attrNames[%i]:\t\t%s\n", i, (schema->attrNames)[i]);
    printf("dataTypes[%i]:\t\t%i\n", i, (schema->dataTypes)[i]);
    printf("typeLength[%i]:\t\t%i\n", i, (schema->typeLength)[i]);
  }
  for (i = 0; i < schema->keySize; i++) {
    printf("keyAttrs[%i]:\t\t%i\n", i, (schema->keyAttrs)[i]);
  }  */

  return schema;
}

int testgetRecordSize(Schema *schema) {
  int size;
  size = getRecordSize(schema);
  printf("Size of schema:\t\t%i\n", size);
  return size;
}

Record *testCreateRecord(Schema *schema) {
  Record *record;
  createRecord(&record, schema);

  printf("record->id->page:\t\t%i\n", (record->id).page);
  printf("record->id->slot:\t\t%i\n", (record->id).slot);
  printf("record->data:\t\t%s\n", record->data);

  // record->data = "1WilfridoVidana78.5true";

  // printf("Data after writing something...\n");
  // printf("record->data:\t\t%s\n", record->data);

  return record;
}

Record *testSetAttr(Schema *schema)
{
	Record *record;
	createRecord(&record, schema);
	
	Value *val_0, *val_1, *val_2, *val_3, *val_4;
	
	char *buff_1, *buff_2;
	buff_1 = "An";
	buff_2 = "Shi";
	
	MAKE_VALUE(val_0, DT_INT, 1021);
	MAKE_STRING_VALUE(val_1, buff_1);
	MAKE_STRING_VALUE(val_2, buff_2);
	MAKE_VALUE(val_3, DT_FLOAT, 88.55);
	MAKE_VALUE(val_4, DT_BOOL, 1);
	
	setAttr(record, schema, 0, val_0);
	setAttr(record, schema, 1, val_1);
	setAttr(record, schema, 2, val_2);
	setAttr(record, schema, 3, val_3);
	setAttr(record, schema, 4, val_4);
	
  char *recordData = serializeRecord(record, schema);
  printf("\nThe record now have the following data: \n%s", recordData);
  free(recordData);

	return record;
}

Record *testAnotherSetAttr(Schema *schema)
{
  Record *record;
  createRecord(&record, schema);
  
  Value *val_0, *val_1, *val_2, *val_3, *val_4;
  
  char *buff_1, *buff_2;
  buff_1 = "Wilfrido";
  buff_2 = "Sayegh";
  
  MAKE_VALUE(val_0, DT_INT, 6258);
  MAKE_STRING_VALUE(val_1, buff_1);
  MAKE_STRING_VALUE(val_2, buff_2);
  MAKE_VALUE(val_3, DT_FLOAT, 33.21);
  MAKE_VALUE(val_4, DT_BOOL, 0);
  
  setAttr(record, schema, 0, val_0);
  setAttr(record, schema, 1, val_1);
  setAttr(record, schema, 2, val_2);
  setAttr(record, schema, 3, val_3);
  setAttr(record, schema, 4, val_4);

  return record;
}

Record *testGetAttr(Schema *schema){
  Record *record;
  record = testSetAttr(schema);  

  int i;
  for(i=0; i<schema->numAttr; i++)
  {
	Value *value;
	printf("The %ith attribute in the record is:\n",i);
	getAttr(record, schema, i, &value);
	
	switch(schema->dataTypes[i])
    {
    case DT_INT: 
	{
		printf("%d\n",value->v.intV);
		free(value);
	}
    break;
    case DT_STRING:
	{
		printf("%s\n", value->v.stringV);
		free(value->v.stringV);
		free(value);
	}
    break;
    case DT_FLOAT:
	{
		printf("%f\n",value->v.floatV);
		free(value);
	}
    break;
    case DT_BOOL:
	{
		printf("%s\n", value->v.floatV ? "true" : "false");
		free(value);
	}
    break;
    default: break;
    }
  }
  return record;
}


RM_TableData *testOpenTable() {
  RM_TableData *rel = (RM_TableData *)malloc(sizeof(RM_TableData));
  RC return_code = openTable(rel, "stupid_table");
  
  printf("\nstupid_table opened\n");
  char *table_data = serializeTableContent(rel);
  printf("\n%s\n", table_data);
  free(table_data);
  /*printf("\nTable name in the RM_TableData structure\t\t%s\n", rel->name);
  printf("\nSchema in the RM_TableData structure:\n");
  printSchema(rel->schema);*/
  printf("\nReturn code from openTable: \t\t%d\n", return_code);
  return rel;
}

void testDeleteTable()
{
  RC return_code = deleteTable("stupid_table");
  printf("\nReturn code from deleteTable: \t\t%d\n", return_code);
  printf("\nstupid_table deleted\n");
}

void testInsertRecord(RM_TableData *rel, Record *record) {
  printf("The number of tuples before any insertion is\t\t%d\n", getNumTuples(rel));
  insertRecord(rel, record);
  printf("The number of tuples after one insertion is\t\t%d\n", getNumTuples(rel));

  //insert again
  insertRecord(rel, record);
  printf("The number of tuples after two insertions is\t\t%d\n", getNumTuples(rel));
  printf("\nTable name in the RM_TableData structure\t\t%s\n", rel->name);
  printf("\nSchema in the RM_TableData structure:\n");
  printSchema(rel->schema);
/*  char *table_data = serializeTableContent(rel);
  printf("got here");
  printf("\n%s\n", table_data);
  free(table_data);*/
}

void testGetRecord(RM_TableData *rel, RID id) {
  Record *record;
  createRecord(&record, rel->schema);
  RC r = getRecord(rel, id, record);
  printf("\nReturn code from getRecord is %d\n", r);
  printf("\nRID of the returned record is page: %d, slot: %d\n", record->id.page, record->id.slot);

  char *recordData = serializeRecord(record, rel->schema);
  printf("\nThe record now have the following data: \n%s\n", recordData);
  free(recordData);

  printf("Test record not found\n");
  id.page = 0;
  id.slot = 2;
  r = getRecord(rel, id, record);
  printf("\nReturn code from getRecord is %d\n", r);
}

void testUpdateRecord(RM_TableData *rel, Record *anotherRecord) {
  updateRecord(rel, anotherRecord);
  RID id;
  id.page = 0;
  id.slot = 0;
  //testGetRecord(rel, id);
  // char *tableData = serializeTableContent(rel);
  // printf("Table Content\n%s\n", tableData);
  // free(tableData);
}

void testDeleteRecord(RM_TableData *rel)
{
  RID id;
  id.page = 0;
  id.slot = 1;
  printf("\nReturn code from deleteRecord is %d\n",deleteRecord(rel, id));
  testGetRecord(rel, id);

}

RM_ScanHandle *testStartScan(RM_TableData *rel) {
  RM_ScanHandle *scan = (RM_ScanHandle *)malloc(sizeof(RM_ScanHandle *));
  printf("\nRecord code from startScan is %d\n",startScan(rel, scan, NULL));
  printf("\nName in scan is %s\n", scan->rel->name);
  return scan;
}

void testNext(RM_ScanHandle *scan) {
  Record *record;
  createRecord(&record,scan->rel->schema);
  //printf("\nRecord code from next is %d\n",next(scan, record));

  while(next(scan, record) != RC_RM_NO_MORE_TUPLES) 
  {
    printf("\nGot record\n %s\n", serializeRecord(record, scan->rel->schema));
  }

  printf("\nRecord code from closeScan is %d\n",closeScan(scan));
}


int main() {
  printf("\n************************Testing initRecordManager************************\n");
  initRecordManager(NULL);
  

  // printf("\nTESTING READING PAGE 1\n");
  // testReadingPage();

  // printf("\nTESTING WRITING PAGE 1\n");
  // testWritingPage();

  printf("\n Testing creating schema\n");
  Schema *schema = testCreateSchema();

  printf("\n Testing create record\n");
  Record *record = testSetAttr(schema);

  printf("\n**********************Testing createTable******************\n");
  createTable("stupid_table", schema);
  createTable("another_table", schema);



  // printf("\n*********************Testing shutDownRecordManager**********************\n");
  // shutdownRecordManager();
  // exit(0);




  printf("\n***********************Testing openTable*********************\n");
  RM_TableData *rel = testOpenTable();


  //printf("\n***********************Testing deleteTable*********************\n");
  //testDeleteTable();

  printf("\n***********************Testing insertRecord*********************\n");
  testInsertRecord(rel, record);

  printf("\n***********************Testing getRecord*********************\n");
  RID id;
  id.page = 0;
  id.slot = 0;
  testGetRecord(rel, id);

  printf("\n***********************Testing updateRecord*********************\n");
  Record *anotherRecord = testAnotherSetAttr(rel->schema);
  testUpdateRecord(rel, anotherRecord);

  // printf("\n***********************Testing deleteRecord*********************\n");
  // testDeleteRecord(rel);

  printf("\n***********************Testing startScan*********************\n");
  RM_ScanHandle *scan = testStartScan(rel);

  printf("\n***********************Testing next*********************\n");
  testNext(scan);

  // printf("\nTesting createTable\n");
  // createTable("stupid_table", schema);

  printf("\nTesting freeing record\n");
  freeRecord(record);
  printf("\nTesting freeing schema\n");
  freeSchema(schema);

  printf("\n*********************Testing shutDownRecordManager**********************\n");
  shutdownRecordManager();

  // printf("\nTesting createTable_Header and free_table_header\n");


  printf("\n\n*************** TEST FINISHED ***********************\n\n");
  return 0;
}
