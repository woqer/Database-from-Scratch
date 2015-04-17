#include <stdlib.h>
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

  printf("numAttr:\t\t%i\n", schema->numAttr);
  printf("keySize:\t\t%i\n", schema->keySize);

  int i;
  for (i = 0; i < schema->numAttr; i ++) {
    printf("attrNames[%i]:\t\t%s\n", i, (schema->attrNames)[i]);
    printf("dataTypes[%i]:\t\t%i\n", i, (schema->dataTypes)[i]);
    printf("typeLength[%i]:\t\t%i\n", i, (schema->typeLength)[i]);
  }
  for (i = 0; i < schema->keySize; i++) {
    printf("keyAttrs[%i]:\t\t%i\n", i, (schema->keyAttrs)[i]);
  }  

  return schema;
}

int testgetRecordSize(Schema *schema) {
  int size;
  size = getRecordSize(schema);
  printf("Size of schema:\t\t%i\n", size);
  return size;
}

void testCreateRecord(Schema *schema) {
  Record *record;
  createRecord(&record, schema);

  printf("record->id->page:\t\t%i\n", (record->id).page);
  printf("record->id->slot:\t\t%i\n", (record->id).slot);
  printf("record->data:\t\t%s\n", record->data);

  record->data = "1WilfridoVidana78.5true";

  printf("Data after writing something...\n");
  printf("record->data:\t\t%s\n", record->data);

}

void testGetAttr(Schema *schema){
  Record *record;
  createRecord(&record, schema);
  
  //not sure how to initialize this data, need to figure out!!!!!!!
  record->data = "0123456789WilfridoVidana78.5true";

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
		printf(value->v.intV);
		free(value);
	}
    break;
    case DT_STRING:
	{
		printf(&(value->v.stringV)); //??????
		free(value->v.stringV);
		free(value);
	}
    break;
    case DT_FLOAT:
	{
		printf(value->v.floatV);
		free(value);
	}
    break;
    case DT_BOOL:
	{
		printf(value->v.floatV);
		free(value);
	}
    break;
    default: break;
    }
  }
}

int main() {
  printf("\nTesting createSchema...\n");
  Schema *schema;
  schema = testCreateSchema();

  printf("\nTesting getRecordSize\n");
  int size;
  size = testgetRecordSize(schema);

  printf("\nTesting createRecord\n");
  testCreateRecord(schema);
  
  printf("\nTesting getAttr\n");
  testGetAttr(schema);
  
  return 0;
}
