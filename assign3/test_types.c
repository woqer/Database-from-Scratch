#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "rm_serializer.c"

#ifndef bool
    typedef short bool;
    #define true 1
    #define false 0
#endif

#define TRUE true
#define FALSE false

#define ATTR_SIZE 32

typedef enum DataType {
    DT_INT = 0,
    DT_STRING = 1,
    DT_FLOAT = 2,
    DT_BOOL = 3
} DataType;

typedef struct Schema
{
    int numAttr;
    char **attrNames;
    DataType *dataTypes;
    int *typeLength;
    int *keyAttrs;
    int keySize;
} Schema;

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys) {

  Schema *schema = (Schema *)malloc(sizeof(Schema));

  schema->numAttr = numAttr;
  schema->attrNames = (char **)malloc(sizeof(char *) * numAttr);
  schema->dataTypes = (DataType *)malloc(sizeof(DataType) * numAttr);
  schema->typeLength = (int *)malloc(sizeof(int) * numAttr);
  schema->keyAttrs = (int *)malloc(sizeof(int) * keySize);
  schema->keySize = keySize;

  int i = 0;
  for (i = 0; i < numAttr; i++) {
    DataType x = dataTypes[i];
    (schema->dataTypes)[i] = x;
    (schema->typeLength)[i] = typeLength[i];
    (schema->attrNames)[i] = (char *)malloc(sizeof(char) * ATTR_SIZE);
    strncpy((schema->attrNames)[i], attrNames[i], ATTR_SIZE);
  }

  return schema;
}

void main() {
  int numAttr = 5;
  char **attrNames = (char *[]) {"id", "name", "last_name", "weight", "active"};
  DataType *dataTypes = (DataType []) { DT_INT, DT_STRING, DT_STRING, DT_FLOAT, DT_BOOL};
  int *typeLength = (int []) { sizeof(int)       ,
                      sizeof(char) * 20 ,
                      sizeof(char) * 32 ,
                      sizeof(float)     ,
                      sizeof(bool) };
  int keySize = 1;
  int *keys   = (int []) {0};

  Schema *schema = createSchema(numAttr, attrNames, dataTypes, typeLength, keySize, keys);


  printf("%s\n", serializeSchema(schema));
}

