#include "storage_mgr.h"
#include "stdio.h"

// simple PageFile creation to test file size

void main (void) {
  // createPageFile ("filePage");
  FILE *fp = fopen("filePage","w");

  printf("ftell(fp)_first %ld\n", ftell(fp));
  fseek(fp, 0L, SEEK_SET);
  printf("ftell(fp)_after_fseek_0L %ld\n", ftell(fp));
  fseek(fp, 4096L, SEEK_SET);
  printf("ftell(fp)_after_fseek_4096L %ld\n", ftell(fp));

}