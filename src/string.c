#include "server.h"
#include <math.h> /* isnan(), isinf() */
#include <stdio.h>


int getGenericCommand(client *c) {
	printf("get command!\n");
}


void getCommand(client *c) {
	getGenericCommand(c);
}

/* SET key value [NX] [XX] [EX <seconds>] [PX <milliseconds>] */
void setCommand(client *c) {
	printf("set command!\n");
}
