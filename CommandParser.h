#include "LSM.h"
#include <stdio.h>

/*
    Module for parsing LSM commands and invoking the appropriate functions
*/

//Parse commands from an input source and run the commands on the tree
int prs_parseLSMCommands(LSMTree * tree, FILE * input);
