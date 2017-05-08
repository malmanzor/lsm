#include <stdio.h>
#include "LSM.h"
#include "CommandParser.h"
#include "config.h"
int main(int argc, const char* argv[]){

    LSMConfiguration * config = cfg_readConfig();

    if(config == NULL){
        return 1;
    }

	LSMTree * tree = lsm_create(config);
	int retVal = prs_parseLSMCommands(tree, stdin);
	cfg_free(config);
	lsm_free(tree);
    return retVal;

}


