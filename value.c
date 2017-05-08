#include "value.h"
#include <stdint.h>

void initHolder(LSMData * h, int32_t key, int32_t value){
	h->key = key;
	h->value = value;
	h->metadata = NORMAL_VALUE;
}

void initDeleteHolder(LSMData * h, int32_t key){
	h->key = key;
	h->value = 0;
	h->metadata = DELETE;
}
