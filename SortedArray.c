#include <stdio.h>
#include <string.h>
#include <stdint.h>
#include "SortedArray.h"
#include "config.h"


SortedArray * sa_createSortedArray(int maxValues){
	SortedArray * arr = malloc(sizeof(SortedArray));

	arr->arr = calloc(maxValues, sizeof(LSMData));
	arr->maxValues = maxValues;
	arr->currentValues = 0;
	arr->deleteCount = 0;
    return arr;
}

void sa_clear(SortedArray * arr){
    arr->currentValues = 0;
    arr->deleteCount = 0;
}

void sa_free(SortedArray * arr){
	free(arr->arr);
	free(arr);
}

static int binarySearch(LSMData * arr, int32_t key, int max){

    int start = 0;
    int end = max;
    while(1){

        if(end - start == 1){
            if(arr[start].key >= key){
                return start;
            }else{
                return end;
            }
        }else if(start == end){
            return start;
        }

        int pivot = (end+start)/2;
        int32_t pivotValue = arr[pivot].key;

        if (pivotValue > key){
            end = pivot - 1;
        }else if (pivotValue == key){
            return pivot;
        }else{
            start = pivot + 1;
        }

    }


}

LSMData * findInArray(SortedArray * r, int32_t key, int32_t * position){
	if(r->currentValues == 0){
		*position = 0;
		return NULL;
	}

	int pos = binarySearch(r->arr, key, r->currentValues -1);
	LSMData * res = &(r->arr[pos]);
	if(res->key == key){
        *position = pos;
	}else if(res->key < key){
        *position = pos+1;
        res = NULL;
	}else{
	    *position = pos;
        res = NULL;
	}

	//LSMData * result = binarySearch(r->arr, key, position, 0,
	//	r->currentValues-1, r->currentValues-1);
	return res;
}

LSMData * sa_searchForValue(SortedArray * r, int32_t key){
	int position;
	return findInArray(r, key, &position);
}

void putInArray(SortedArray * arr, int32_t key, int32_t value, int position){
	initHolder(&(arr->arr[position]), key, value);
	arr->currentValues++;
}

static void putDeleteInArray(SortedArray * arr, int32_t key,int position){
    initDeleteHolder(&(arr->arr[position]), key);
    arr->currentValues++;
    arr->deleteCount++;
}


bool sa_add(SortedArray * arr, int32_t key, int32_t value){
	int position;
	LSMData * current = findInArray(arr, key, &position);

	//printf("adding %i to position %i\n",key,position);

	if(current != NULL){

        if(current->metadata == DELETE){
            arr->deleteCount--;
        }

		current->value=value;
		current->metadata = NORMAL_VALUE;
		return true;
	}

	if(arr->currentValues == arr->maxValues){
        return false;
	}

	if(arr->currentValues != 0){
		memmove(&(arr->arr[position+1]), &(arr->arr[position]),
			sizeof(LSMData)
			* (arr->currentValues - position));

	}

	putInArray(arr, key, value, position);

    return true;
}

bool sa_delete(SortedArray * arr, int32_t key){
	int position;
	LSMData * current = findInArray(arr, key, &position);

	//printf("adding %i to position %i\n",key,position);

	if(current != NULL){

        if(current->metadata != DELETE){
            arr->deleteCount++;
        }
		current->metadata = DELETE;
		current->value = 0;
		return true;
	}

	if(arr->currentValues == arr->maxValues){
        return false;
	}

	//current = createHolder(key, value);

	if(arr->currentValues != 0){
		memmove(&(arr->arr[position+1]), &(arr->arr[position]),
			sizeof(LSMData)
			* (arr->currentValues - position));

	}

	putDeleteInArray(arr, key, position);
    return true;
}

void sa_printSummary(SortedArray * arr, const char * label){
	int i;
	for(i = 0; i < arr->currentValues; i++){
		if(arr->arr[i].metadata != DELETE){
            printf("%i:%i:%s ", arr->arr[i].key, arr->arr[i].value,label);
		}
	}

}

void appendToArray(SortedArray * arr, int32_t key, int32_t value){
	putInArray(arr, key, value, arr->currentValues);
}

bool sa_searchForKeyInFile(FILE * f, int32_t key, int32_t * value, ValueMetadata * metadata){
    //LSMData holder = {0,0,NORMAL_VALUE};
    LSMData holders[PAGE_SIZE];
    int numberRead;
    int i;
    while( (numberRead = fread(&holders, sizeof(LSMData), PAGE_SIZE, f)) >0 ){
        for(i = 0; i < numberRead; i++){
            LSMData * holder = &holders[i];
            if(holder->key == key ){
                *value = holder->value;
                *metadata = holder->metadata;
                return 1;
            }
            if(holder->key > key){
                //We have not found the instance
                return false;
            }
        }
    }

    return true;
}

void sa_printArrayFile(FILE * f, const char * label){
    LSMData holder = {0,0,NORMAL_VALUE};

    while( fread(&holder, sizeof(LSMData), 1, f) == 1){

        if(holder.metadata != DELETE){
            printf("%i:%i:%s ", holder.key, holder.value,label);
        }
    }
}

void sa_scanFileForRange(FILE * f, RangeResult* range, int32_t min, int32_t max){

    LSMData result = {0,0,NORMAL_VALUE};
    while(fread(&result, sizeof(LSMData), 1, f) != 0){
        int resultKey = result.key;
        if(resultKey >= max){
            return;
        }
        if( resultKey >= min){
            rng_add(range, &result);
        }
    }
}

void sa_splitArray(SortedArray * old, SortedArray * newArr){
    int added = 0;
    int deleteAdded = 0;
    int startIndex = old->currentValues/2;

    int i;
    for(i = startIndex; i < old->currentValues;i++){
        newArr->arr[added] = old->arr[i];
        added++;
        if(newArr->arr[i].metadata == DELETE){
            deleteAdded++;
        }
    }

    old->currentValues -= added;
    old->deleteCount -= deleteAdded;

    newArr->currentValues = added;
    newArr->deleteCount = deleteAdded;


}
