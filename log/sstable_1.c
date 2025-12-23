#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>  
#include <dirent.h>
#include <string.h>
#include "../uthash-master/src/uthash.h"

char* TABLE_FILE_HEADER = "===========TABLE===========";
char* SSTABLE_FILE_HEADER = "===========SSTABLE==========";
char* TABLE_DIR = "tables";

typedef struct {
    unsigned char* bytes;          
    size_t size;
} record_t;

// hash map we'll use for record lookups 
typedef struct { 
    int id;
    int address_start;
    UT_hash_handle hh;
} id_address_lookup_t;

typedef struct {
    int max_file_size;
    char* sstable_dir;
    id_address_lookup_t* address_lookup;
} sstable_t;

typedef struct {
    char* name;
    size_t key_size;
    size_t record_size;
    char* data_file_path;
    sstable_t* sstable;
} table_t;


// unsigned char* rpad(unsigned char* str, size_t size) {

// } 

int MAX_DIR_LEN = 256;
int MAX_LEN_TABLE_NAME = 20;
char* SSTABLE_DIR = "sstable";

char* get_table_file_path(char* table_name) {
    size_t path_len = MAX_DIR_LEN + MAX_LEN_TABLE_NAME + 2;
    char* path = malloc(path_len);
    if (!path) return NULL;

    char current_dir[MAX_DIR_LEN]; 
    if (getcwd(current_dir, sizeof(current_dir)) == NULL) {
        free(path);
        return NULL;
    }

    snprintf(
        path, 
        path_len, 
        "%s/%s/%s.table", 
        current_dir, 
        TABLE_DIR, 
        table_name
    );

    return path;
}

char* get_data_file_dir(char* table_name) {
    size_t dir_len = MAX_DIR_LEN + MAX_LEN_TABLE_NAME + 2;
    char* current_dir = malloc(dir_len); 
    
    if (getcwd(current_dir, dir_len) == NULL) {
        free(current_dir);
        return NULL;
    }

    struct stat st;
    sprintf(current_dir, "%s/%s", current_dir, TABLE_DIR);
    if (!(stat(current_dir, &st) == 0 && S_ISDIR(st.st_mode))) {
        mkdir(current_dir, 777);
    }

    sprintf(current_dir, "%s/%s", current_dir, table_name);
    return current_dir;
}

char* create_data_file_dir(char* table_name) {
    char* dir = get_data_file_dir(table_name);
    
    struct stat st;
    if (!(stat(dir, &st) == 0 && S_ISDIR(st.st_mode))) {
        mkdir(dir, 777);
    }

    return dir;
}

char* get_sstable_dir(char* table_name) {
    char* dir = create_data_file_dir(table_name);
    sprintf(dir, "%s/%s", dir, SSTABLE_DIR);

    return dir;
}

char* create_sstable_dir(char* table_name) {
    char* dir = get_sstable_dir(table_name);

    struct stat st;
    if (!(stat(dir, &st) == 0 && S_ISDIR(st.st_mode))) {
        mkdir(dir, 777);
    }

    return dir;
}

int get_dir_file_count(const char *path) {
    DIR *dir;
    struct dirent *entry;
    int num_files = 0;

    dir = opendir(path);
    if (dir == NULL) {
        perror("opendir");
        return -1; 
    }

    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_type == DT_REG) { 
            num_files++;
        }
    }

    closedir(dir);
    return num_files;
}

char* get_sstable_file_name(char* table_name, int file_num) {
    char* dir = create_sstable_dir(table_name);
    char* file_path = malloc(MAX_DIR_LEN + MAX_LEN_TABLE_NAME + 10);
    
    sprintf(
        file_path, 
        "%s/%s_%d.sstable", 
        dir, 
        table_name, 
        file_num
    );

    free(dir);
    return file_path;
}

char* get_next_sstable_file_name(char* table_name) {
    char* dir = create_sstable_dir(table_name);
    int num_files = get_dir_file_count(dir);
    
    if (num_files <= 0) {
        num_files = 0;    
    }else { 
        num_files++;
    }

    free(dir);
    return get_sstable_file_name(table_name, num_files);
}

void create_sstable_file(char* table_name) {
    char* file_path = get_next_sstable_file_name(table_name);
    FILE* file_ptr = fopen(file_path, "w");
    fprintf(file_ptr, "%s", SSTABLE_FILE_HEADER);
    fclose(file_ptr);
}

id_address_lookup_t* init_sstable_lookup(int id, int address_start) {
    id_address_lookup_t* address_lookup = malloc(sizeof(id_address_lookup_t));
    address_lookup->id = id;
    address_lookup->address_start = address_start;
    return address_lookup;
}

sstable_t* init_sstable(
    long max_file_size,
    char* table_name
) {
    sstable_t* sstable = malloc(sizeof(sstable_t));
    
    sstable->sstable_dir = create_sstable_dir(table_name);
    create_sstable_file(table_name);
    
    sstable->max_file_size = max_file_size; 
    sstable->address_lookup = NULL;
    return sstable;
}

record_t* init_record(unsigned char* key, unsigned char* bytes) {
    record_t* record = malloc(sizeof(record_t));
    record->bytes = bytes;
    record->size = sizeof(bytes);
    return record;
}

table_t* init_table(
    char* table_name, 
    size_t key_size,
    size_t record_size,
    sstable_t* sstable
) {
    table_t* table = malloc(sizeof(table_t));
    table->name = table_name;
    table->key_size = key_size;
    table->record_size = record_size;
    table->data_file_path = get_table_file_path(table_name);
    table->sstable = sstable;

    return table;
}

// unsigned char* get_sstable_for_key(unsigned char* key) {

// }

// int get_record_address_from_sstable(unsigned char* key, FILE* file_ptr) {

// }

// void get_record_from_data_file(unsigned char* buffer, int record_address) {

// }

// int set(unsigned char* key, record_t* record) {
    // compare key to sstable lookup 
    // if sstable lookup available 
    //      - look for val in sstable file 
    //      - if address in lookup
    //          - use address for update 
    // else 
    //      - look for table in data file 
    //      - use new space in file for write 
    // write to file 
// }

// record_t* get(unsigned char* key) {
//     // 
// }

/*
* ===== TYPE DEFS
* init_table
*   - name
*   - key_size (num bytes each key should be)
*   - record_size (num bytes each record should be)
*   - table_file_path (where data will be stored)
*   - sstable_t sstable
* 
* init_record
*   - key (id)
*   - bytes (unsigned char*: data to be stored in related file)
* 
* * init_sstable 
*   - sstable_size (4kb)
*   - sstable_paths (where each sstable will be stored)
*   - sstable_lookup_map (sorted map of key-sstable_path)
* 
* init_sstable_lookup
*   - key (id)
*   - sstable_address
* 
* 
* ===== DATA HANDLING
* unsigned char* get_sstable_for_key(unsigned char* key)
*   - returns path to the sstable if key exists
*   - returns NULL if not exists 
* 
* int get_record_address_from_sstable(unsigned char* key, FILE* file_ptr)
*   - returns int address of corresponding record in table file if key exists
*   - returns -1 if key doens't exist 
* 
* get_record_from_data_file(unsigned char* buffer, int record_address)
*   - sets buffer to content at record address if exists 
*   - throws error if does not exist
* 
* unsigned char* rpad(unsigned char* str, size_t size)
* 
* 
* ===== FILE HANDLING
* get_table_file_path -> log/data/<table_name>/<table_name>.table
* get_sstable_file -> log/data/<table_name>/sstable/<new_file_name>
* get_sstable_dir -> log/data/<table_name>/sstable/
* get_data_dir    -> log/data/<table_name>
*/

int main() {
    char* table_name = "test_table";
    sstable_t* sstable = init_sstable(4096, "test_table");
    table_t* table = init_table("test_table", 10, 10, sstable);
    
    
}