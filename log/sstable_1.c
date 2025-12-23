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
unsigned char* SSTABLE_LOOKUP_SEPERAOTOR = ":";
char* TABLE_DIR = "tables";

typedef struct {
    unsigned char* id;
    unsigned char* bytes;          
    size_t size;
} record_t;

// hash map we'll use for record lookups 
typedef struct { 
    unsigned char* id;
    int address_start;
    UT_hash_handle hh;
} id_address_lookup_t;

int cmp_address_lookup(
    const id_address_lookup_t* lookup_1, 
    const id_address_lookup_t* lookup_2
) {
    return strcmp(lookup_1->id, lookup_2->id);
}

int cmp_address_lookup_reverse(
    const id_address_lookup_t* lookup_1, 
    const id_address_lookup_t* lookup_2
) {
    return -cmp_address_lookup(lookup_1, lookup_2);
}

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


void rpad(unsigned char* buffer, const char *str, size_t size) {
    strncpy(buffer, str, size);

    int str_len = strlen(buffer);
    if (str_len >= size) return;

    for (int i = str_len; i < size; i++) {
        buffer[i] = ' ';
    }
}

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

char* create_sstable_file(char* table_name) {
    char* file_path = get_next_sstable_file_name(table_name);
    
    FILE* file_ptr = fopen(file_path, "w");
    fprintf(file_ptr, "%s", SSTABLE_FILE_HEADER);
    fclose(file_ptr);
    
    return file_path;
}

id_address_lookup_t* init_sstable_lookup(unsigned char* id, int address_start) {
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

record_t* init_record(
    unsigned char* id, 
    unsigned char* bytes, 
    size_t key_size, 
    size_t content_size
) {
    record_t* record = malloc(sizeof(record_t));
    record->id = id;
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

unsigned char* get_sstable_for_key(unsigned char* key) {

}

int get_record_address_from_sstable(unsigned char* key, sstable_t* sstable) {

}

int get_address_from_sstable(unsigned char* key, char* sstable_dir) {
    FILE* file_ptr = fopen(sstable_dir, "r");
    int address = get_record_address_from_sstable(key, file_ptr);
    fclose(file_ptr);
    return address;
}

void get_record_from_data_file(record_t* record, table_t* table, int record_address) {

}

int set(table_t* table, unsigned char* key, record_t* record) {
    // compare key to sstable lookup 
    int sstable_address = get_address_from_sstable(
        key, 
        table->sstable->sstable_dir
    );

    // if sstable lookup available 
    //      - look for val in sstable file 
    //      - if address in lookup
    //          - use address for update 
    // else 
    //      - look for table in data file 
    //      - use new space in file for write 
    // write to file 
}

// record_t* get(unsigned char* key) {
//     // 
// }

// append record to data file
// seek to end of data file 
// add record append to data file  
// return start of address 
int append_entry_to_data_file(table_t* table, record_t* record) {
    FILE *file_ptr = fopen(table->data_file_path, "rb");
    // TODO: handle file open errors 

    fseek(file_ptr, 0, SEEK_END);
    int start_address = ftell(file_ptr);
    fwrite(record->bytes, record->size, 1, file_ptr);
    fclose(file_ptr);
    
    return start_address;
}

id_address_lookup_t* parse_sstable_file_entry(
    id_address_lookup_t* lookup, 
    unsigned char* entry
) {
    char* id = strtok(entry, SSTABLE_LOOKUP_SEPERAOTOR);
    char* address = strtok(NULL, SSTABLE_LOOKUP_SEPERAOTOR);

    lookup->id = id;
    lookup->address_start = atoi(address);
    return lookup;
}

// add as record to address_lookup_map 
//     - id: record->id
//     - file_path: created file path from above 

int append_entry_to_sstable_file(
    table_t* table,
    char* sstable_file_path, 
    unsigned char* id, 
    int start_address
) {
    unsigned char* formatted_key; 
    unsigned char* formatted_address;
    rpad(formatted_key, id, table->key_size);
    rpad(formatted_address, (char*)start_address, sizeof(int));

    size_t entry_size = table->key_size + sizeof(int) + sizeof(SSTABLE_LOOKUP_SEPERAOTOR);
    
    unsigned char* file_entry[entry_size];
    snprintf(
        file_entry, 
        entry_size, 
        "%s%s%s", 
        formatted_key, 
        SSTABLE_LOOKUP_SEPERAOTOR, 
        formatted_address
    );

    FILE *file_ptr = fopen(sstable_file_path, "rb+");
    fseek(file_ptr, 0, SEEK_END);
    int file_length = ftell(file_ptr); 
    rewind(file_ptr);

    fseek(file_ptr, 0, strlen(SSTABLE_FILE_HEADER));

    unsigned char* curr_file_entry[entry_size];
    size_t bytes_read;
    id_address_lookup_t* lookup = malloc(sizeof(id_address_lookup_t));

    while ((bytes_read = fread(curr_file_entry, 1, entry_size, file_ptr)) == entry_size) {
        lookup = parse_sstable_file_entry(lookup, curr_file_entry);
        
        if (strcmp(lookup->id, id) < 0) {
            break;
        }
    }

    int final_index = ftell(file_ptr); 
    if (final_index < file_length) {
        // shift file in place 
    } else {
        fprintf(file_ptr, "%s", file_entry);
    }

    free(lookup);
    fclose(file_ptr);
}

int append_entry(table_t* table, record_t* record) {
    int start_address = append_entry_to_data_file(table, record);
    char* sstable_file = create_sstable_file(table->name);
    append_entry_to_sstable_file(table, sstable_file, record->id, start_address);
    // add as record to address_lookup_map 
    //     - id: record->id
    //     - file_path: created file path from above 

    FILE* file_ptr = fopen(table->data_file_path, "rb+");
    fwrite(record->bytes, record->size, 1, file_ptr);
    fclose(file_ptr);
}

int main() {
    size_t key_size = 10;
    size_t record_size = 10;

    char* table_name = "test_table";
    sstable_t* sstable = init_sstable(4096, "test_table");
    table_t* table = init_table("test_table", key_size, record_size, sstable);
    record_t* record = init_record("key1", "value1", key_size, record_size); 

    /*
    * - set 
    *   - while an available key exists in sstable_lookup_map 
    *       - find key: 
    *           - closest key < current_key that hasn't been checked yet 
    *           - if no key exists, use first key in sstable_lookup_map
    *       - if key exists  
    *           - check sstable file for key  
    *       - if record in sstable file exists 
    *           - update record at address  
    *           - return  
    *   - if record found  
    *       - add record to end of table file  
    *       - find the key closest to the current key in sstable_lookup_map
    *       - loop through sstable in file 
    *          - find sorted order for key in that file 
    *          - insert record at address
    */
    HASH_SORT(table->sstable->address_lookup, cmp_address_lookup_reverse);
    id_address_lookup_t *curr_lu;
    unsigned char* closest_id = NULL;
    
    for(
        curr_lu=table->sstable->address_lookup; 
        curr_lu != NULL; 
        curr_lu=(id_address_lookup_t*)(curr_lu->hh.next)
    ) {
        if (strcmp(curr_lu->id, record->id) > 0) {
            continue;
        }

        // look for key in sstable file  
        int record_address = get_record_address_from_sstable(
            record->id,
            table->sstable
        );

        if (record_address < 0) {
            continue;
        }

        record_t* found_record;
        get_record_from_data_file(found_record, table, record_address);
        closest_id = curr_lu->id;
    }

    if (closest_id == NULL) { 
        append_entry(table, record);
        // add that entry to the sstable map 
    }
}

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