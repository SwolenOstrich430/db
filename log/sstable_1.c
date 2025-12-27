#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>  
#include <dirent.h>
#include <string.h>
#include <libgen.h>
#include <uuid/uuid.h>
#include <ctype.h>
#include "../uthash-master/src/uthash.h"

unsigned char* SSTABLE_LOOKUP_SEPERATOR = ":";
char* TABLE_DIR = "tables";

typedef struct {
    unsigned char* id;
    unsigned char *file_name;
    UT_hash_handle hh;
} sstable_lookup_t;
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
    sstable_lookup_t* address_lookup;
} sstable_t;

typedef struct {
    char* name;
    size_t key_size;
    size_t record_size;
    char* data_file_path;
    sstable_t* sstable;
} table_t;


void rpad(char* buffer, char *str, size_t size) {
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

char* create_data_file_path(char* table_name) {
    char* path = get_table_file_path(table_name);
    struct stat st;

    if (stat(path, &st) != 0) {
        FILE* file_ptr = fopen(path, "w");
        fprintf(file_ptr, "%s", "");
        fclose(file_ptr);
    }

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
    if (file_num > 0) file_num--;

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
    fprintf(file_ptr, "%s", "");
    fclose(file_ptr);
    
    return file_path;
}

char* get_latest_sstable_file(char* table_name) {
    char* dir = get_sstable_dir(table_name);
    int num_files = get_dir_file_count(dir);
    free(dir);

    // TODO: make this into a constant 
    char* latest_file = malloc(MAX_DIR_LEN + MAX_LEN_TABLE_NAME + 10);

    if (num_files <= 0) {
        latest_file = create_sstable_file(table_name);
    } else {
        latest_file = get_sstable_file_name(table_name, num_files);
    }

    return latest_file;
}

// opa_status -p 2206
// sscv -t opa2206  
// can you reset the plc for that station 

int get_file_size(char* file_path) {
    struct stat st;
    if (stat(file_path, &st) != 0) {
        // TODO: make this into a helper
        perror(
            "Unable to get file size"
            // sprintf("Unable to get file size from: %s\n", file_path)
        );
        return -1;
    }

    return st.st_size;
}

char* get_current_sstable_file(char* table_name, int max_file_size) {
    create_sstable_dir(table_name);
    char* latest_sstable = get_latest_sstable_file(table_name);
    int latest_size = get_file_size(latest_sstable);

    if (latest_size >= max_file_size) {
        create_sstable_file(table_name);
    }

    return latest_sstable;
}

id_address_lookup_t* init_address_lookup(unsigned char* id, int address_start) {
    id_address_lookup_t* address_lookup = malloc(sizeof(id_address_lookup_t));
    address_lookup->id = id;
    address_lookup->address_start = address_start;
    return address_lookup;
}

id_address_lookup_t* init_id_adddress_lookup(
    unsigned char* id, 
    int address_start
) {
    id_address_lookup_t* lookup = malloc(sizeof(id_address_lookup_t));
    lookup->id = id;
    lookup->address_start = address_start;
    return lookup;
}

void parse_sstable_file_entry(
    id_address_lookup_t* lookup, 
    unsigned char* entry,
    int key_size
) {
    unsigned char* id = malloc(key_size);
    rpad(id, strtok(entry, SSTABLE_LOOKUP_SEPERATOR), key_size);
    lookup->id = id;
    lookup->address_start = atoi(strtok(NULL, SSTABLE_LOOKUP_SEPERATOR));
    
    return;
}

void set_sstable_lookup_map(
    sstable_t* sstable, 
    char* table_name,
    int key_size
) {
    create_sstable_dir(table_name);
    struct dirent *entry;
    int num_files = 0;
    DIR *dir = opendir(sstable->sstable_dir);

    if (dir == NULL) {
        perror("Unable to open sstable directory for lookup map creation");
    }
    
    size_t entry_size = key_size + sizeof(int) + sizeof(SSTABLE_LOOKUP_SEPERATOR);
    unsigned char* curr_lookup_entry = malloc(entry_size);
    char sstable_file_path[MAX_DIR_LEN + MAX_LEN_TABLE_NAME + 10];
    size_t bytes_read;
    FILE* file_ptr;

    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_type == DT_REG) { 
            sprintf(
                sstable_file_path, 
                "%s/%s", 
                sstable->sstable_dir, 
                entry->d_name
            );              
            file_ptr = fopen(sstable_file_path, "rb");

            if ((bytes_read = fread(curr_lookup_entry, 1, entry_size, file_ptr)) == entry_size) {
                sstable_lookup_t* sstable_lookup = malloc(sizeof(sstable_lookup_t));
                id_address_lookup_t* address_lookup = malloc(sizeof(id_address_lookup_t));
                parse_sstable_file_entry(address_lookup, curr_lookup_entry, key_size);

                sstable_lookup->id = malloc(key_size);
                sstable_lookup->id = strcpy(sstable_lookup->id, address_lookup->id);
                sstable_lookup->file_name = strdup(entry->d_name);
                unsigned char* id = sstable_lookup->id;

                HASH_ADD(
                    hh,
                    sstable->address_lookup, 
                    id, 
                    key_size, 
                    sstable_lookup
                );
            }
        }
    }

    free(curr_lookup_entry);
    closedir(dir);

    return;
}

sstable_lookup_t* init_sstable_lookup_t(
    unsigned char* id, 
    char* file_name
) {
    sstable_lookup_t* lookup = malloc(sizeof(sstable_lookup_t));
    lookup->id = id;
    lookup->file_name = basename(file_name);
    return lookup;
}

sstable_t* init_sstable(
    long max_file_size,
    char* table_name,
    int key_size
) {
    get_current_sstable_file(table_name, max_file_size);

    sstable_t* sstable = malloc(sizeof(sstable_t));  
    sstable->sstable_dir = get_sstable_dir(table_name);      
    sstable->max_file_size = max_file_size; 
    sstable->address_lookup = NULL;

    set_sstable_lookup_map(sstable, table_name, key_size);

    return sstable;
}

record_t* init_record(
    unsigned char* id, 
    unsigned char* bytes, 
    size_t key_size, 
    size_t content_size
) {
    record_t* record = malloc(sizeof(record_t));
    record->id= malloc(key_size);
    rpad(record->id, id, key_size);

    record->bytes = malloc(content_size);
    rpad(record->bytes, bytes, content_size);
    record->size = content_size;

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
    table->data_file_path = create_data_file_path(table_name);
    table->sstable = sstable;

    return table;
}

unsigned char* get_sstable_for_key(unsigned char* key) {

}

int get_record_address_from_sstable(unsigned char* id, FILE* file_ptr, int key_size) {
    int bytes_read;
    int address_start = -1;
    size_t entry_size = key_size + sizeof(int) + sizeof(SSTABLE_LOOKUP_SEPERATOR);
    unsigned char* curr_lookup_entry = malloc(entry_size);
    id_address_lookup_t* lookup = malloc(sizeof(id_address_lookup_t));

    while ((bytes_read = fread(curr_lookup_entry, 1, entry_size, file_ptr)) == entry_size) {
        parse_sstable_file_entry(lookup, curr_lookup_entry, key_size);

        if (strcmp(lookup->id, id) < 0) {
            break;
        }

        if (strcmp(lookup->id, id) == 0) {
            address_start = lookup->address_start;
            break;
        }
    }
    
    free(lookup);
    free(curr_lookup_entry);

    return address_start;
}

int get_address_from_sstable(unsigned char* key, unsigned char* sstable_file_path, int key_size) {
    FILE* file_ptr = fopen(sstable_file_path, "rb");
    int address = get_record_address_from_sstable(key, file_ptr, key_size);
    fclose(file_ptr);

    return address;
}

void get_record_from_data_file(record_t* record, table_t* table, int record_address) {
    FILE* file_ptr = fopen(table->data_file_path, "rb");
    fseek(file_ptr, record_address, SEEK_SET);

    size_t bytes_read = fread(
        record->bytes, 
        1, 
        table->record_size * sizeof(unsigned char), 
        file_ptr
    );

    if (bytes_read == NULL || bytes_read == 0) {
        fprintf(
            stderr, 
            "Error reading file, read %zu bytes out of %ld\n", 
            bytes_read, 
            table->record_size * sizeof(unsigned char)
        );
    }

    fclose(file_ptr);
    return;
}

// append record to data file
// seek to end of data file 
// add record append to data file  
// return start of address 
int append_entry_to_data_file(table_t* table, record_t* record, int curr_address) {
    // TODO: handle file open errors 
    FILE *file_ptr = fopen(table->data_file_path, "ab+");

    if (curr_address >= 0) {
        fseek(file_ptr, curr_address, SEEK_SET);
    } else {
        curr_address = ftell(file_ptr);
    }

    fwrite(record->bytes, record->size, 1, file_ptr);
    fclose(file_ptr);
    
    return curr_address;
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
    unsigned char* formatted_key = malloc(table->key_size); 
    unsigned char* formatted_address = malloc(sizeof(long));
    unsigned char* address_as_str = malloc(sizeof(long));
    rpad(formatted_key, id, table->key_size);
    snprintf(address_as_str, sizeof(long), "%d", start_address);
    rpad(formatted_address, address_as_str, sizeof(long));
    size_t entry_size = table->key_size + sizeof(int) + sizeof(SSTABLE_LOOKUP_SEPERATOR);
    unsigned char* file_entry = malloc(entry_size);

    snprintf(
        file_entry, 
        entry_size, 
        "%s%s%s", 
        formatted_key, 
        SSTABLE_LOOKUP_SEPERATOR, 
        formatted_address
    );

    FILE *file_ptr = fopen(sstable_file_path, "ab+");
    fseek(file_ptr, 0, SEEK_END);
    int file_length = ftell(file_ptr); 
    rewind(file_ptr);

    unsigned char* curr_file_entry = malloc(entry_size);
    size_t bytes_read;
    size_t total_bytes_read = 0;
    id_address_lookup_t* lookup = malloc(sizeof(id_address_lookup_t));

    while ((bytes_read = fread(curr_file_entry, 1, entry_size, file_ptr)) == entry_size) {
        parse_sstable_file_entry(lookup, curr_file_entry, table->key_size);
        free(curr_file_entry);

        total_bytes_read += bytes_read;

        if (strcmp(lookup->id, id) < 0) {
            break;
        }

        curr_file_entry = malloc(entry_size);
    }

    int final_index = ftell(file_ptr); 
    fwrite(file_entry, entry_size, 1, file_ptr);
    fclose(file_ptr);

    free(lookup);
    free(formatted_key);
    free(formatted_address);
    free(file_entry);
    free(address_as_str);

    return final_index;
}

int set_sstable_lookup_entry(
    sstable_t* sstable,
    unsigned char* id, 
    char* sstable_file_path, 
    int sstable_address
) {
    id_address_lookup_t* new_lookup = init_sstable_lookup_t(
        id, 
        sstable_file_path
    );

    HASH_ADD(
        hh,
        sstable->address_lookup, 
        id, 
        sizeof(id),
        new_lookup
    );
}

id_address_lookup_t* get_first_id_address_lookup_t(table_t* table, char* sstable_file_path) {
    FILE* file_ptr = fopen(sstable_file_path, "rb");
    // TODO: duplication, move to helper
    size_t entry_size = table->key_size + sizeof(int) + sizeof(SSTABLE_LOOKUP_SEPERATOR);
    unsigned char* curr_lookup_entry = malloc(entry_size);
    id_address_lookup_t* address_lookup = malloc(sizeof(id_address_lookup_t));

    if (fread(curr_lookup_entry, 1, entry_size, file_ptr) == entry_size) {
        parse_sstable_file_entry(address_lookup, curr_lookup_entry, entry_size);
    } else {
        free(address_lookup);
        address_lookup = NULL;
    }

    free(curr_lookup_entry);
    fclose(file_ptr);

    return address_lookup;
}

int append_entry(
    table_t* table, 
    record_t* record, 
    int curr_address, 
    char* sstable_file_path
) {
    if (sstable_file_path == NULL || strlen(sstable_file_path) == 0) {
        sstable_file_path = get_current_sstable_file(
            table->name, 
            table->sstable->max_file_size
        );
    }

    int create_file = 0;
    id_address_lookup_t* first_entry;
    struct stat st;
    if (stat(sstable_file_path, &st) == 0) {
        first_entry = get_first_id_address_lookup_t(
            table, sstable_file_path
        );
        create_file = strcmp(record->id, first_entry->id) < 0;
    } else {
        create_file = 1;
    }

    if (create_file) {
        sstable_file_path = create_sstable_file(table->name);
    }

    int start_address = append_entry_to_data_file(table, record, curr_address);
    if (start_address < 0) {
        return -1;
    }

    int sstable_address = append_entry_to_sstable_file(
        table, 
        sstable_file_path,
        record->id, 
        start_address
    );

    if (sstable_address == 0) {
        set_sstable_lookup_entry(
            table->sstable, 
            record->id, 
            sstable_file_path, 
            sstable_address
        );
    }

    free(sstable_file_path);
    return start_address;
}

record_t* get(table_t* table, unsigned char* key) {
    unsigned char* formatted_key = malloc(table->key_size); 
    rpad(formatted_key, key, table->key_size);

    HASH_SORT(table->sstable->address_lookup, -cmp_address_lookup);
    sstable_lookup_t *curr_lu, *tmp;
    unsigned char* file_path[sizeof(table->sstable->sstable_dir) + MAX_LEN_TABLE_NAME + 10];
    int record_address = -1;

    HASH_ITER(hh, table->sstable->address_lookup, curr_lu, tmp) {
        if (strcmp(curr_lu->id, formatted_key) > 0) {
            continue;
        }

        snprintf(
            file_path, 
            sizeof(file_path), 
            "%s/%s", 
            table->sstable->sstable_dir, 
            curr_lu->file_name
        );
        // look for key in sstable file  
        int temp_record_address = get_address_from_sstable(
            formatted_key,
            file_path,
            table->key_size
        );

        if (temp_record_address < 0) {
            continue;
        } 

        record_address = temp_record_address;
        break;
    }

    free(formatted_key);

    if (record_address < 0) {
        return NULL;
    }

    record_t* record = init_record(
        key, 
        malloc(table->record_size), 
        table->key_size, 
        table->record_size
    );
    get_record_from_data_file(record, table, record_address);

    return record;
}

int set(table_t* table, record_t* record) {
    HASH_SORT(table->sstable->address_lookup, cmp_address_lookup_reverse);
    sstable_lookup_t *curr_lu, *tmp;
    size_t file_path_size = 200; // sizeof(table->sstable->sstable_dir) + MAX_LEN_TABLE_NAME + 10;
    unsigned char* file_path;
    int record_address = -1;

    HASH_ITER(hh, table->sstable->address_lookup, curr_lu, tmp) {
        file_path = malloc(file_path_size);
        if (strcmp(curr_lu->id, record->id) > 0) {
            continue;
        }

        snprintf(
            file_path, 
            file_path_size, 
            "%s/%s", 
            table->sstable->sstable_dir, 
            curr_lu->file_name
        );
        // look for key in sstable file  
        int temp_record_address = get_address_from_sstable(
            record->id, file_path, table->key_size
        );

        record_address = temp_record_address;    
        break;
    }

    // TODO: handle failure for setting record 
    append_entry(table, record, record_address, file_path);
}

char *strstrip(char *s) {
    size_t size;
    char *end;
    size = strlen(s);

    if (!size) {
        return s;
    }

    end = s + size - 1;
    while (end >= s && isspace(*end)) {
        end--;
    }

    *(end + 1) = '\0';

    while (*s && isspace(*s)) {
        s++;
    }
    return s;
}


#define DICTIONARY "/usr/share/dict/words"
#define SIZE 256 // Buffer size for individual words

int main() {
    size_t key_size = 37;
    size_t record_size = 250;
    size_t num_keys = 10;
    int num_entries = 100;
    int entries_set = 0;
    
    uuid_t binuuid;
    char *uuid_str = malloc(key_size); 
    record_t* record;

    unsigned char* key1 = "key1";
    unsigned char* key2 = "key2";
    unsigned char* value2 = "value2";
    char* table_name = "test_table";

    sstable_t* sstable = init_sstable(4096, "test_table", key_size);
    table_t* table = init_table("test_table", key_size, record_size, sstable);
    // record_t* record = init_record(key1, "value1", key_size, record_size); 

    FILE *dict;
    char word[SIZE];

    // Open the dictionary file in read mode
    dict = fopen(DICTIONARY, "r");
    if (dict == NULL) {
        fprintf(stderr, "Unable to open %s\\n", DICTIONARY);
        exit(1);
    }

    while (fgets(word, SIZE, dict) != NULL) {
        uuid_generate_random(binuuid);
        uuid_unparse(binuuid, uuid_str);
        set(table, init_record(
            (unsigned char*)uuid_str, 
            (unsigned char*)word, 
            key_size, 
            record_size
        ));

        record = get(table, (unsigned char*)uuid_str);
        printf("Key: %s, Value: %s\n", record->id, record->bytes);  

        strstrip(word);
        strstrip((char*)record->bytes);
        if (strcmp(record->bytes, word) != 0) {
            printf("Mismatch on key: %s, expected: %s, got: %s\n", uuid_str, word, record->bytes);
        }

        strstrip(uuid_str);
        strstrip((char*)record->id);
        if (strcmp(record->id, (unsigned char*)uuid_str) != 0) {
            printf("Mismatch on id: %s, expected: %s, got: %s\n", uuid_str, uuid_str, record->id);
        }


        entries_set++;
    }

    // TODO: add lifecycle methods 
    free(record);
    free(uuid_str);
    free(table);
    free(sstable);
    // Close the file
    fclose(dict);

    // set(table, record); 
    // record_t* fetched_record = get(table, key1);  
    // record_t* record_2 = init_record(key1, value2, key_size, record_size);
    // set(table, record_2); 
    // record_t* fetched_record_2 = get(table, key1);

    // int i = 0;
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