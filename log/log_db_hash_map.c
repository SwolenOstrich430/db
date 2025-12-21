#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "../uthash-master/src/uthash.h"

char* TABLE_FILE_HEADER = "===========TABLE===========";
char* TABLE_DIR = "tables";

// value that will go in each table's file 
typedef struct {
    unsigned char* bytes;          
    size_t size;
} record_t;

// hash map we'll use for record lookups 
typedef struct { 
    int id;
    int address_start;
    UT_hash_handle hh;
} record_lookup_t;

/**
 * Table representation 
 * Includes: 
 *      - file_path:            path to file where records will be stored 
 *      - record_size:          size of bytes for each record stored in the file
 *      - record_address_map:   
 *                              keys: record Ids 
 *                              values: record's start address in the file
 */
typedef struct {
    char* file_path;
    size_t record_size;
    record_lookup_t* record_address_map;
} table_t;

void create_file(char* file_path, char* content) {
    FILE *file_ptr = fopen(file_path, "w");

    if (file_ptr == NULL) {
        int a = 0;
    }
    if (content != NULL) {
        fprintf(file_ptr, "%s", content);
    }

    fclose(file_ptr);
    return;
}

FILE* get_file_ptr(table_t* table, char* read_mode) {
    FILE *file_ptr = fopen(table->file_path, read_mode);

    if (!file_ptr) {
        perror("Error opening file\\n");
    }

    return file_ptr;
}

int get_record_address(table_t* table, int record_id) {
    record_lookup_t *record_lookup; 

    HASH_FIND_INT(
        table->record_address_map, 
        &record_id, 
        record_lookup
    );
    
    if (record_lookup == NULL || record_lookup->address_start < 0) {
        perror("Found record in lookup, but it has an invalid address.\\n");
        return -1;
    } 

    return record_lookup->address_start;
}
int MAX_DIR_LEN = 256;
int MAX_LEN_TABLE_NAME = 20;

// TODO: make this into a constant when you know what you're doing 
char* get_table_dir() {
    char* current_dir[MAX_DIR_LEN];
    getcwd(current_dir, sizeof(current_dir));

    return current_dir;
}

char* get_table_file_path(char* table_name) {
    size_t path_len = MAX_DIR_LEN + MAX_LEN_TABLE_NAME + 2;
    char* path = malloc(path_len);
    if (!path) return NULL;

    char current_dir[MAX_DIR_LEN]; 
    if (getcwd(current_dir, sizeof(current_dir)) == NULL) {
        free(path);
        return NULL;
    }

    snprintf(path, path_len, "%s/%s/%s", current_dir, TABLE_DIR, table_name);

    return path; 
}

table_t* init_table(char* table_name, size_t record_size) {
    record_size = record_size * sizeof(unsigned char);
    table_t *table = malloc(sizeof(table_t));

    char* table_file_path = get_table_file_path(table_name);
    create_file(table_file_path, TABLE_FILE_HEADER);

    table->file_path = table_file_path;
    table->record_size = record_size;
    table->record_address_map = NULL;

    return table;
}

void rpad(unsigned char* buffer, const char *str, size_t size) {
    strncpy(buffer, str, size);

    int str_len = strlen(buffer);
    if (str_len >= size) return;

    for (int i = str_len; i < size; i++) {
        buffer[i] = ' ';
    }
}

record_t* init_record(unsigned char* bytes, size_t size) {
    record_t *record = malloc(sizeof(record_t));
    unsigned char* content = (unsigned char*)malloc(size);
    rpad(content, bytes, size);

    record->bytes = content;
    record->size = size;

    return record;
}

/*
* todo: 
*     - no option for going back to history if program exits
* args: 
*     - table:      table where the new record will be added
*     - new_record: record_t that will be added to table  
* returns: int 
*     -  >0: insert record success 
*     -  -1: insert record failed 
*/ 
int set( 
    table_t *table,
    int id,
    record_t *new_record
) {
    int address_start = get_record_address(table, id);
    FILE* file_ptr = get_file_ptr(table, "r+");

    if (address_start < 0) {
        if (table->record_size != new_record->size) {
            printf(
                "New record size: %d is not equal to expected record size: %d.\\n", 
                table->record_size,
                new_record->size
            );
            return -1; 
        }

        if (file_ptr == NULL) {
            printf("Error opening file %s.\\n", table->file_path);
            return -1; 
        }

        fseek(file_ptr, 0L, SEEK_END);
        address_start = ftell(file_ptr);
    } else {
        fseek(file_ptr, address_start, SEEK_SET);
    }

    fprintf(file_ptr, "%s", new_record->bytes);
    fclose(file_ptr);
    
    record_lookup_t *new_lookup = malloc(sizeof(record_lookup_t));
    new_lookup->address_start = address_start;
    new_lookup->id = id;

    HASH_ADD_INT(
        table->record_address_map, 
        id, 
        new_lookup
    );

    return address_start;
}

unsigned char* get(table_t* table, int record_id) {
    int record_address = get_record_address(table, record_id);
    
    if (record_address == NULL) {
        return NULL;
    }

    FILE* file_ptr = get_file_ptr(table, "rb");

    int record_size = table->record_size * sizeof(unsigned char);
    unsigned char *record_content = (unsigned char *)malloc(record_size);

    if (!record_content) {
        perror("Error allocating memory");
        fclose(file_ptr);
        return NULL;
    }

    fseek(file_ptr, record_address, SEEK_SET);
    size_t bytes_read = fread(
        record_content, 1, record_size, file_ptr
    );

    if (bytes_read == NULL || bytes_read == 0) {
        fprintf(
            stderr, 
            "Error reading file, read %zu bytes out of %ld\n", 
            bytes_read, 
            record_size
        );

        free(record_content);
        fclose(file_ptr);
        return NULL;
    }

    fclose(file_ptr);

    return record_content;
}

int main() {
    size_t record_size = 10;
    int ids[4] = {1, 2, 3, 1};
    char *names[4] = {"me", "you", "them", "suppose"};

    table_t *table = init_table("users", record_size);
    char* found_name;
    record_t* new_record;

    size_t loop_size = sizeof(ids) / sizeof(ids[0]);
    for (int i = 0; i < loop_size; i++) {
        new_record = init_record(names[i], record_size);
        set(table, ids[i], new_record);
        found_name = get(table, ids[i]);

        if (strcmp(found_name, names[i]) != 0) {
            printf(
                "Id: %d | Found name: %s | Expected name: %s", 
                ids[i], found_name, names[i]
            );
        }
    }

    return 0;
}