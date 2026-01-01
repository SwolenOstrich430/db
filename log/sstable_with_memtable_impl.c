/**
 * TODO: fix: casting unsigned char to char for str-util methods 
 * TODO: fix: mkdir_force memory allocation
 */

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

char* TABLE_DIR_NAME = "tables";
char* TABLE_FILE_EXTENSION = ".table";
char* SSTABLE_DIR_NAME = "sstable";
char* SSTABLE_LOOKUP_SEPERATOR = ":";
char* MEMTABLE_DIR = "memtable";
char* MEMTABLE_FILE_EXTENSION = "memtable";
int DEFAULT_PERMISSIONS = 0775;
size_t MAX_PATH_SIZE = 256;

typedef struct {
    unsigned char* sstable_file_name;
    int block_number;
} sstable_address_t;

typedef struct {
    unsigned char* id;
    char* file_name;
    int block_number;
    UT_hash_handle hh;
} sstable_lookup_t;

typedef struct {
    unsigned char* id;
    int address;
    UT_hash_handle hh;
} id_address_lookup_t;

typedef struct {
    char* table_name;
    size_t id_size;
    size_t block_size;
    size_t max_size;
    id_address_lookup_t* memtable;
    sstable_lookup_t* lookup_map;
    char* sstable_dir_path;
    char* memtable_file_path;
} sstable_t;

// ========== FILE HANDLING 
char* get_base_dir(char* table_name) {
    char* curr_wd = malloc(MAX_PATH_SIZE);
    getcwd(curr_wd, MAX_PATH_SIZE);

    char* base_dir = malloc(MAX_PATH_SIZE);

    snprintf(
        base_dir,
        MAX_PATH_SIZE,
        "%s/%s/%s",
        curr_wd,
        TABLE_DIR_NAME,
        table_name
    );

    free(curr_wd);
    return base_dir;
}

char* get_sstable_dir(char* table_name) {
    char* base_dir = get_base_dir(table_name);
    char* sstable_dir = malloc(MAX_PATH_SIZE);
    
    snprintf(
        sstable_dir,
        MAX_PATH_SIZE, 
        "%s/%s", 
        base_dir, 
        SSTABLE_DIR_NAME
    ); 

    return sstable_dir;
}

void mkdir_force(char* dir_path) {
    char* dir_path_copy = malloc(strlen(dir_path));
    strcpy(dir_path_copy, dir_path);

    char* full_dir = malloc(MAX_PATH_SIZE);
    char* curr_dir = strtok(dir_path_copy, "/");
    snprintf(full_dir, MAX_PATH_SIZE, "/%s", curr_dir);
    
    struct stat st;
    if (stat(full_dir, &st) != 0) {
        mkdir(full_dir, DEFAULT_PERMISSIONS);
    }

    while ((curr_dir = strtok(NULL, "/")) != NULL) {
        snprintf(full_dir, MAX_PATH_SIZE, "%s/%s", full_dir, curr_dir);
        if (stat(full_dir, &st) != 0) {
            mkdir(full_dir, DEFAULT_PERMISSIONS);
        }
    }

    free(dir_path_copy);
    free(full_dir);
    return;
}

int create_dir(char* dir_path) {
    mkdir_force(dir_path);
    struct stat st;

    int folder_created = (stat(dir_path, &st) == 0) ? 1 : -1;
    if (folder_created != 1) {
        perror("error: could not create base dir");
    }

    return folder_created; 
}

int create_file(char* file_path) {
    struct stat st;
    if (stat(file_path, &st) != 0) {
        FILE *fptr;
        fptr = fopen(file_path, "w");

        fprintf(fptr, "");
        fclose(fptr);
    }

    int file_created = (stat(file_path, &st) == 0) ? 1 : -1;
    if (file_created != 1) {
        perror("error: could not create base dir");
    }

    return file_created; 
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

char* create_base_dir(char* table_name) {
    char* base_dir = get_base_dir(table_name);
    int dir_created = create_dir(base_dir);
    return base_dir;
}

char* get_table_file(char* table_name) {
    char* base_dir = get_base_dir(table_name);
    char* table_dir = malloc(MAX_PATH_SIZE);

    snprintf(
        table_dir, 
        MAX_PATH_SIZE, 
        "%s/%s.%s", 
        base_dir, 
        table_name,
        TABLE_FILE_EXTENSION
    );

    return table_dir;
}

char* create_table_file(char* table_name) {
    create_base_dir(table_name);
    char* table_file = get_table_file(table_name);
    int file_created = create_file(table_file);
    
    return table_file;
}

char* create_sstable_dir(char* table_name) {
    create_base_dir(table_name);
    char* sstable_dir = get_sstable_dir(table_name);
    create_dir(sstable_dir);

    return sstable_dir;
}

char* get_sstable_file_name(char* table_name, int file_num) {
    char* dir = create_sstable_dir(table_name);
    char* file_path = malloc(MAX_PATH_SIZE);
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
    char* latest_file = malloc(MAX_PATH_SIZE);

    if (num_files <= 0) {
        latest_file = create_sstable_file(table_name);
    } else {
        latest_file = get_sstable_file_name(table_name, num_files);
    }

    return latest_file;
}

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

char* get_memtable_dir(char* table_name) {
    char* base_dir = create_base_dir(table_name);
    char* memtable_dir = malloc(MAX_PATH_SIZE);

    snprintf(
        memtable_dir,
        MAX_PATH_SIZE,
        "%s/%s",
        base_dir,
        MEMTABLE_DIR
    );

    free(base_dir);
    return memtable_dir;
}

char* create_memtable_dir(char* table_name) {
    char* memtable_dir = get_memtable_dir(table_name);
    create_dir(memtable_dir);

    return memtable_dir;
}

char* get_memtable_file(char* table_name) {
    char* memtable_dir = create_memtable_dir(table_name);
    char* memtable_file = malloc(MAX_PATH_SIZE);

    snprintf(
        memtable_file,
        MAX_PATH_SIZE,
        "%s/%s.%s",
        memtable_dir,
        table_name,
        MEMTABLE_FILE_EXTENSION
    );

    return memtable_file;
}

char* create_memtable_file(char* table_name) {
    char* memtable_dir = create_memtable_dir(table_name);
    free(memtable_dir);

    char* memtable_file = get_memtable_file(table_name);
    create_file(memtable_file);

    return memtable_file;
}

// ========== FILE HANDLING

// ========== FUTURE UTILS 
void rpad(unsigned char* buffer, char *str, size_t size) {
    strncpy((char*) buffer, str, size);

    int str_len = strlen(str);
    if (str_len >= size) return;

    for (int i = str_len; i < size; i++) {
        buffer[i] = ' ';
    }
}

// ========== FUTURE UTILS 

// ========== LOAD LOOKUPS 


void parse_sstable_file_entry(
    id_address_lookup_t* lookup, 
    unsigned char* entry,
    size_t key_size
) {
    unsigned char* id = malloc(key_size);
    rpad(id, strtok((char*) entry, SSTABLE_LOOKUP_SEPERATOR), key_size);
    lookup->id = id;
    lookup->address = atoi(strtok(NULL, SSTABLE_LOOKUP_SEPERATOR));
    
    return;
}

void set_sstable_lookup_map(
    sstable_t* sstable, 
    char* table_name
) {
    struct dirent *entry;
    int num_files = 0;
    int curr_block = 0;
    size_t file_size = 0;
    DIR *dir = opendir(sstable->sstable_dir_path);

    size_t block_size = sstable->block_size;
    size_t entry_size = sstable->id_size + sizeof(int) + sizeof(SSTABLE_LOOKUP_SEPERATOR);
    unsigned char* curr_lookup_entry = malloc(entry_size);
    char sstable_file_path[MAX_PATH_SIZE];
    size_t bytes_read;
    FILE* file_ptr;

    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_type != DT_REG) continue;

        snprintf(
            sstable_file_path, 
            MAX_PATH_SIZE,
            "%s/%s", 
            sstable->sstable_dir_path, 
            entry->d_name
        );           
        
        block_size = 0;
        file_size = get_file_size(sstable_file_path);
        file_ptr = fopen(sstable_file_path, "rb");

        while (sstable->block_size * curr_block < file_size) {
            if ((bytes_read = fread(curr_lookup_entry, 1, entry_size, file_ptr)) == entry_size) {
                sstable_lookup_t* sstable_lookup = malloc(sizeof(sstable_lookup_t));
                id_address_lookup_t* address_lookup = malloc(sizeof(id_address_lookup_t));
                parse_sstable_file_entry(address_lookup, curr_lookup_entry, sstable->id_size);

                sstable_lookup->id = malloc(sstable->id_size);
                sstable_lookup->id = address_lookup->id;
                sstable_lookup->block_number = curr_block;
                sstable_lookup->file_name = strdup(entry->d_name);
                unsigned char* id = sstable_lookup->id;

                HASH_ADD(
                    hh,
                    sstable->lookup_map, 
                    id, 
                    sstable->id_size, 
                    sstable_lookup
                );

                block_size++;
            } else {
                break;
            }
        }
    }

    free(curr_lookup_entry);
    closedir(dir);

    return;
}

// ========== LOAD LOOKUPS 


// ========== TYPES 
sstable_t* init_sstable(
    char* table_name,
    size_t id_size,
    size_t block_size,
    int num_blocks
) {

    sstable_t* sstable = malloc(sizeof(sstable_t));
    sstable->table_name = table_name;
    sstable->id_size = id_size;
    sstable->block_size = block_size;
    sstable->max_size = block_size * num_blocks;

    sstable->sstable_dir_path = create_sstable_dir(table_name);
    sstable->lookup_map = NULL;

    create_sstable_file(table_name);

    sstable->memtable_file_path = create_memtable_file(table_name);
    sstable->memtable = NULL;

    return sstable;
}



// ========== TYPES 

int main() {
    char* table_name = "test_table";
    size_t id_size = 10;
    size_t block_size = 4096;
    int blocks_per_sstable = 4;
    size_t sstable_size = block_size * blocks_per_sstable;
    sstable_t* sstable = init_sstable(
        table_name, 
        id_size,
        block_size, 
        blocks_per_sstable
    );

    return 0;
}

// SSTABLE 
// TABLE  
