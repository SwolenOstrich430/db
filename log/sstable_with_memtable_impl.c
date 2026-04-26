

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
#include <time.h>
#include "../cJSON/cJSON.h"
#include "../uthash-master/src/uthash.h"

char* TABLE_DIR_NAME = "tables";
char* TABLE_FILE_EXTENSION = ".table";
char* SSTABLE_FILE_EXTENSION = "sstable";
char* SSTABLE_CONFIG_FOLDER_NAME = "config";
char* SSTABLE_DIR_NAME = "sstable";
char* SSTABLE_LOOKUP_SEPERATOR = ":";
char* MEMTABLE_DIR = "memtable";
char* MEMTABLE_FILE_EXTENSION = "memtable";
mode_t DEFAULT_PERMISSIONS = 0775;
size_t MAX_PATH_SIZE = 2048;
size_t MAX_UC_LENGTH = 10240;
size_t SSTABLE_ADDRESS_HASH_SIZE = 20;

typedef struct {
    char* sstable_file_name;
    int block_number;
    unsigned char* hash_code;
    int is_empty;
} sstable_address_t;

typedef struct {
    unsigned char* id;
    sstable_address_t* address;
    size_t id_size;
    UT_hash_handle hh;
} sstable_lookup_t;

typedef struct future_sstable_address_t {
    sstable_address_t* address;
    char* new_sstable_file_name;
    int brand_new;
    time_t epoch;
    struct future_sstable_address_t* next;
} future_sstable_address_t;

typedef struct {
    unsigned char* id;
    unsigned char* address;
    unsigned char* raw_id;
    int raw_address;
    UT_hash_handle hh;
} id_address_lookup_t;

typedef struct {
    unsigned char* id;
    unsigned char* bytes;  
    int raw_id_length;    
    unsigned char* raw_id;
    int raw_bytes_length;
    unsigned char* raw_bytes;    
    size_t size;
    size_t id_size;
    UT_hash_handle hh;
} entry_t;

typedef struct sstable_t {
    char* table_name;
    size_t id_size;
    size_t block_size;
    size_t display_block_size;
    size_t entry_size;
    size_t max_size;
    size_t record_size;
    int num_blocks;
    entry_t* memtable;
    sstable_lookup_t* lookup_map;
    char* sstable_dir_path;
    char* memtable_file_path;
    size_t sstable_address_hash_size;
} sstable_t;

unsigned char* entry_get_id(entry_t* entry) {
    return entry->id;
}

unsigned char* entry_get_value(entry_t* entry) {
    return entry->bytes;
}

char* sstable_get_table_name(sstable_t* sstable) {
    return sstable->table_name;
}

char* sstable_get_memtable_file_path(sstable_t* sstable) {
    return sstable->memtable_file_path;
}

char* sstable_get_sstable_dir_path(sstable_t* sstable) {
    return sstable->sstable_dir_path;
}

size_t sstable_get_block_size(sstable_t* sstable) {
    return sstable->block_size;
}

size_t sstable_get_display_block_size(sstable_t* sstable) {
    return sstable->display_block_size;
}

size_t sstable_get_entry_size(sstable_t* sstable) {
    return sstable->entry_size;
}

size_t sstable_get_max_size(sstable_t* sstable) {
    return sstable->max_size;
}

size_t sstable_get_record_size(sstable_t* sstable) {
    return sstable->record_size;
}

size_t sstable_get_id_size(sstable_t* sstable) {
    return sstable->id_size;
}

int sstable_get_num_blocks(sstable_t* sstable) {
    return sstable->num_blocks;
}

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
    char* dir_path_copy = malloc(MAX_PATH_SIZE);
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
        perror("error: could not create base dir\n");
        exit(1);
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

char* get_uuid() {
    uuid_t binuuid;
    uuid_generate_random(binuuid);
    
    char* uuid_str = malloc(37 * sizeof(char));
    uuid_unparse(binuuid, uuid_str); 
    
    return uuid_str;
}

char* create_base_dir(char* table_name) {
    char* base_dir = get_base_dir(table_name);
    int dir_created = create_dir(base_dir);

    if (dir_created != 1) {
        perror("error: could not create base dir\n");
        exit(1);
    }

    return base_dir;
}

char* create_sstable_dir(char* table_name) {
    create_base_dir(table_name);
    char* sstable_dir = get_sstable_dir(table_name);
    create_dir(sstable_dir);

    return sstable_dir;
}

char* get_temp_sstable_file_name(sstable_t* sstable, time_t flush_epoch) {
    char* temp_dir = malloc(MAX_PATH_SIZE * sizeof(char));
    snprintf(temp_dir, MAX_PATH_SIZE, "%s/%ld", sstable->sstable_dir_path, flush_epoch);
    create_dir(temp_dir);

    char* final_path = malloc(MAX_PATH_SIZE * sizeof(char));
    char* uuid_str = get_uuid();

    snprintf(
        final_path,
        MAX_PATH_SIZE * sizeof(char),
        "%s/%s_%s.%s",
        temp_dir,
        sstable->table_name,
        uuid_str,
        SSTABLE_FILE_EXTENSION
    );

    free(temp_dir);
    free(uuid_str);
    return final_path;
}

char* create_temp_sstable_file(sstable_t* sstable, time_t flush_epoch) {
    char* file_path = get_temp_sstable_file_name(sstable, flush_epoch);
    
    FILE* file_ptr = fopen(file_path, "w");
    fprintf(file_ptr, "%s", "");
    fclose(file_ptr);
    
    return file_path;
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

int get_entry_size(sstable_t* sstable) {
    return (
        sstable->id_size + 
        sstable->record_size + 
        strlen(SSTABLE_LOOKUP_SEPERATOR)
    );
}

// TODO: for now this won't handle quoted spaces (i.e., null and spaces are the same rn)
int uclen(unsigned char* uc) {
    int length = 0;

    while ((uc[length] != '\0' && uc[length] != ' ') && length < MAX_UC_LENGTH) {
        length++;
    }

    return length;
}

unsigned char* ucstrip(unsigned char* uc) {
    size_t uc_length = uclen(uc);
    unsigned char* raw_uc = malloc(uc_length + 1);
    memcpy(raw_uc, uc, uc_length);
    raw_uc[uc_length] = '\0';

    return raw_uc;
}

void rpad(unsigned char* buffer, size_t size, unsigned char *src) {
    size_t src_size = uclen(src);

    if (src_size > size) {
        src_size = size;
    }

    memset(buffer, '\0', size);
    memcpy(buffer, src, src_size);
}

int id_address_lookup_cmp(
    id_address_lookup_t* id_addr_1, 
    id_address_lookup_t* id_addr_2 
) {
    if (id_addr_1 == NULL && id_addr_2 != NULL) {
        return -1;
    } else if (id_addr_1 != NULL && id_addr_2 == NULL) {
        return 1;
    } else if (id_addr_1 == NULL && id_addr_2 == NULL) {
        return 0;
    }

    int diff = strcmp(
        (const char*)id_addr_1->id, 
        (const char*)id_addr_2->id
    );

    if (diff == 0) {
        diff = id_addr_1->raw_address - id_addr_2->raw_address;
    }
    
    return diff;
}


entry_t* init_entry(
    sstable_t* sstable,
    unsigned char* id,
    unsigned char* bytes
) {
    if (sstable == NULL) {
        perror("error: sstable cannot be null\n");
        exit(1);
    }

    if (sstable->id_size <= 0) {
        perror("error: sstable id size must be greater than 0\n");
        exit(1);
    }

    if (sstable->record_size <= 0) {
        perror("error: sstable record size must be greater than 0\n");
        exit(1);
    }

    if (bytes == NULL) {
        perror("error: entry bytes cannot be null\n");
        exit(1);
    }

    entry_t* entry = malloc(sizeof(entry_t));
    entry->id = malloc(sstable->id_size);
    entry->raw_id_length = uclen(id);
    rpad(entry->id, sstable->id_size, id);
    entry->raw_id = ucstrip(entry->id);

    entry->bytes = malloc(sstable->record_size);
    entry->raw_bytes_length = uclen(bytes);
    rpad(entry->bytes, sstable->record_size, bytes);
    entry->raw_bytes_length = uclen(bytes);
    entry->raw_bytes = ucstrip(entry->bytes);

    entry->size = sstable->record_size;
    entry->id_size = sstable->id_size;

    return entry;
}

typedef struct uc_node_t {
    unsigned char* value;
    size_t length;
    struct uc_node_t* next;
} uc_node_t;

uc_node_t* init_uc_node(
    unsigned char* value,
    size_t length
) {
    if (length <= 0 || length > 999999) {
        perror("error: length must be greater than 0 and less than or equal to 1024\n");
        exit(1);
    }

    uc_node_t* node = calloc(1, sizeof(uc_node_t));
    node->value = malloc(sizeof(unsigned char) * (length + 1));
    memcpy(node->value, value, length);
    node->value[length] = '\0';
    node->length = length;
    node->next = NULL; 
    return node;
}

uc_node_t* get_spliterator_uc(
    unsigned char* uc, 
    size_t uc_length,
    const char* delim
) {
    if (delim == NULL) {
        perror("error: delimiiter cannot be null\n");
        exit(1);
    }

    if (uc == NULL) {
        perror("error: uc cannot be null\n");   
        exit(1);       
    }

    if (uc_length == 0 || uc_length > 999999) {
        perror("error: length must be greater than 0 and less than or equal to 1024\n");
        exit(1);
    }

    int uc_index = 0;
    int last_start_index = 0;
    int delim_index = 0;
    size_t delim_len = strlen(delim);

    uc_node_t* start = NULL;
    uc_node_t* end = NULL;

    while (uc_index < uc_length) {
        if (delim_index > delim_len - 1) {            
            if (start == NULL) {
                start = init_uc_node(
                    &uc[last_start_index], 
                    uc_index - last_start_index - delim_len
                );
                end = start;
                last_start_index = uc_index;
            } else if (end == NULL) {
                perror("error(internal): end can't be null if start is set\n");
                exit(1);
            } else {
                end->next = init_uc_node(
                    &uc[last_start_index],
                    uc_index - last_start_index - delim_len
                );
                end = end->next;
                last_start_index = uc_index;
            }

            delim_index = 0;
            continue;
        } 

        if (uc[uc_index] == delim[delim_index]) {
            delim_index++;
        } else {
            delim_index = 0;
        }

        uc_index++;
    }
    // 
 
    // make sure we get uc[-1] if it doesn't end in the delim 
    // but we have previous matches (since end isn't null)
    // e.g., delim=; | uc='ab;cd;ef;gg'
    // 'gg' wouldn't be added to the list if we didn't do this 
    // TODO: this is gross. clean up so everything's in the while loop
    //       this depends on 'last_start_index' only being set if we're not 
    //       at the last character in 'uc'
    if (start != NULL && last_start_index < uc_index - 1) {
        end->next = init_uc_node(
            &uc[last_start_index],
            uc_length - last_start_index
        );
        end = end->next;
    }

    return start;
}


int get_record_size(sstable_t* sstable) {
    return (
        sstable->id_size + 
        sstable->record_size + 
        strlen(SSTABLE_LOOKUP_SEPERATOR)
    );
}

void destroy_uc_node(uc_node_t* node) {
    if (node == NULL) return;

    while (node != NULL) {
        uc_node_t* temp = node;
        node = node->next;
        free(temp->value);
        free(temp);
    }

    free(node);
    return;
}

entry_t* parse_sstable_file_entry(
    sstable_t* sstable,
    unsigned char* entry,
    int allow_null_parse
) {
    size_t entry_size = get_entry_size(sstable);
    uc_node_t* parsed_entry = get_spliterator_uc(entry, entry_size, SSTABLE_LOOKUP_SEPERATOR);

    if (parsed_entry == NULL && allow_null_parse != 1) {
        perror("error(internal): parsed address cannot be null -- \
                likely the entry format is bad or 'SSTABLE_LOOKUP_SEPERATOR' \
                is out of sync\n");
        exit(1);
    
    } else if (parsed_entry == NULL && allow_null_parse == 1) {
        return NULL;
    } else if (parsed_entry->next == NULL) {
        perror("error(internal): parsed address must be two non-null or empty values \
                separated by a colon\n");
        exit(1);
    } else if (parsed_entry->value == NULL) {
        perror("error(internal): parsed entry value cannot be null or empty\n");
        exit(1);
    } 
    
    uc_node_t* next_node = parsed_entry->next;

    entry_t* final_entry = init_entry(
        sstable,
        parsed_entry->value,
        next_node->value
    );

    destroy_uc_node(parsed_entry);

    return final_entry;
}

void load_memtable_from_file(sstable_t* sstable) {
    sstable->memtable = NULL;
    // create the file if it doesn't exist 
    char* file_path = create_memtable_file(sstable->table_name);

    // if it doesn't exist after we force create, error and exit 
    struct stat st;
    if (stat(file_path, &st) != 0) {
        perror("error: unable to create memtable file");
        exit(1);
    }

    // loop through each entry
    // if we fail to get to the end of the file, error
    size_t entry_size = get_record_size(sstable);
    FILE* pfile = fopen(file_path, "rb");
    unsigned char* curr_entry = malloc(entry_size);
    int bytes_read;

    while ((bytes_read = fread(curr_entry, 1, entry_size, pfile)) == entry_size) {
        entry_t* record = parse_sstable_file_entry(sstable, curr_entry, 0);
        entry_t* found_record = NULL;

        HASH_FIND(
            hh,
            sstable->memtable,
            record->id,
            sstable->id_size,
            found_record
        );

        // TODO: should we do something if we find an existing record?
        if (found_record == NULL) {
            HASH_ADD_KEYPTR(
                hh, 
                sstable->memtable,
                record->id,
                sstable->id_size,
                record
            );
        }
    }

    unsigned int num_users = HASH_COUNT(sstable->memtable);

    free(curr_entry);
    sstable->memtable_file_path = file_path;
    return;
}

unsigned long djb2_hash(char *str) {
    unsigned long hash = 5381; 
    int c;

    while ((c = *str++)) {
        hash = ((hash << 5) + hash) + c; 
    }

    return hash;
}

char *strremove(char *str, const char *sub) {
    size_t sub_len = strlen(sub);
    char *result = malloc(strlen(str) + 1); // Allocate space for a new copy
    if (!result) return NULL;

    const char *p = str;
    char *q = result;

    while (*p) {
        // Check if the current position matches the substring to remove
        if (sub_len > 0 && strncmp(p, sub, sub_len) == 0) {
            p += sub_len; // Skip the substring
        } else {
            *q++ = *p++;  // Copy the character
        }
    }
    *q = '\0'; // Null-terminate the new string
    return result;
}

unsigned char* sstable_address_to_hash(
    sstable_address_t* address, 
    size_t hash_size
) {
    if (address == NULL || address->sstable_file_name == NULL) {
        perror("Provided 'addres' or 'address->sstable_file_name' was null");
        exit(1);
    }
    
    size_t val_size = strlen(address->sstable_file_name) + 32;
    char val_to_hash[val_size];
    unsigned long hash_as_long = djb2_hash(val_to_hash);
    size_t copy_size = hash_size < sizeof(hash_as_long)
                   ? hash_size
                   : sizeof(hash_as_long);
    unsigned char* addr_hash = malloc(copy_size);
    unsigned char* addr_hash_final = malloc(val_size);
    
    snprintf(
        val_to_hash, 
        val_size, 
        "%s%d", 
        address->sstable_file_name, 
        address->block_number
    );

    memcpy(addr_hash, &hash_as_long, copy_size);
    rpad(addr_hash_final, hash_size, addr_hash);
    free(addr_hash);
    
    return addr_hash_final;
}

sstable_address_t* init_sstable_address(
    sstable_t* sstable,
    char* sstable_file_name,
    int block_number
) {
    // TODO: turn these into helper functions 
    if (sstable_file_name == NULL || strlen(sstable_file_name) == 0) {
        perror("Provided 'sstable_file_name' is null or empty.");
        exit(1);
    }

    if (block_number < 0) {
        perror("Provided block_number is null or less than zero.");
        exit(1);
    }

    int file_path_size = MAX_PATH_SIZE + sizeof(int);
    sstable_address_t* address = malloc(sizeof(sstable_address_t));
    
    if (strstr(sstable_file_name, sstable->sstable_dir_path) == NULL) {
        address->sstable_file_name = malloc(file_path_size);
        snprintf(
            address->sstable_file_name, 
            file_path_size, 
            "%s/%s", 
            sstable->sstable_dir_path, 
            sstable_file_name
        );
    } else {
        address->sstable_file_name = sstable_file_name;
    }

    address->block_number = block_number;
    address->hash_code = sstable_address_to_hash(
        address, 
        SSTABLE_ADDRESS_HASH_SIZE
    );

    return address;
}

void memcpy_sstable_address(sstable_address_t* dest, sstable_address_t* src) {
    if (dest->sstable_file_name == NULL) {
        dest->sstable_file_name = malloc(MAX_PATH_SIZE * sizeof(char));
    }

    if (dest->hash_code == NULL) {
        dest->hash_code = malloc(SSTABLE_ADDRESS_HASH_SIZE * sizeof(char));
    }

    strncpy(dest->sstable_file_name, src->sstable_file_name, MAX_PATH_SIZE);
    memcpy(dest->hash_code, src->hash_code, SSTABLE_ADDRESS_HASH_SIZE);
    dest->block_number = src->block_number;
}

future_sstable_address_t* init_future_sstable_address(
    sstable_t* sstable,
    sstable_address_t* address,
    time_t flush_epoch
) {
    future_sstable_address_t* future_address = calloc(1, sizeof(future_sstable_address_t));

    if (address == NULL) {
        future_address->address = NULL;
        future_address->brand_new = 1;
    } else {
        future_address->address = calloc(1, sizeof(sstable_address_t));
        memcpy_sstable_address(future_address->address, address);
        future_address->brand_new = 0;
    }

    future_address->new_sstable_file_name = create_temp_sstable_file(
        sstable,
        flush_epoch
    );
    future_address->epoch = flush_epoch;
    
    return future_address;
}

sstable_lookup_t* init_sstable_lookup(
    sstable_t* sstable,
    unsigned char* id,
    char* sstable_file_name,
    int block_number
) {
    sstable_address_t* address = init_sstable_address(
        sstable,
        sstable_file_name,
        block_number
    );

    sstable_lookup_t* lookup = malloc(sstable->id_size + sizeof(sstable_address_t));
    lookup->id = id;
    lookup->address = address;
    lookup->id_size = sstable->id_size;

    return lookup;
}

void load_sstable_lookup_map(sstable_t* sstable) {
    if (sstable == NULL) {
        perror("Provided sstable cannot be null\n");
        exit(1);
    }

    sstable->lookup_map = NULL;
    DIR *dr;
    struct dirent *sstable_file;
    dr = opendir(sstable->sstable_dir_path);
    if (dr == NULL) {
        perror("Could not open directory");
        exit(1);
    }

    int block_number;
    int bytes_read;
    int curr_sstable_bytes_read;
    char* curr_file = malloc(MAX_PATH_SIZE);
    FILE* pfile = NULL;
    int entry_size = get_entry_size(sstable);

    while ((sstable_file = readdir(dr)) != NULL) {
        if (
            strcmp(sstable_file->d_name, ".") == 0 || 
            strcmp(sstable_file->d_name, "..") == 0 || 
            strstr(sstable_file->d_name, "_temp")
        ) {
            continue;
        }

        block_number = 0;
        snprintf(
            curr_file, 
            MAX_PATH_SIZE, 
            "%s/%s", 
            sstable->sstable_dir_path, 
            sstable_file->d_name
        );

        if (pfile != NULL) {
            fclose(pfile);
        }

        pfile = fopen(curr_file, "rb");
        unsigned char* entry = malloc(entry_size);
        block_number = 0;
        int entry_count = 0;

        while ((bytes_read = fread(entry, 1, entry_size, pfile)) == entry_size) {
            entry_t* parsed_entry = parse_sstable_file_entry(sstable, entry, 0);
            
            sstable_lookup_t* address = init_sstable_lookup(
                sstable,
                parsed_entry->id,
                sstable_file->d_name,
                block_number
            );

            HASH_ADD_KEYPTR(hh, sstable->lookup_map, parsed_entry->id, sstable->id_size, address);
            block_number++;
            fseek(pfile, (block_number * sstable->block_size), SEEK_SET);

            if (block_number > sstable->num_blocks) {
                // CRITICAL: NEED TO UNCOMMENT AFTER DEBUG
                perror("Current sstbale file has too many blocks\n");
                exit(1);
            }

            if (ftell(pfile) > sstable->max_size) {
                perror("Current sstable file is greater than max size\n");
                exit(1);
            }
        }
    }

    if (pfile != NULL) {
        fclose(pfile);
    }

}

char* sstable_get_config_file_path(char* table_name) {
    char* config_file_path = malloc(MAX_PATH_SIZE);
    char* sstable_dir_path = get_sstable_dir(table_name);

    snprintf(
        config_file_path,
        MAX_PATH_SIZE,
        "%s/%s/%s.config",
        sstable_dir_path,
        SSTABLE_CONFIG_FOLDER_NAME,
        table_name
    );

    free(sstable_dir_path);

    return config_file_path;
}

int sstable_exists(char* table_name) {
    char* config_file = sstable_get_config_file_path(table_name);
    struct stat st;

    int exists = (stat(config_file, &st) == 0) ? 1 : -1;
    free(config_file);

    return exists;
}

void sstable_store_config_to_file(sstable_t* sstable) {
    cJSON *config_obj = cJSON_CreateObject();
    cJSON_AddItemToObject(config_obj, "table_name", cJSON_CreateString(sstable->table_name));
    cJSON_AddItemToObject(config_obj, "id_size", cJSON_CreateNumber(sstable->id_size));
    cJSON_AddItemToObject(config_obj, "record_size", cJSON_CreateNumber(sstable->record_size));
    cJSON_AddItemToObject(config_obj, "block_size", cJSON_CreateNumber(sstable->block_size));
    cJSON_AddItemToObject(config_obj, "num_blocks", cJSON_CreateNumber(sstable->num_blocks));

    char* config_file_path = sstable_get_config_file_path(sstable->table_name);
    create_dir(dirname(config_file_path));

    FILE* file_ptr = fopen(config_file_path, "w");

    if (file_ptr == NULL) {
        perror("Unable to open config file for writing\n");
        exit(1);
    }

    char* config_text = cJSON_Print(config_obj);
    fprintf(file_ptr, "%s", config_text);
    fclose(file_ptr);
}

cJSON* sstable_get_config(char* table_name) {
    char* config_file_path = sstable_get_config_file_path(table_name);
    FILE* file_ptr = fopen(config_file_path, "r");

    if (file_ptr == NULL) {
        perror("Unable to open config file for reading\n");
        exit(1);
    }

    fseek(file_ptr, 0, SEEK_END);
    long file_size = ftell(file_ptr);
    rewind(file_ptr);

    char* config_text = malloc(file_size + 1);
    fread(config_text, 1, file_size, file_ptr);
    config_text[file_size] = '\0';

    fclose(file_ptr);

    cJSON* config_json = cJSON_Parse(config_text);
    free(config_text);
    free(config_file_path);

    if (config_json == NULL) {
        perror("Unable to parse config JSON\n");
        exit(1);
    }

    return config_json;
}

sstable_t* load_sstable(char* table_name) {
    sstable_t* sstable = malloc(sizeof(sstable_t));
    cJSON* config_json = sstable_get_config(table_name);
    
    sstable->table_name = strdup(table_name);
    sstable->id_size = cJSON_GetObjectItem(config_json, "id_size")->valueint;
    sstable->record_size = cJSON_GetObjectItem(config_json, "record_size")->valueint;
    sstable->entry_size = get_entry_size(sstable);
    sstable->block_size = cJSON_GetObjectItem(config_json, "block_size")->valueint;
    sstable->display_block_size = sstable->block_size;
    sstable->num_blocks = cJSON_GetObjectItem(config_json, "num_blocks")->valueint;
    sstable->max_size = sstable->block_size * sstable->num_blocks;

    sstable->sstable_dir_path = get_sstable_dir(table_name);
    load_sstable_lookup_map(sstable);

    sstable->memtable = NULL;
    load_memtable_from_file(sstable);

    cJSON_Delete(config_json);
    
    return sstable;
}

sstable_t* init_sstable(
    char* table_name,
    size_t id_size,
    size_t record_size,
    size_t block_size,
    int num_blocks
) {
    if (table_name == NULL || strlen(table_name) == 0) {
        perror("error: table name cannot be null or empty\n");
        exit(1);
    }

    if (id_size <= 0) {
        perror("error: id size must be greater than 0\n");
        exit(1);
    }

    if (record_size <= 0) {
        perror("error: record size must be greater than 0\n");
        exit(1);
    }

    if (block_size <= 0) {
        perror("error: block size must be greater than 0\n");
        exit(1);
    }

    if (num_blocks <= 0) {
        perror("error: num blocks must be greater than 0\n");
        exit(1);
    }

    // TODO: in the future check if the existing sstable config matches the provided config and error if not
    //       update the sstable config or check if we should change the config  
    if (sstable_exists(table_name) == 1) {
        return load_sstable(table_name);
    }

    sstable_t* sstable = malloc(sizeof(sstable_t));
    sstable->table_name = strdup(table_name);
    sstable->id_size = id_size;
    sstable->record_size = record_size;
    sstable->entry_size = get_entry_size(sstable);
    // e.g., block size is 300 and entry size is 70
    //       actual block size is 280
    //       why write another 20 bytes if they're going to be null anyway
    sstable->block_size = ((block_size / sstable->entry_size) * sstable->entry_size);
    sstable->display_block_size = block_size;
    sstable->num_blocks = num_blocks;
    // TODO: add this as a config option in sstbale instead of just as constant 
    sstable->sstable_address_hash_size = SSTABLE_ADDRESS_HASH_SIZE;
    sstable->max_size = sstable->block_size * sstable->num_blocks;

    sstable->sstable_dir_path = create_sstable_dir(table_name);
    load_sstable_lookup_map(sstable);

    sstable->memtable = NULL;
    load_memtable_from_file(sstable);

    sstable_store_config_to_file(sstable);

    return sstable;
}

void memtable_hash_set(sstable_t* sstable, entry_t* record) {
    entry_t* existing_record; 
    HASH_FIND(
        hh,
        sstable->memtable,
        record->id,
        sstable->id_size,
        existing_record
    );
    
    if (existing_record == NULL) {
        HASH_ADD_KEYPTR(
            hh,
            sstable->memtable,
            record->id,
            sstable->id_size,
            record
        );
    } else {
        memcpy(existing_record->bytes, record->bytes, sstable->record_size);
    }

    entry_t* stored_record; 
    HASH_FIND(
        hh,
        sstable->memtable,
        record->id,
        sstable->id_size,
        stored_record
    );

    if (memcmp(record->bytes, stored_record->bytes, sstable->record_size) != 0) {
        perror("error: failed to add record to memtable");
        exit(1);
    }
}

unsigned char* entry_to_file_entry(
    sstable_t* sstable,
    entry_t* record
) {
    size_t id_size = sstable->id_size;
    size_t record_size = sstable->record_size;
    size_t sep_len = strlen(SSTABLE_LOOKUP_SEPERATOR);
    size_t entry_size = id_size + sep_len + record_size;

    unsigned char* file_entry = malloc(entry_size);
    size_t offset = 0;

    memcpy(file_entry + offset, record->id, id_size);
    offset += id_size;
    memcpy(file_entry + offset, SSTABLE_LOOKUP_SEPERATOR, sep_len);
    offset += sep_len;
    memcpy(file_entry + offset, record->bytes, record_size);

    return file_entry;
}

void memtable_file_append(sstable_t* sstable, entry_t* record) {
    FILE* file_ptr = fopen(sstable->memtable_file_path, "ab");
    if (file_ptr == NULL) {
        perror("Unable to open memtable file.");
        exit(1);
    }

    unsigned char* record_entry = entry_to_file_entry(sstable, record);
    size_t entry_size = get_entry_size(sstable);

    fwrite(record_entry, 1, entry_size, file_ptr);
    fclose(file_ptr);
    free(record_entry);

    return;
}

void memtable_append(sstable_t* sstable, entry_t* record) {
    memtable_hash_set(sstable, record);
    memtable_file_append(sstable, record);
}

// ========== PROTECTED

int cmp_sstable_lookup(
    const sstable_lookup_t* lookup_1, 
    const sstable_lookup_t* lookup_2
) {
    return memcmp(lookup_1->id, lookup_2->id, lookup_1->id_size);
}

int cmp_entry_t(
    const entry_t* entry_1, 
    const entry_t* entry_2

) {
    return memcmp(entry_1->id, entry_2->id, entry_1->id_size);
}

char* get_file_extension(char* file_path) {
    char *dot = strrchr(file_path, '.');
    if (!dot || dot == file_path) return NULL; 
    const char *slash = strrchr(file_path, '/');

    if (slash == NULL || dot > slash) {
        return dot;
    } 

    return NULL;
}

char* get_temp_sstable_file(sstable_t* sstable, char* file_path, int file_num) {
    char* temp_file_path = strdup(file_path);
    char* bname = basename(temp_file_path);
    const char* dot_position = strrchr(bname, '.');
    char* final_file = malloc(MAX_PATH_SIZE);
    
    const char* file_name;
    const char *last_slash = strrchr(file_path, '/');
    const char *last_backslash = strrchr(file_path, '\\');

    if (last_slash != NULL) {
        file_name = last_slash + 1;
    } else if (last_backslash != NULL) {
        file_name = last_backslash + 1;
    } else {
        file_name = file_path; // No path separators, the input is just a file_name
    }

    size_t length_no_ext = strlen(file_name) - strlen(dot_position);

    char* basename_no_ext = (char*)malloc(length_no_ext + 1);
    if (basename_no_ext != NULL) {
        strncpy(basename_no_ext, file_name, length_no_ext);
    }

    char* temp_suffix = malloc(10);
    if (file_num > 0) {
        snprintf(temp_suffix, 10, "_temp_%d", file_num);
    } else {
        snprintf(temp_suffix, 10, "_temp");
    }

    snprintf(
        final_file,
        MAX_PATH_SIZE,
        "%s/%s%s%s",
        sstable->sstable_dir_path,
        basename_no_ext,
        temp_suffix,
        get_file_extension(file_path)
    );

    free(temp_suffix);
    free(basename_no_ext);

    return final_file;
}

int file_exists(char* file_path) {
    struct stat st;
    return (stat(file_path, &st) != 0) ? 0 : 1;
}

void clear_memtable_file(sstable_t* sstable) {
    FILE* file_ptr = fopen(sstable->memtable_file_path, "w");
    fprintf(file_ptr, "%s", "");
    fclose(file_ptr);
    return;
}

void reset_memtable(sstable_t* sstable) {
    sstable->memtable = NULL;
    clear_memtable_file(sstable);
    create_memtable_file(sstable->table_name);
}

future_sstable_address_t* get_next_address_for_flush(
    sstable_t* sstable,
    entry_t* memtable_entry,
    sstable_lookup_t* sstable_lookup,
    time_t flush_epoch
) {
    if (memtable_entry == NULL) {
        // TODO: turn into helper (error_and_exit?)
        perror("Memtable entry cannot be null\n");
        exit(1);
    }

    int diff = 1;

    while (
        (diff > 0) &&
        sstable_lookup != NULL &&
        sstable_lookup->id != NULL && 
        sstable_lookup->hh.next != NULL
    ) {
        diff = strcmp(
            (const char*)memtable_entry->id, 
            (const char*)sstable_lookup->id
        );

        if (diff > 0 && sstable_lookup->hh.next != NULL) {
            sstable_lookup = sstable_lookup->hh.next;
        }
    }

    // if (sstable_lookup != NULL && sstable_lookup->hh.prev != NULL) {
    //     sstable_lookup = sstable_lookup->hh.prev;
    // }

    if (sstable_lookup == NULL || sstable_lookup->address == NULL) {
        return init_future_sstable_address(sstable, NULL, flush_epoch);
    } 

    return init_future_sstable_address(sstable, sstable_lookup->address, flush_epoch);
}

void entryccpy(entry_t* dest, entry_t* src, sstable_t* sstable) {
    if (dest == NULL || src == NULL) {
        perror("Provided 'dest' or 'src' entry cannot be null");
        exit(1);
    }

    if (sstable == NULL) {
        perror("Provided 'sstable' cannot be null");
        exit(1);
    }

    if (dest->id == NULL) {
        dest->id = malloc(sstable->id_size);
    }

    if (dest->bytes == NULL) {
        dest->bytes = malloc(sstable->record_size);
    }
    
    dest->size = src->size;
    memcpy(dest->id, src->id, sstable->id_size);
    memcpy(dest->bytes, src->bytes, sstable->record_size);
}

// TODO: consider returning list of future_sstable_address_t
unsigned char* flush_sstable_address(
    sstable_t* sstable,
    entry_t* memtable_entry,
    future_sstable_address_t* head_fut_sstable_address,
    int is_last_entry
) {
    unsigned char* last_id_written = malloc(sstable->id_size * sizeof(unsigned char));
    entry_t* curr_mem_entry = memtable_entry;
    FILE* sstable_file = NULL; 
    int starting_address = 0;
    if (head_fut_sstable_address->brand_new == 0) {
        sstable_file = fopen(head_fut_sstable_address->address->sstable_file_name, "rb");
        starting_address = sstable->block_size * head_fut_sstable_address->address->block_number;
    }

    FILE* new_sstable_file = fopen(head_fut_sstable_address->new_sstable_file_name, "wb+");
    // write up to the address that you need 
    // track the current address based on bytes read 
    int total_bytes_read = 0;
    entry_t* parsed_entry = NULL;
    entry_t* last_parsed_entry = calloc(1, sizeof(entry_t)); 
    size_t entry_size = get_entry_size(sstable);
    unsigned char* entry = malloc(entry_size * sizeof(unsigned char));

    // if we're at a block greater than 0, write everything before then
    // TODO: think about bulk writing this all at once? 
    //       not sure if that's going to perform all that differently 
    //       but definitely could
    while (total_bytes_read < starting_address && head_fut_sstable_address->brand_new == 0) {
        total_bytes_read += fread(entry, 1, entry_size, sstable_file); 
        parsed_entry = parse_sstable_file_entry(sstable, entry, 0);
        fwrite(entry, entry_size, 1, new_sstable_file);
    }

    // TODO: add check for max iterations 
    int id_diff;
    int last_write_from_memtable = 1;
    future_sstable_address_t* curr_fut_address = head_fut_sstable_address;

    while (1) {
        total_bytes_read += entry_size;

        if (parsed_entry != NULL) {
            entryccpy(last_parsed_entry, parsed_entry, sstable);
        } 

        if ((sstable_file != NULL) && (parsed_entry == NULL || last_write_from_memtable == 0)) {
            fread(entry, 1, entry_size, sstable_file); 
            parsed_entry = parse_sstable_file_entry(sstable, entry, 1);

            if (last_parsed_entry->id != NULL && 
                memcmp(
                    parsed_entry->id, 
                    last_parsed_entry->id,
                    sstable->id_size
                ) == 0
            ) {
                parsed_entry = NULL;
            }
        }  
        
        if (sstable_file == NULL) {
            parsed_entry = NULL;
        }

        if (
            (parsed_entry == NULL && 
            last_parsed_entry->id != NULL && 
            curr_mem_entry != NULL && 
            curr_mem_entry->id != NULL) && 
            is_last_entry == 0 && 
            strcmp(
                (const char*)curr_mem_entry->id,
                (const char*)last_parsed_entry->id
            ) > 0
        ) {
            break;
        } else if(parsed_entry == NULL && (curr_mem_entry == NULL || curr_mem_entry->id == NULL)) {
            break;
        } else if (parsed_entry == NULL || parsed_entry->id == NULL) {
            id_diff = 0;
        } else if (curr_mem_entry == NULL || curr_mem_entry->id == NULL) {
            id_diff = 1;
        } else {
            // TODO: make strcmp for unsigned char 
            id_diff = strcmp((const char*)curr_mem_entry->id, (const char*) parsed_entry->id);
        }
        
        if (id_diff > 0) {
            last_write_from_memtable = 0;
            fwrite(entry, entry_size, 1, new_sstable_file);
        } else if (id_diff <= 0) {
            last_write_from_memtable = 1;
            unsigned char* mem_entry = entry_to_file_entry(sstable, curr_mem_entry);
            fwrite(mem_entry, entry_size, 1, new_sstable_file);
            free(mem_entry);
            memcpy(last_id_written, curr_mem_entry->id, sstable->id_size);
            curr_mem_entry = curr_mem_entry->hh.next;
        } else {
            perror("error(internal): unexpected id_diff value");
            exit(1);
        }

        if (total_bytes_read == sstable->max_size) {
            fclose(new_sstable_file);
            total_bytes_read = 0;
            curr_fut_address->next = get_next_address_for_flush(
                sstable, curr_mem_entry, NULL, curr_fut_address->epoch
            );
            curr_fut_address = curr_fut_address->next;
            new_sstable_file = fopen(curr_fut_address->new_sstable_file_name, "wb+");
        }
    }

    free(entry);
    free(last_parsed_entry);
    fclose(new_sstable_file);

    if (sstable_file != NULL) {
        fclose(sstable_file);
    }

    return last_id_written;
}

// TODO: replace all dirty sstables 
void flush_memtable_new(sstable_t* sstable) {
    HASH_SORT(sstable->memtable, cmp_entry_t);
    HASH_SORT(sstable->lookup_map, cmp_sstable_lookup);

    entry_t* memtable_entry = sstable->memtable;
    sstable_lookup_t* sstable_lookup = sstable->lookup_map;
    time_t current_epoch_time = time(NULL);
    future_sstable_address_t* head_fut_table = NULL;
    future_sstable_address_t* curr_fut_table = NULL;

    while (memtable_entry != NULL) {
        if (head_fut_table == NULL) {
            head_fut_table = get_next_address_for_flush(
                sstable, memtable_entry, sstable_lookup, current_epoch_time
            );
            curr_fut_table = head_fut_table;
        } else {
            while (curr_fut_table != NULL && curr_fut_table->next != NULL) {
                curr_fut_table = curr_fut_table->next;
            }

            if (curr_fut_table == NULL) {
                curr_fut_table = get_next_address_for_flush(
                    sstable, memtable_entry, sstable_lookup, current_epoch_time
                );
            } else {
                curr_fut_table->next = get_next_address_for_flush(
                    sstable, memtable_entry, sstable_lookup, current_epoch_time
                );
                curr_fut_table = curr_fut_table->next;
            }            
        }
        
        if (curr_fut_table == NULL) {
            perror("Address returned from next address for flush cannot be null\n");
            exit(1);
        }

        // Get the current time as a time_t value (seconds since epoch)
        int is_last_entry = 0;
        if (sstable_lookup == NULL || sstable_lookup->hh.next == NULL) {
            is_last_entry = 1;
        }

        unsigned char* last_id_written = flush_sstable_address(
            sstable, 
            memtable_entry, 
            curr_fut_table,
            is_last_entry 
        );

        if (last_id_written != NULL) {
            HASH_FIND(
                hh, sstable->memtable, last_id_written, sstable->id_size, memtable_entry
            );

            if (memtable_entry != NULL) {
                memtable_entry = memtable_entry->hh.next;
            }

            while(
                sstable_lookup != NULL && 
                sstable_lookup->id != NULL && 
                strcmp((const char*)last_id_written, (const char*)sstable_lookup->id) > 0
            ) {
                sstable_lookup = sstable_lookup->hh.next;
            }
        }

        free(last_id_written);
    }

    curr_fut_table = head_fut_table;
    while (curr_fut_table != NULL) {
        if (!curr_fut_table->brand_new) {
            rename(
                curr_fut_table->new_sstable_file_name,
                curr_fut_table->address->sstable_file_name
            );
        } else {
            char* new_file = malloc(MAX_PATH_SIZE * sizeof(char));
            snprintf(
                new_file, 
                MAX_PATH_SIZE * sizeof(char),
                "%s/%s",
                sstable->sstable_dir_path,
                basename(curr_fut_table->new_sstable_file_name)
            );
            rename(
                curr_fut_table->new_sstable_file_name,
                new_file 
            );
            free(new_file);
        }

        remove(curr_fut_table->new_sstable_file_name);
        curr_fut_table = curr_fut_table->next;
    }

    reset_memtable(sstable);
}

entry_t* memtable_get(
    unsigned char* in_id, 
    sstable_t* sstable
) {
    // entry->id = malloc(sstable->id_size);
    // rpad(entry->id, sstable->id_size, id, sstable->id_size);

    unsigned char* id = malloc(sstable->id_size);
    rpad(id, sstable->id_size, in_id);
    entry_t* entry = NULL;

    HASH_FIND(
        hh,
        sstable->memtable,
        id,
        sstable->id_size,
        entry
    );

    if (entry == NULL) {
        HASH_FIND(
            hh,
            sstable->memtable,
            in_id,
            sstable->id_size,
            entry
        );
    }

    // TODO: come back and uncomment CRITICAL
    // free(id);
    
    if (entry != NULL) {
        return init_entry(sstable, id, entry->bytes);
    }

    return NULL;
}

entry_t* sstable_get(unsigned char* id, sstable_t* sstable) {
    entry_t* found_entry = memtable_get(id, sstable);

    if (found_entry != NULL) {
        return found_entry;
    }

    unsigned char* formatted_id = malloc(sstable->id_size);
    rpad(formatted_id, sstable->id_size, id);

    // nothing's been added to the table yet, so return null
    if (sstable->lookup_map == NULL) {
        return NULL;
    }

    HASH_SORT(sstable->lookup_map, -cmp_sstable_lookup);
    sstable_lookup_t* lookup = sstable->lookup_map;
    int diff;

    while(lookup != NULL) {
        diff = memcmp(formatted_id, lookup->id, sstable->id_size);

        if (diff >= 0) {
            break;
        } 
        
        lookup = lookup->hh.next;
    }

    // if we don't have anything that's greater than or equal to the id
    // we don't have the id in our sstable
    if (lookup == NULL || lookup->address == NULL) {
        return NULL;
    }

    FILE* sstable_file;
    if (!file_exists(lookup->address->sstable_file_name)) {
        perror("error: sstable file does not exist\n");
        exit(1);
    }

    sstable_file = fopen(lookup->address->sstable_file_name, "rb");
    if (sstable_file == NULL) {
        perror("error: unable to open sstable file\n");
        exit(1);
    }

    fseek(
        sstable_file, 
        lookup->address->block_number * sstable->block_size, 
        SEEK_SET
    );

    int bytes_read;
    size_t entry_size = get_entry_size(sstable);
    unsigned char* entry = malloc(entry_size);
    entry_t* parsed_entry = NULL;

    while ((bytes_read = fread(entry, 1, entry_size, sstable_file)) == entry_size) {
        parsed_entry = parse_sstable_file_entry(sstable, entry, 0);
        if (parsed_entry == NULL) {
            perror("error: unable to parse sstable entry from file\n"); 
            exit(1);
        }

        diff = memcmp(formatted_id, parsed_entry->id, sstable->id_size);

        if (diff == 0) {
            found_entry = parse_sstable_file_entry(sstable, entry, 0);
            break;
        } else if (diff < 0) {
            break;
        }
    }

    fclose(sstable_file);
    free(formatted_id);

    if (found_entry == NULL) {
        free(found_entry);
        return NULL;
    }

    return found_entry;
}

int get_random_int(int min, int max) {
    if (min > max) {
        perror("Minimum value cannot be greater than maximum value");
        exit(1);
    }
    return rand() % (max - min + 1) + min;
}

unsigned char* generate_random_uc_array(int min_length, int max_length, int boop) {
    int length = get_random_int(min_length, max_length);

    unsigned char* random_array = (unsigned char*)malloc(length * sizeof(unsigned char));
    if (random_array == NULL) {
        perror("Memory allocation failed");
        exit(EXIT_FAILURE);
    }

    for (size_t i = 0; i < length; i++) {
        random_array[i] = (unsigned char)('a' + rand() % 24);
    }

    int len2 = uclen(random_array);
    if (boop ==1 && len2 > 250) {
        perror("Generated random array length exceeds maximum allowed length");
        exit(EXIT_FAILURE);
    }

    return random_array;
}

int main() {
    char* ttable_name = "40e00ac5-48ff-40f2-882b-d717dfd736ee";
    size_t id_size = 250;
    size_t block_size = 262144;
    int blocks_per_sstable = 2;
    size_t record_size = 8306;
    sstable_t* sstable = init_sstable(
        ttable_name, 
        id_size,
        record_size,
        block_size, 
        blocks_per_sstable
    );
   
    flush_memtable_new(sstable);
    unsigned char* id = "xvPwXsfOi0hSjfgrElT53zakzCyYFGuH610iRm9L55CktDztQ6JyYPGF6uQQFR7Kz4md1Os7lyYqtgJXAOjQDrGkAfyZuVAcl9l2NsHJxn4hLiCFPCGUKXsALRbSC92EI907lViiXj4rRWBNeshnQjsQxgiDu6PTkOCUM8C8kYWmP6fwivhSj2ytnTdFSx6jv4BLzzzgvjFtPD2BBbHohEzWnp8gIYj0HQBoepkcyhAXJWGOggVboSdGAsQb5wu5lNfZRN7Jihi1PbbdAhcCleTIL8zS5X58p82JzCRzaf1LoNmrAbUptkgwx7dacYvTggxvrW8vmnuDdtVchUnGQNfLFkvdkLwEkietvaD2rtZ4lPxThwuLY6Fz8fMhC7INS6lTJj4DLPftE8B8vYClUmro3UMNv";
    entry_t* centry = sstable_get(id, sstable);
    return 0;

    int num_iters = 10;
    unsigned char *entry_bytes = NULL;

    for (int i = 0; i < num_iters; i++) {
        unsigned char *id = generate_random_uc_array(
            1, sstable->id_size, 1
        );
        unsigned char* id2 = memcpy(malloc(sstable->id_size), id, sstable->id_size);
        unsigned char* formatted_id = malloc(sstable->id_size);
        rpad(formatted_id, sstable->id_size, id);

        unsigned char* entry_bytes = generate_random_uc_array(
            1000, sstable->record_size, 0
        );
        entry_t* entry = init_entry(sstable, id, entry_bytes);

        memtable_append(sstable, entry);
        entry_t* fetched_entry = sstable_get(formatted_id, sstable);
        entry_t* bad_find_entry = sstable_get(entry->id, sstable);

        if (fetched_entry == NULL) {
            int a = 0;
        }

        int len = uclen(formatted_id);
        int len1 = uclen(entry->id);
        int len2 = uclen(id);
        int len3 = strlen(id);
        int diff = memcmp(formatted_id, entry->id, sstable->id_size);
        int diff2 = memcmp(formatted_id, entry->id, entry->raw_id_length);
        entry_t* entry2 = init_entry(sstable, id, entry_bytes);
        entry_t* why_entry = sstable_get(formatted_id, sstable);
        if (memcmp(fetched_entry->raw_bytes, entry_bytes, fetched_entry->raw_bytes_length) != 0) {
            perror("error: fetched bytes do not match inserted bytes\n");
            exit(1);
        }

        free(fetched_entry);
    }
    
    free(entry_bytes);
    flush_memtable_new(sstable);
    return 0;
}

sstable_t* create_sstable_instance(
    char* table_name,
    size_t id_size,
    size_t record_size,
    size_t block_size,
    int blocks_per_sstable
) {
    return init_sstable(
        table_name, 
        id_size,
        record_size,
        block_size, 
        blocks_per_sstable
    );
}

int put_entry(sstable_t* sstable, unsigned char* id, unsigned char* bytes) {
    entry_t* entry = init_entry(sstable, id, bytes);
    memtable_append(sstable, entry);
    entry_t* found_entry = memtable_get(entry->id, sstable);

    return (
        memcmp(found_entry->id, entry->id, sstable->id_size) == 0 &&
        memcmp(found_entry->bytes, entry->bytes, sstable->record_size) == 0
    );
}

entry_t* fetch_entry(sstable_t* sstable, unsigned char* id) {
    return sstable_get(id, sstable);
}

void flush_memtable(sstable_t* sstable) {
    flush_memtable_new(sstable);
    load_sstable_lookup_map(sstable);
    load_memtable_from_file(sstable);
}

void destroy_lookup_map(sstable_t* sstable) {
    sstable_lookup_t* current;
    sstable_lookup_t* tmp;

    HASH_ITER(hh, sstable->lookup_map, current, tmp) {
        HASH_DEL(sstable->lookup_map, current);
        free(current->id);
        free(current->address->sstable_file_name);
        free(current->address->hash_code);
        free(current->address);
        free(current);
    }
}

void destroy_memtable(sstable_t* sstable) {
    entry_t* current;
    entry_t* tmp;

    HASH_ITER(hh, sstable->memtable, current, tmp) {
        HASH_DEL(sstable->memtable, current);
        free(current->id);
        free(current->bytes);
        free(current);
    }
}

void destroy_sstable_instance(sstable_t* sstable) {
    destroy_lookup_map(sstable);
    destroy_memtable(sstable);

    free(sstable->sstable_dir_path);
    free(sstable->memtable_file_path);
    free(sstable->table_name);
    free(sstable);
}

void reload_sstable_instance(sstable_t* sstable) {
    destroy_lookup_map(sstable);
    destroy_memtable(sstable);

    load_sstable_lookup_map(sstable);
    load_memtable_from_file(sstable);
}


