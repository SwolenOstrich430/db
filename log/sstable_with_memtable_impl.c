/**
 * TODO: fix: casting unsigned char to char for str-util methods 
 * TODO: fix: mkdir_force memory allocation
 * TODO: fix: stop making sstable files if one isn't full
 * TODO: fix: add validation that an entry was able to be prased in 'parse_sstable_file_entry'
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
size_t MIN_BLOCK_SIZE = 512;
size_t SSTABLE_ADDRESS_HASH_SIZE = 20;

typedef struct {
    unsigned char* sstable_file_name;
    int block_number;
    unsigned char* hash_code;
} sstable_address_t;

typedef struct {
    unsigned char* id;
    sstable_address_t* address;
    UT_hash_handle hh;
} sstable_lookup_t;

typedef struct {
    unsigned char** ids;
    size_t ids_length;
    size_t ids_capacity;
    sstable_address_t* address;
    unsigned char* id;
    UT_hash_handle hh;
} future_sstable_lookup_t;

typedef struct {
    unsigned char* id;
    unsigned char* address;
    unsigned char* raw_id;
    int raw_address;
    UT_hash_handle hh;
} id_address_lookup_t;

typedef struct {
    char* table_name;
    size_t id_size;
    size_t block_size;
    size_t max_size;
    int num_blocks;
    id_address_lookup_t* memtable;
    sstable_lookup_t* lookup_map;
    char* sstable_dir_path;
    char* memtable_file_path;
    size_t sstable_address_hash_size;
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

int get_id_address_entry_size(size_t id_size) {
    return (
        id_size + 
        strlen(SSTABLE_LOOKUP_SEPERATOR) + 
        sizeof(int)
    );
}

void rpad(unsigned char* buffer, size_t size, char *str) {
    snprintf((char*)buffer, size, "%s", str);
    
    int str_len = strlen((char*)buffer);
    if (str_len >= size) return;

    for (int i = str_len; i < size; i++) {
        buffer[i] = ' ';
    }
}

// ========== FUTURE UTILS 

// ========== LOAD LOOKUPS 


// void set_sstable_lookup_map(
//     sstable_t* sstable, 
//     char* table_name
// ) {
//     struct dirent *entry;
//     int num_files = 0;
//     int curr_block = 0;
//     size_t file_size = 0;
//     DIR *dir = opendir(sstable->sstable_dir_path);

//     size_t block_size = sstable->block_size;
//     size_t entry_size = sstable->id_size + sizeof(int) + sizeof(SSTABLE_LOOKUP_SEPERATOR);
//     unsigned char* curr_lookup_entry = malloc(entry_size);
//     char sstable_file_path[MAX_PATH_SIZE];
//     size_t bytes_read;
//     FILE* file_ptr;

//     while ((entry = readdir(dir)) != NULL) {
//         if (entry->d_type != DT_REG) continue;

//         snprintf(
//             sstable_file_path, 
//             MAX_PATH_SIZE,
//             "%s/%s", 
//             sstable->sstable_dir_path, 
//             entry->d_name
//         );           
        
//         block_size = 0;
//         file_size = get_file_size(sstable_file_path);
//         file_ptr = fopen(sstable_file_path, "rb");

//         while (sstable->block_size * curr_block < file_size) {
//             if ((bytes_read = fread(curr_lookup_entry, 1, entry_size, file_ptr)) == entry_size) {
//                 sstable_lookup_t* sstable_lookup = malloc(sizeof(sstable_lookup_t));
//                 id_address_lookup_t* address_lookup = parse_sstable_file_entry(curr_lookup_entry, sstable->id_size);

//                 sstable_lookup->id = malloc(sstable->id_size);
//                 sstable_lookup->id = address_lookup->id;
//                 sstable_lookup->block_number = curr_block;
//                 sstable_lookup->file_name = strdup(entry->d_name);
//                 unsigned char* id = sstable_lookup->id;

//                 HASH_ADD(
//                     hh,
//                     sstable->lookup_map, 
//                     id, 
//                     sstable->id_size, 
//                     sstable_lookup
//                 );

//                 block_size++;
//             } else {
//                 break;
//             }
//         }
//     }

//     free(curr_lookup_entry);
//     closedir(dir);

//     return;
// }

// ========== LOAD LOOKUPS 


// ========== TYPES 

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

id_address_lookup_t* init_id_address_lookup(
    size_t id_size,
    unsigned char* id,
    int address
) {
    id_address_lookup_t* lookup = malloc(sizeof(id_address_lookup_t));
    lookup->id = malloc(id_size);
    rpad((char*)lookup->id, id_size, (char*)id);
    lookup->raw_id = id;

    lookup->address = malloc(sizeof(address));
    snprintf((char*)lookup->address, sizeof(address), "%d", address); 
    rpad((char*)lookup->address, sizeof(lookup->address), (char*)lookup->address);
    lookup->raw_address = address;

    return lookup;
}

id_address_lookup_t* parse_sstable_file_entry(
    unsigned char* entry,
    size_t key_size
) {
    return init_id_address_lookup(
        key_size,
        strtok(entry, SSTABLE_LOOKUP_SEPERATOR), 
        atoi(strtok(NULL, SSTABLE_LOOKUP_SEPERATOR))
    );
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
    size_t entry_size = get_id_address_entry_size(sstable->id_size);
    FILE* pfile = fopen(file_path, "rb");
    char* curr_entry = malloc(entry_size);
    unsigned char* id = NULL;
    int bytes_read;
    unsigned char* prev_id = NULL;

    while ((bytes_read = fread(curr_entry, 1, entry_size, pfile)) == entry_size) {
        id_address_lookup_t* lookup = parse_sstable_file_entry(curr_entry, sstable->id_size);
        id_address_lookup_t* found_lookup = NULL;

        HASH_FIND(
            hh,
            sstable->memtable,
            lookup->id,
            sstable->id_size,
            found_lookup
        );

        if (found_lookup == NULL) {
            HASH_ADD_KEYPTR(
                hh, 
                sstable->memtable,
                lookup->id,
                sstable->id_size,
                lookup
            );
        }

        if (prev_id != NULL && strcmp(prev_id, lookup->id) == 0) {
            int i = 0;
        }

        prev_id = lookup->id;
    }

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

unsigned char* sstable_address_to_hash(
    sstable_address_t* address, 
    size_t hash_size
) {
    if (address == NULL || address->sstable_file_name == NULL) {
        perror("Provided 'addres' or 'address->sstable_file_name' was null");
        exit(1);
    }
    
    size_t val_size = strlen(address->sstable_file_name) + sizeof(int);
    char val_to_hash[val_size];
    char* addr_hash = malloc(
        strlen(address->sstable_file_name) + sizeof(int)
    );
    
    snprintf(
        val_to_hash, 
        sizeof(val_size), 
        "%s%d", 
        address->sstable_file_name, 
        address->block_number
    );

    unsigned long hash_as_long = djb2_hash(val_to_hash);
    memcpy(addr_hash, &hash_as_long, hash_size);
    rpad(addr_hash, hash_size, addr_hash);

    return addr_hash;
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

    return lookup;
}

int get_sstable_lookup_entry_size(sstable_t* sstable) {
    return (
        sstable->id_size + 
        sizeof(int) + 
        sizeof(SSTABLE_LOOKUP_SEPERATOR)
    );
}

// for each file in the sstable dir 
// for each block in the current file 
// create an sstable_address_t 
//      - with file_path 
//      - and the current block number 
// create a variable to hold the current id
// read sstable->id_size bytes into the current_id variable  
// create an sstable_lookup_t
//      - set id to current_id
//      - set address to the sstable_address_t
// add the sstable_lookup_t to the sstable->lookup_map:
//      - key: id
//      - value: the sstable_lookup_t
void load_sstable_lookup_map_from_file(sstable_t* sstable) {
    if (sstable == NULL) {
        perror("Provided sstable cannot be null");
        exit(1);
    }

    if (sstable->block_size == NULL || sstable->block_size < MIN_BLOCK_SIZE) {
        perror("Provided sstable has an invalid block size");
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
    char* curr_file = malloc(MAX_PATH_SIZE);
    FILE* pfile;
    int entry_size = get_id_address_entry_size(sstable->id_size);

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

        pfile = fopen(curr_file, "rb");
        unsigned char* entry = malloc(entry_size);
        block_number = 0;

        while ((bytes_read = fread(entry, 1, entry_size, pfile)) == entry_size) {
            id_address_lookup_t* lookup = parse_sstable_file_entry(entry, sstable->id_size);

            if (lookup == NULL) {
                perror("error: unable to parse sstable lookup entry from file");
                exit(1);
            }
            
            sstable_lookup_t* address = init_sstable_lookup(
                sstable,
                lookup->id,
                sstable_file->d_name,
                block_number
            );

            HASH_ADD_KEYPTR(hh, sstable->lookup_map, lookup->id, sstable->id_size, address);

            // TODO: double check that this gets us to the next block correctly
            fseek(pfile, sstable->block_size + 1, SEEK_SET);
            block_number++;

            if (block_number > sstable->num_blocks) {
                perror("Current sstbale file has too many blocks");
                exit(1);
            }

            if (ftell(pfile) > sstable->max_size) {
                perror("Current sstable file is greater than max size");
                exit(1);
            }
        }
    }

}

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
    sstable->num_blocks = num_blocks;
    // TODO: add this as a config option in sstbale instead of just as constant 
    sstable->sstable_address_hash_size = SSTABLE_ADDRESS_HASH_SIZE;
    sstable->max_size = sstable->block_size * sstable->num_blocks;

    sstable->sstable_dir_path = create_sstable_dir(table_name);
    // create_sstable_file(table_name);
    load_sstable_lookup_map_from_file(sstable);

    sstable->memtable = NULL;
    load_memtable_from_file(sstable);

    return sstable;
}

// ========== TYPES 

// ========== PROTECTED 

void memtable_hash_set(sstable_t* sstable, id_address_lookup_t* lookup) {
    unsigned char* id = lookup->id;
    HASH_ADD_KEYPTR(
        hh,
        sstable->memtable,
        id,
        sstable->id_size,
        lookup
    );

    id_address_lookup_t* stored_lookup; 
    HASH_FIND(
        hh,
        sstable->memtable,
        lookup->id,
        sstable->id_size,
        stored_lookup
    );

    if (id_address_lookup_cmp(lookup, stored_lookup) != 0) {
        perror("error: failed to add id_address lookup to memtable");
        exit(1);
    }
}

char* id_address_to_file_entry(
    sstable_t* sstable, 
    id_address_lookup_t* lookup
) {
    int entry_size = get_id_address_entry_size(sstable->id_size);
    char* file_entry = malloc(entry_size);

    snprintf(
        file_entry,
        entry_size,
        "%s%s%s",
        lookup->id,
        SSTABLE_LOOKUP_SEPERATOR,
        lookup->address
    );

    return file_entry;
}

void memtable_file_append(sstable_t* sstable, id_address_lookup_t* lookup) {
    FILE* file_ptr = fopen(sstable->memtable_file_path, "ab");
    if (file_ptr == NULL) {
        perror("Unable to open memtable file.");
        exit(1);
    }

    char* id_addr_entry = id_address_to_file_entry(sstable, lookup);
    int address = ftell(file_ptr);
    size_t entry_size = get_id_address_entry_size(sstable->id_size);

    fwrite(id_addr_entry, 1, entry_size, file_ptr);
    fclose(file_ptr);
    free(id_addr_entry);

    return;
}

void memtable_append(sstable_t* sstable, id_address_lookup_t* lookup) {
    memtable_hash_set(sstable, lookup);
    memtable_file_append(sstable, lookup);
}

// ========== PROTECTED

int cmp_sstable_lookup(
    const sstable_lookup_t* lookup_1, 
    const sstable_lookup_t* lookup_2
) {
    return strcmp(lookup_1->id, lookup_2->id);
}

int cmp_id_address_lookup(
    const id_address_lookup_t* lookup_1, 
    const id_address_lookup_t* lookup_2
) {
    return strcmp((char*)lookup_1->id, (char*)lookup_2->id);
}

 future_sstable_lookup_t* init_future_sstable_lookup_t(
    sstable_t* sstable,
    char* sstable_file,
    int block_number,
    unsigned char* entry_id
 ) {
    if (block_number < 0) {
        perror("Provided 'block_number' cannot be less than 0");
        exit(1);
    }

    future_sstable_lookup_t* future_lookup = malloc(
        sizeof(future_sstable_lookup_t)
    );
    future_lookup->ids_length = 1;
    // TODO: move this into a param
    future_lookup->ids_capacity = 10;
    
    future_lookup->ids = malloc(
        future_lookup->ids_capacity * sizeof(unsigned char*)
    );
    future_lookup->ids[future_lookup->ids_length - 1] = malloc(
        sstable->id_size
    );
    memcpy(
        future_lookup->ids[future_lookup->ids_length - 1], 
        entry_id,
        sstable->id_size
    );

    if (sstable_file == NULL) {
        future_lookup->address = init_sstable_address(
            sstable,
            create_sstable_file(sstable->table_name),
            block_number 
        );
    } else {
        future_lookup->address = init_sstable_address( 
            sstable,
            sstable_file,
            block_number
        );
    }

    future_lookup->id = future_lookup->address->hash_code;


    return future_lookup;
}

char* get_file_extension(char* file_path) {
    const char *dot = strrchr(file_path, '.');
    if (!dot || dot == file_path) return NULL; 
    const char *slash = strrchr(file_path, '/');

    if (slash == NULL || dot > slash) {
        return dot;
    } 

    return NULL;
}

char* get_temp_sstable_file(sstable_t* sstable, char* file_path) {
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

    snprintf(
        final_file,
        MAX_PATH_SIZE,
        "%s/%s%s%s",
        sstable->sstable_dir_path,
        basename_no_ext,
        "_temp",
        get_file_extension(file_path)
    );

    return final_file;
}

int file_exists(char* file_path) {
    struct stat st;
    return (stat(file_path, &st) != 0) ? 0 : 1;
}

void future_sstable_lookup_append(
    sstable_t* sstable,
    future_sstable_lookup_t* future_lookup,
    unsigned char* entry_id
) {
    future_lookup->ids_length += 1;

    if (future_lookup->ids_length > future_lookup->ids_capacity) {
        future_lookup->ids_capacity *= 2;
        unsigned char** temp = realloc(
            future_lookup->ids,
            (future_lookup->ids_capacity * sizeof(unsigned char*))
        );
        future_lookup->ids = temp;
    }

    future_lookup->ids[future_lookup->ids_length - 1] = malloc(
        sstable->id_size 
    );

    memcpy(
        future_lookup->ids[future_lookup->ids_length - 1],
        entry_id,
        sstable->id_size
    );

    return;
}


// TODO: 
// 1. handle block overflow when writing new entries
// 2. reset the lookup map after flushing memtable
// 3. reset the memtable after flushing 
// 4. make it so that later entries don't automatically get added to new sstable  
void flush_memtable(sstable_t* sstable) {
    load_memtable_from_file(sstable);
    HASH_SORT(sstable->memtable, cmp_id_address_lookup);
    HASH_SORT(sstable->lookup_map, cmp_sstable_lookup);

    int diff;
    int tmp_entry_diff;
    future_sstable_lookup_t* future_lookups = NULL;
    future_sstable_lookup_t* tmp_fut_lookup = NULL;
    id_address_lookup_t* curr_memtable_entry = sstable->memtable;
    sstable_lookup_t* curr_sstable_lookup = sstable->lookup_map;

    while (curr_memtable_entry != NULL) {
        if (curr_sstable_lookup != NULL) {
            diff = strcmp(
                (const char*)curr_memtable_entry->id, 
                (const char*)curr_sstable_lookup->id
            );
        } 
        
        // e.g., memtable entry = "id3", sstable lookup = "id5"
        if (curr_sstable_lookup != NULL && diff > 0) {
            // reset the tmp lookup since the next entries will need to go to a different sstable 
            curr_sstable_lookup = curr_sstable_lookup->hh.next;
            tmp_fut_lookup = NULL;
            continue;
        }

        // otherwise, this memtable entry belongs in the current sstable 
        // or a new one if we don't have tmp instantiated yet 
        if (tmp_fut_lookup != NULL) {
            future_sstable_lookup_append(
                sstable,
                tmp_fut_lookup,
                curr_memtable_entry->id
            ); 
        } else if (curr_sstable_lookup == NULL) {
            tmp_fut_lookup = init_future_sstable_lookup_t(
                sstable,
                get_next_sstable_file_name(sstable->table_name),
                0,
                curr_memtable_entry->id
            );

            HASH_ADD_KEYPTR(
                hh,
                future_lookups,
                tmp_fut_lookup->id,
                sstable->sstable_address_hash_size,
                tmp_fut_lookup
            );
        } else {
            tmp_fut_lookup = init_future_sstable_lookup_t(
                sstable,
                curr_sstable_lookup->address->sstable_file_name,
                curr_sstable_lookup->address->block_number,
                curr_memtable_entry->id
            );

            HASH_ADD_KEYPTR(
                hh,
                future_lookups,
                tmp_fut_lookup->id,
                sstable->sstable_address_hash_size,
                tmp_fut_lookup
            );

            curr_sstable_lookup = curr_sstable_lookup->hh.next;
        }
        
        curr_memtable_entry = (id_address_lookup_t*)curr_memtable_entry->hh.next;
    }

    future_sstable_lookup_t* curr_lu = malloc(sizeof(future_sstable_lookup_t));
    future_sstable_lookup_t* tmp = malloc(sizeof(future_sstable_lookup_t));
    FILE* curr_sstable;
    FILE* temp_sstable;

    HASH_ITER(hh, future_lookups, curr_lu, tmp) {
        if (file_exists(curr_lu->address->sstable_file_name)) {
            curr_sstable = fopen(curr_lu->address->sstable_file_name, "rb+");
            fseek(
                curr_sstable, 
                sstable->block_size * curr_lu->address->block_number,
                SEEK_SET
            );
        }

        char* temp_sstable_file = get_temp_sstable_file(sstable, curr_lu->address->sstable_file_name);
        temp_sstable = fopen(temp_sstable_file, "wb+");
        unsigned char* curr_id = malloc(sstable->id_size);
        id_address_lookup_t* parsed_lookup = NULL;
        int bytes_read = 0;
        int total_bytes_read = 0;

        for (int i = 0; i < curr_lu->ids_length; i++) {
            size_t entry_size = get_id_address_entry_size(sstable->id_size);
            char* entry = malloc(entry_size);

            if (file_exists(curr_lu->address->sstable_file_name)) {
                while (
                    (bytes_read = fread(entry, 1, entry_size, curr_sstable)) == entry_size && 
                    total_bytes_read < sstable->block_size && 
                    (parsed_lookup = parse_sstable_file_entry(entry, sstable->id_size)) && 
                    parsed_lookup != NULL &&
                    parsed_lookup->id != NULL &&  
                    curr_lu != NULL && 
                    curr_lu->ids != NULL && 
                    (strcmp(curr_lu->ids[i], parsed_lookup->id)) > 0
                ) {
                    total_bytes_read += bytes_read;
                    fwrite(entry, entry_size, 1, temp_sstable);
                }
                
                // TODO: need to handle block overflow 
                total_bytes_read = 0;
            }
            // TODO: make sure that we're not reading partial before writing?
            unsigned char* curr_key = curr_lu->ids[i];
            id_address_lookup_t* curr_address = NULL;

            HASH_FIND(
                hh, sstable->memtable, curr_key, sstable->id_size, curr_address
            );

            if (curr_address == NULL) {
                perror("No matching entry for key");
                exit(1);
            }

            unsigned char* id_addr_entry = id_address_to_file_entry(sstable, curr_address);
            fwrite(id_addr_entry, entry_size, 1, temp_sstable);
            free(id_addr_entry);

            // If there's still ids left to compare for the memtable, keep comparing to what's in the file 
            if (i < curr_lu->ids_length - 1) {
                continue;
            }

            // If we're not done with entries in the current sstable, continue adding to the temp file
            // TODO: need to consider what happens if we need to resize the sstable  
            if (file_exists(curr_lu->address->sstable_file_name)) {
                while (
                    (bytes_read = fread(entry, 1, entry_size, curr_sstable)) == entry_size) {
                    total_bytes_read += bytes_read;
                    fwrite(entry, entry_size, 1, temp_sstable);
                }

                total_bytes_read = 0;
            }
        }

        fclose(temp_sstable);
        fclose(curr_sstable);
        // TODO: will need to do checks to see if anyone's still using that file 
        //       maybe better to move to an entirely new file 
        // TODO: overwrite the memtable file 
        rename(temp_sstable_file, curr_lu->address->sstable_file_name);
    }

    int i = 0;
}


int main() {
    char* table_name = "test_table";
    size_t id_size = 50;
    size_t block_size = 4096;
    int blocks_per_sstable = 4;
    size_t sstable_size = block_size * blocks_per_sstable;
    sstable_t* sstable = init_sstable(
        table_name, 
        id_size,
        block_size, 
        blocks_per_sstable
    );

    int num_iters = 10;
    uuid_t binuuid;
    unsigned char *uuid_str = malloc(id_size); 
    id_address_lookup_t* lookup;

    for (int i = 0; i < num_iters; i++) {
        uuid_generate_random(binuuid);
        uuid_unparse(binuuid, (char*)uuid_str); 
        lookup = init_id_address_lookup(sstable->id_size, uuid_str, i);

        memtable_append(sstable, lookup);
    }
    
    free(uuid_str);
    flush_memtable(sstable);
    return 0;
}

// SSTABLE 
// TABLE  
