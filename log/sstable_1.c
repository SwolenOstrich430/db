/*
* ===== TYPE DEFS
* init_table
*   - name
*   - table_file_path (where data will be stored)
*   - sstable_size (4kb)
*   - sstable_paths (where each sstable will be stored)
*   - sstable_lookup_map (sorted map of key-sstable_path)
* 
* init_record
*   - key (id)
*   - bytes (unsigned char*: data to be stored in related file)
* 
* init_sstable_lookup
*   - key (id)
*   - sstable_address
* 
* ===== DATA HANDLING
* char* get_sstable_for_key(unsigned char* key)
*   - returns path to the sstable if key exists
*   - returns NULL if not exists 
* 
* int get_address_from_sstable(unsigned char* key, FILE* file_ptr)
*   - function: 
*       - loop through 
*   - returns int address of corresponding record in table file if key exists
*   - returns -1 if key doens't exist 
* 
* char* get_from_data_file
* 
* 
* 
* ===== FILE HANDLING
* get_table_file_path -> log/data/<table_name>/<table_name>.table
* get_sstable_file -> log/data/<table_name>/sstable/<new_file_name>
* get_sstable_dir -> log/data/<table_name>/sstable/
* get_data_dir    -> log/data/<table_name>
*/