/*
* ===== FLOW 
* 1. Create File Structure
*   - table dir 
*   - sstable dir
*   - data file
*   - first sstable file
*   - memtable file 
* 2. Initialize sstable 
*   - block size (4kb) 
*   - max file size (e.g., 64kb) 
*   - record_address_lookup map 
*   - memtable (in-memory hash map)
*   - max memtable size (16kb)
*   - create all necessary directories and files (see Create File Structure)
* 3. Initialize table  
*   - name
*   - id size 
*   - record size  
*   - data file path
*   - sstable
*   - create all necessary directories and files (see Create File Structure)
* 4. Load SSTable Lookup Map 
* 5. Load Memtable 
* 6. Set Record
*   - validate id size 
*   - validate record size 
*   - append record to table file 
*       - return address of record in table file
*   - add id and address to memtable
*      - add id and address to memtable file 
*   - if memtable size + new record size > max memtable size
*       - flush memtable to new sstable file (see Flush Memtable)
*           - in the future this will be handled on a seperate thread 
*           - but for now, it'll block 
*       - reset memtable
*       - reset memtable file 
*   - validate record was added to memtable 
*   - return -1 on failure, 0 on success 
* 7. Get Record
*   - validate id size
*   - check memtable for id
*       - if exists, get record address from memtable
*       - else, check sstable lookup map for id
*           - if exists, get sstable file path from lookup map
*           - get record address from sstable file
*               - else, return error (record not found)
*   - get record from table file using record address
*      - else, return error (record not found)
*   - return record bytes
* ===== Future Async Jobs 
* ## Load SSTable Lookup Map 
* - For each file with format <SSTABLE_FILE_FORMAT> 
*   - in directory: `./tables/<table_name>/sstable/`
*       - validate block sizes  
*       - for each block 
*           - if <CONFIG::VALIDATE_SSTABLE_LOOKUP_ON_LOAD>
*               - validate lookup entries in block
*                   - if invalid, throw error and stop loading sstable lookup map
*           - create <SSTABLE_LOOKUP_T> from: 
*               - sstable_file_name 
*               - block_number 
*               - first id in block 
*          - add to sstable lookup map
* ## Load Memtable 
* - open memtable file: `./tables/<table_name>/memtable/<table_name>.memtable`
* - ensure that `sstable->memtable` is initialized and empty
* - for each entry in memtable file
*     - validate entry
*         - if invalid, throw error and stop loading memtable
*     - add entry to in-memory memtable
* ## Flush Memtable 
* - find overlap with memtable and sstable lookups 
*   - overlap: 
*       - memtable start index 
*       - memtable end index 
*       - where memtable[end_index][key] < sstable[file_path:block_address][key]
* - create a new version of the sstable file 
* - create copy of current file 
* - create append cursor for new version of file 
* - create read cursor for current file copy
* - get the size of a sstable_lookup_t
* - create variable to store current index in memtable overlap (memtable_index)
* - create variable to store total_bytes_read from file
* - create variable to store last_position_written in file
* - while bytes read from file == size of sstable_lookup_t
*   - if sstable_lookup_t[key] == memtable[memtable_index]
*       - write tmp_old_file[last_position_written:total_bytes_read - sstable_lookup_t_size] to new version of file 
*       - write memtable[memtable_index] to new version of file 
*       - memtable_index++
*       - last_position_written = total_bytes_read
*   - if sstable_lookup_t:key > memtable[memtable_index]
*       - write tmp_old_file[last_position_written:total_bytes_read - sstable_lookup_t_size] to new version of file 
*       - write memtable[memtable_index] to new version of file 
*       - memtable_index++
*       - last_position_written = total_bytes_read - sstable_lookup_t_size
* - if last_position_written < total_bytes_read 
*   - write tmp_old_file[last_position_written:total_bytes_read] to new version of file 
* - while memtable_index < len(memtable) 
*   - write memtable[memtable_index] to new version of file 
*   - memtable_index++
*   - TODO: how to handle failure 
*   - TODO: need to handle overflows in sstables 
*   - TODO: need to handle blocks in files 
*       - create an iterator for the sstable directory 
*       - create an iterator for an sstable file 
* - validate that the size of the new version of memtable equals 
*   - TODO: add tracking for number of new keys vs. number of old keys set 
*   - the size of the old version + (num_num_keys * sstable_lookup_t_size)
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
