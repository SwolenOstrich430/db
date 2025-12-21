# Log-Structured Storage 
* append-only files 

## Implementations 

### Hash Map 
* in-memory Hash Map 
    - keys: ids of objects 
    - values: byte offset 
        - where record starts in file 
* [example](./log_db_hash_map.c)
