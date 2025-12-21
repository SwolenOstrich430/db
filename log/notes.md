# Log-Structured Storage 
* append-only files 

## Implementations 

### Hash Map 
* in-memory Hash Map 
    - keys: ids of objects 
    - values: byte offset 
        - where record starts in file 
* [example](./log_db_hash_map.c)

### Log-Structured Storage (SSTable)
* files of 4kb
    * list of sorted key-val pairs
    * where:
        * keys: ids of objects
        * values: byte offset in table file
    * basically the #"Hash Map" Section and puts it into files
* in-memory sorted Map
    * keys: the first entry in each SSTable
    * values: the corresponding SSTable file
* [example](./sstable.c) 
