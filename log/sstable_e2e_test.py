'''
1. Create x (num_sstables) sstable with n (num_entries) entries each 
2. For each sstables 
    a. Create the instance 
    b. Verify that the instance was created successfully 
    c. For each future entry
        i. Fetch the entry before insert 
        ii. Verify that the entry was not found 
        iii. Insert the entry 
        iv. Fetch the entry after insert 
        v. Verify that the entry was found and matches the inserted entry
        vi. Update the entry with new value 
        vii. Fetch the entry after update
        viii. Verify that the entry was found and matches the updated entry
    d. Flush the memtable to disk
    e. For each entry that was verified in step c
        i. Fetch the entry after flush
        ii. Verify that the entry was found and matches the inserted entry  
3. Shut down the instance
4. Restart the instance 
5. Repeat step 2 n times (num_runs) for each sstable to verify that the entries are still found after restart
'''

'''
interface sstable_instance {
    create_sstable_instance() -> sstable
    put_entry(sstable_instance, entry) -> int
    fetch_entry(sstable_instance, key) -> entry
    flush_memtable(sstable_instance) -> void
    destroy_sstable_instance(sstable_instance) -> void
    reload_sstable_instance(sstable_instance) -> void
}
'''

import os
import random
from typing import Set
import uuid
import ctypes
import string
import shutil


class SSTableTestConfig():
    def __init__(
        self, 
        id_size,
        value_size,
        num_blocks,
        block_size,
        sstable_name
    ):
        self.sstable_name = sstable_name
        self.entries = []
        self.id_size = id_size
        self.value_size = value_size 
        self.block_size = block_size
        self.num_blocks = num_blocks
        self.sstable_name = sstable_name
        self.sstable = None
        self.num_flushes = 0

    def __hash__(self):
        return hash(self.sstable_name)

    def __eq__(self, other):
        if not isinstance(other, SSTableTestConfig):
            return NotImplemented
        
        return self.sstable_name == other.sstable_name

class TestEntryLookup():
    def __init__(
        self, 
        key: bytes, 
        value: bytes
    ):
        c_ubyte_p = ctypes.POINTER(ctypes.c_ubyte)

        if type(key) == str:
            key = key.encode('utf-8')
        if type(value) == str:
            value = value.encode('utf-8')

        self.key = key
        self.value = value
        self.formatted_key = ctypes.cast(key, c_ubyte_p)
        self.formatted_value = ctypes.cast(value, c_ubyte_p)
        self.num_flushes = 0

def random_uuid():
    my_uuid_object = uuid.uuid4()
    return str(my_uuid_object)

def get_random_string_of_random_length(min_len, max_len):
    length = random.randint(min_len, max_len) #
    characters = string.ascii_letters + string.digits
    return ''.join(random.choices(characters, k=length))

def sstable_public():
    c_ubyte_p = ctypes.POINTER(ctypes.c_ubyte)
    script_directory = os.path.dirname(os.path.abspath(__file__))
    lib = ctypes.CDLL(f"{script_directory}/sstable_with_memtable_impl1.so")

    lib.create_sstable_instance.argtypes = [ctypes.c_char_p, ctypes.c_size_t, ctypes.c_size_t, ctypes.c_size_t, ctypes.c_int]
    lib.create_sstable_instance.restype = ctypes.c_void_p

    lib.put_entry.argtypes = [ctypes.c_void_p, c_ubyte_p, c_ubyte_p]
    lib.put_entry.restype = ctypes.c_int

    lib.fetch_entry.argtypes = [ctypes.c_void_p ,c_ubyte_p]
    lib.fetch_entry.restype = ctypes.c_void_p

    lib.flush_memtable.argtypes = [ctypes.c_void_p]
    lib.flush_memtable.restype = None

    lib.destroy_sstable_instance.argtypes = [ctypes.c_void_p]
    lib.destroy_sstable_instance.restype = None

    lib.reload_sstable_instance.argtypes = [ctypes.c_void_p]
    lib.reload_sstable_instance.restype = None

    lib.sstable_get_table_name.argtypes = [ctypes.c_void_p]
    lib.sstable_get_table_name.restype = ctypes.c_void_p

    lib.sstable_get_memtable_file_path.argtypes = [ctypes.c_void_p]
    lib.sstable_get_memtable_file_path.restype = ctypes.c_char_p

    lib.sstable_get_sstable_dir_path.argtypes = [ctypes.c_void_p]
    lib.sstable_get_sstable_dir_path.restype = ctypes.c_char_p

    lib.sstable_get_block_size.argtypes = [ctypes.c_void_p]
    lib.sstable_get_block_size.restype = ctypes.c_size_t

    lib.sstable_get_display_block_size.argtypes = [ctypes.c_void_p]
    lib.sstable_get_display_block_size.restype = ctypes.c_size_t

    lib.sstable_get_entry_size.argtypes = [ctypes.c_void_p]
    lib.sstable_get_entry_size.restype = ctypes.c_size_t

    lib.sstable_get_max_size.argtypes = [ctypes.c_void_p]
    lib.sstable_get_max_size.restype = ctypes.c_size_t

    lib.sstable_get_record_size.argtypes = [ctypes.c_void_p]
    lib.sstable_get_record_size.restype = ctypes.c_size_t

    lib.sstable_get_id_size.argtypes = [ctypes.c_void_p]
    lib.sstable_get_id_size.restype = ctypes.c_size_t

    lib.sstable_get_num_blocks.argtypes = [ctypes.c_void_p]
    lib.sstable_get_num_blocks.restype = ctypes.c_int

    lib.entry_get_id.argtypes = [ctypes.c_void_p]
    lib.entry_get_id.restype = c_ubyte_p

    lib.entry_get_value.argtypes = [ctypes.c_void_p]
    lib.entry_get_value.restype = c_ubyte_p

    return lib

def setup():
    # set num sstables 
    # set num entries
    # set num runs
    pass 

def test_create_sstable_instance(sstable_public, test_config) -> ctypes.c_void_p:
    sstable = sstable_public.create_sstable_instance(
        test_config.sstable_name.encode('utf-8'),
        test_config.id_size, 
        test_config.value_size, 
        test_config.block_size, 
        test_config.num_blocks
    )

    set_key_size = sstable_public.sstable_get_id_size(sstable)
    assert set_key_size == test_config.id_size, f"Expected key size {test_config.id_size}, but got {set_key_size}"

    set_record_size = sstable_public.sstable_get_record_size(sstable)
    assert set_record_size == test_config.value_size, f"Expected record size {test_config.value_size}, but got {set_record_size}"

    set_entry_size = sstable_public.sstable_get_entry_size(sstable)
    assert set_entry_size == test_config.id_size + test_config.value_size + 1, f"Expected entry size {test_config.id_size + test_config.value_size}, but got {set_entry_size}"

    set_display_block_size = sstable_public.sstable_get_display_block_size(sstable)
    assert set_display_block_size == test_config.block_size, f"Expected display block size {test_config.block_size}, but got {set_display_block_size}"

    set_block_size = sstable_public.sstable_get_block_size(sstable)
    expected_block_size = int(test_config.block_size / set_entry_size) * set_entry_size
    assert set_block_size == expected_block_size, f"Expected block size {expected_block_size}, but got {set_block_size}"
    
    set_num_blocks = sstable_public.sstable_get_num_blocks(sstable)
    assert set_num_blocks == test_config.num_blocks, f"Expected number of blocks {test_config.num_blocks}, but got {set_num_blocks}"

    raw_ptr = sstable_public.sstable_get_table_name(sstable)
    set_table_name = ctypes.string_at(raw_ptr, len(test_config.sstable_name)).decode("utf-8")
    assert set_table_name == test_config.sstable_name, f"Expected table name {test_config.sstable_name}, but got {set_table_name}"

    return sstable

def test_insert_and_fetch_entries(sstable_public, sstable, num_entries):
   
    for i in range(num_entries + len(sstable.entries)):
        if (i < len(sstable.entries)):
            curr_entry = sstable.entries[i]
            print(f"curr entry: {curr_entry.key}, {curr_entry.value}")
            if curr_entry is None: 
                raise f"current entry cannot be null. Index: {i}"

            print("fetching entry before insert")
            fetch_result = sstable_public.fetch_entry(sstable.sstable, curr_entry.formatted_key)
            print(f"fetch result: {fetch_result}")
            assert fetch_result is not None, f"Expected to not find entry with key {curr_entry.key}, but it was found"
            
            print("211 getting id from entry")
            found_key = sstable_public.entry_get_id(fetch_result)
            found_key = ctypes.string_at(found_key)
            print(f"214 got id from entry: {found_key}")
            assert found_key == curr_entry.key, f"Expected to find entry with key {curr_entry.key}, but got key {found_key}"
            
            print("217 getting value from entry")
            found_value = sstable_public.entry_get_value(fetch_result)
            found_value = ctypes.string_at(found_value)
            print(f"220 got value from entry: {found_value}")
            assert found_value == curr_entry.value, f"Expected to find entry with value {curr_entry.value}, but got value {found_value}"
        
            curr_entry = TestEntryLookup(
                get_random_string_of_random_length(
                    15,
                    sstable_public.sstable_get_id_size(sstable.sstable)
                ),
                get_random_string_of_random_length(
                    100,
                    sstable_public.sstable_get_record_size(sstable.sstable)
                )
            )
            sstable.entries[i] = curr_entry
        else:
            curr_entry = TestEntryLookup(
                get_random_string_of_random_length(
                    15,
                    sstable_public.sstable_get_id_size(sstable.sstable)
                ),
                get_random_string_of_random_length(
                    1,
                    sstable_public.sstable_get_record_size(sstable.sstable)
                )
            )

            sstable.entries.append(curr_entry)

            print(f"curr entry 248: {curr_entry.key}, {curr_entry.value}")
            fetch_result = sstable_public.fetch_entry(sstable.sstable, curr_entry.formatted_key)
            print(f"fetch result 250: {fetch_result}")
            print(f"curr entry: {curr_entry.key}, {curr_entry.value}")
            assert fetch_result is None, f"Expected to not find entry with key {curr_entry.key}, but it was found"
        
        print(f"putting entry {curr_entry.key} | {curr_entry.value}")
        put_result = sstable_public.put_entry(sstable.sstable, curr_entry.formatted_key, curr_entry.formatted_value)
        print(f"put result: {put_result}")
        assert put_result == 1, f"Expected to successfully put entry with key {curr_entry.key}, but got error code {put_result}"

        print("259 fetching entry after insert")
        fetch_result = sstable_public.fetch_entry(sstable.sstable, curr_entry.formatted_key)
        print(f"261 fetched entry: {fetch_result}")
        assert fetch_result is not None, f"Expected to find entry with key {curr_entry.key}, but it was not found"
        
        print("264 getting id from entry")
        found_key = sstable_public.entry_get_id(fetch_result)
        found_key = ctypes.string_at(found_key)
        print(f"267 got id from entry: {found_key}")
        assert found_key == curr_entry.key, f"Expected to find entry with key {curr_entry.key}, but got key {found_key}"
        
        print("274 getting value from entry")
        found_value = sstable_public.entry_get_value(fetch_result)
        found_value = ctypes.string_at(found_value)
        print(f"277 got value from entry: {found_value}")
        assert found_value == curr_entry.value, f"Expected to find entry with value {curr_entry.value}, but got value {found_value}"
        
def test_flush_and_fetch_entries(sstable_public, sstable):
    print("flushing memtable==================================")
    shutil.copytree(
        f"{sstable_public.sstable_get_sstable_dir_path(sstable.sstable).decode('utf-8')}", 
        f"{sstable_public.sstable_get_sstable_dir_path(sstable.sstable).decode('utf-8')}_backup_{sstable.num_flushes}"
    )
    shutil.copytree(
        f"{sstable_public.sstable_get_sstable_dir_path(sstable.sstable).decode('utf-8')}/../memtable", 
        f"{sstable_public.sstable_get_sstable_dir_path(sstable.sstable).decode('utf-8')}/../memtable_backup_{sstable.num_flushes}"
    )
    sstable_public.flush_memtable(sstable.sstable)
    sstable.num_flushes += 1
    
    for entry in sstable.entries:
        fetch_result = sstable_public.fetch_entry(sstable.sstable, entry.formatted_key)
        if fetch_result is None:
            fetch_result = sstable_public.fetch_entry(sstable.sstable, entry.formatted_key)

        print(f"284 looking for: {entry.key}, {entry.value}")
        assert fetch_result is not None, f"Expected to find entry with key {entry.key}, but it was not found"
        
        found_key = sstable_public.entry_get_id(fetch_result)
        found_key = ctypes.string_at(found_key)
        assert found_key == entry.key, f"Expected to find entry with key {entry.key}, but got key {found_key}"
        
        found_value = sstable_public.entry_get_value(fetch_result)
        found_value = ctypes.string_at(found_value)
        assert found_value == entry.value, f"Expected to find entry with value {entry.value}, but got value {found_value}"

def generate_sstables(num_sstables):
    sstables = []

    for i in range(num_sstables):
        sstables.append(random_uuid())

    return sstables 

def run_tests(
    num_sstables,
    num_entries,
    num_runs    
):
    sstable_names = generate_sstables(num_sstables)
    sstab_lib = sstable_public()
    sstable_tests = {}
    for _ in range(num_runs):
        for sstable_name in sstable_names:
            curr_config = sstable_tests.get(sstable_name)

            if curr_config is None:
                curr_config = SSTableTestConfig(
                    random.randint(10, 1000),
                    random.randint(10, 10000),
                    4,
                    int(1024**2 / 4),
                    sstable_name
                )

                curr_config.sstable = test_create_sstable_instance(
                    sstab_lib, curr_config
                )

                sstable_tests[sstable_name] = curr_config

            test_insert_and_fetch_entries(sstab_lib, curr_config, num_entries)
            test_flush_and_fetch_entries(sstab_lib, curr_config)

run_tests(
    num_sstables=1,
    num_entries=10,
    num_runs=5
)