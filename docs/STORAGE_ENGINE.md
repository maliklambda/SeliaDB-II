# Storage Engine Documentation

## Table of Contents

- [Overview](#overview)
- [Storage](#storage)
- [Indexing](#indexing)
- [Future Topics](#additional-and-planned-topics)

## Overview


## Storage
### Different file types
The heart of SeliaDB's storage are the table files. 
They can be divided into three parts:
1. Table Metadata: <br>
    Table metadata includes number_of_columns (4B), start_entries (4B)
2. Columns: <br>
    All columns are stored sequentially with their metadata (type, size, etc.). 
    At runtime, they are read as instances of the struct Column_t.
3. Entries: <br>
    From the offset "start_entries" onwards are the actual entries stored. 
    Each entry stores the values for each column in sequential order. 
    A detailed explanaition on how entries are stored can be found [here](#buffering--entry-chains).

Other essential files include index-files, database-files and the selia_init file.

### Fixed Size vs. Variable Length
The storage engine supports both fixed size-types (such as I32, BOOL, etc.) and types that are naturally variable in their size (e.g. VARCHAR). 
For both types that are fixed in size and those that are not, the column-metadata contains the variable "size" (which is a field of the struct "Column_t")
For fix-sized types, this does what it sounds like: it gives the size of the type (e.g. 4 Bytes for I32). 
This makes traversing really quick, since it is known how many bytes are stored per entry for the column.
For types of variable size, the column-size describes the maximum length that a value of this column can be (NOT including the NULL-terminator which signalizes the end of a string).
If a column is initialized as VARCHAR(255), 256 bytes is the maximum length of an attribute value for this column (255B data + 1B NULL-terminator).
The minimum size is 1B (only the NULL-terminator).


### Buffering & Entry-Chains
In order to allow for flexibility and changes to existing data, the storage-engine makes use of buffering and chaining entries. 
After each entry, a buffer of a certain size is placed. 
The implementation and concrete size of the buffer is in types.GetTableDataBuffer(). 
When the length of an entry changes, the buffer can help prevent unnecessary complexity caused by reordering of offsets that point to the entry that follows.
When the length exceeds the buffer, the buffer is increased (multiplied by the buffer-size) until the entry fits or the EOF is reached.
If thereafter the entry is modified in a way that makes it shorter, the buffer stays the same size because the entry might grow in size again.
If the size of an entry is increased, this means that all B-trees need to update the offsets to the following entries. 
Since this is costly, there are mechanisms (which are described in the [section on Indexing](#indexing)) put into place to handle this and to make this less of a performance-swallower. 
Since the buffer is of variable length, two Bytes (unsigned 16 bit int) are appended directly at the end of each entry. They point to the start of the next entry.
This results is a structure that resembles a linked list.
While traversing with variable entry-length can hardly be made as performant as with fixed-sizes, this makes the resulting performance of traversals acceptable.


## Indexing
SeliaDB's indexing-system is based on B-trees. 
Since the database in it's entirety aims to be as fast as possible, while handeling frequent changes to the data it stores, the indexing-system is arranged accordingly.
It works as follows: 
Each column has the info, whether it is indexed or not, stored in the main file on table-level (where also the column-names and types are stored).
On startup, the indexes for each column, as well as the columns themselves, are read into memory, which allows for extremely efficent index-lookup.
Since it is costly for the index to be in memory - and not on disk and then read partly into memory, as does e.g. SQLite - the index-system is optimized to consume as little memory as possible.
<br>
Any destructive operation (INSERT, DELETE, UPDATE) changes the tree in memory.
To make the index storage persistant, the changed B-tree is written onto the disk at latest when the system is turned off.
This approach allows for a logarithmic search time without any IO-operations, while still allowing persistant index-storage.
For this reason, frequen changes to the data are far less costly than with indexing-techniques that are primarily stored on disk (since the entire tree is kept in memory).
The tradeoff is that the number of indexed columns that can be held simoultaneously is directly limited by the amount of RAM available, which can cause issues on large-scale systems.
For indexes that are AUTO-INCREMENT, the implementation of Right-most-append is planned.


## Additional And Planned Topics
These are topics that are planned for the future.
- Caching strategies
- Concurrency control
- transaction isolation
- Data compaction/cleanup processes
