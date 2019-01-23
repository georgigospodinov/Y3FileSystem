//#include "fs.h"
#include <uuid/uuid.h>
//#include <unqlite.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <time.h>
#include <fuse.h>
#include "unqlite.h"
#include "logging_macros.h"

#define MY_MAX_PATH FILENAME_MAX
#define MY_MAX_FILE_SIZE 4194304
// 4*1024*1024          (4*2^20 Bytes == 4MB)

extern void write_log(const char*, ...);

typedef struct _meta_data {
    off_t size;
    __nlink_t nlinks;
    time_t atime;   /* time of last access*/
    time_t mtime;   /* time of last modification */
    time_t ctime;   /* time of last change to meta-data (status) */
} meta_data;
#define META_DATA_SIZE (sizeof(meta_data))

void print_meta(meta_data md) {
    write_log("\t\tmeta data:\n");
    write_log("\t\tsize=%lld\n", md.size);
    write_log("\t\tnlinks=%lld\n", md.nlinks);
    write_log("\t\tmtime=%ld\n", md.mtime);
    write_log("\t\tctime=%ld\n", md.ctime);
}

meta_data create_meta_data() {
    meta_data md = {
            .size = 0,
            .nlinks = 1,
            .atime = time(0),
            .mtime = time(0),
            .ctime = time(0),
    };

    return md;
}

typedef struct _myfcb {
    char path[MY_MAX_PATH];
    uuid_t file_data_id;
    uuid_t data;

    uid_t uid;     /* user */
    gid_t gid;     /* group */
    mode_t mode;    /* protection */

} myfcb;

#define MYFCB_SIZE (sizeof(struct _myfcb))

void print_fcb(myfcb fcb) {
    write_log("\t\tFile Control Block:\n");
    write_log("\t\tpath:%s\n", fcb.path);

    write_log("\t\tfile_data_id:\"");
    for (int i = 0; i < sizeof(uuid_t); ++i) write_log("%c", fcb.file_data_id[i]);
    write_log("\"\n");

    write_log("\t\tdata:\"");
    for (int i = 0; i < sizeof(uuid_t); ++i) write_log("%c", fcb.data[i]);
    write_log("\"\n");

    write_log("\t\tmode:0%03o\n", fcb.mode);
    write_log("\t\t--------------------\n");
}

myfcb create_fcb(const char* path, uid_t uid, gid_t gid, mode_t mode) {
    myfcb new_fcb = {
            .uid = uid,
            .gid = gid,
            .mode = mode,
    };

    sprintf(new_fcb.path, path);
    uuid_generate(new_fcb.file_data_id);
    uuid_generate(new_fcb.data);

    return new_fcb;
}

// Some other useful definitions we might need

extern unqlite_int64 root_object_size_value;

// We need to use a well-known value as a key for the root object.
#define ROOT_OBJECT_KEY "root_object_key"
#define ROOT_OBJECT_KEY_SIZE ((int)strlen(ROOT_OBJECT_KEY) +1)

// This is the size of a regular key used to fetch things from the 
// database. We use uuids as keys, so 16 bytes each
#define KEY_SIZE 16

#define META_PREFIX "meta "
#define META_PREFIX_SIZE strlen(META_PREFIX)
#define META_KEY_SIZE ((int) (META_PREFIX_SIZE + KEY_SIZE))

// The length of a direntry that is stored in the db.
#define MY_DENTRY_SIZE ((KEY_SIZE + MY_MAX_PATH)*sizeof(char))
#define OPEN_CALLED 1
// The name of the file which will hold our filesystem
// If things get corrupted, unmount it and delete the file
// to start over with a fresh filesystem
#define DATABASE_NAME "myfs.db"

extern unqlite* pDb;

extern void error_handler(int);

extern FILE* init_log_file();

extern uuid_t zero_uuid;

// We can use the fs_state struct to pass information to fuse, which our handler functions can
// then access. In this case, we use it to pass a file handle for the file used for logging
struct myfs_state {
    FILE* logfile;
};
#define NEWFS_PRIVATE_DATA ((struct myfs_state *) fuse_get_context()->private_data)


// Some helper functions for logging etc.

// In order to log actions while running through FUSE, we have to give
// it a file handle to use. We define a couple of helper functions to do
// logging. No need to change this if you don't see a need
//

FILE* logfile;

// Open a file for writing so we can obtain a handle
FILE* init_log_file() {
    //Open logfile.
    logfile = fopen("myfs.log", "w");
    if (logfile == NULL) {
        perror("Unable to open log file. Life is not worth living.");
        exit(EXIT_FAILURE);
    }
    //Use line buffering
    setvbuf(logfile, NULL, _IOLBF, 0);
    return logfile;
}

// Write to the provided handle
void write_log(const char* format, ...) {
    va_list ap;
    va_start(ap, format);
    vfprintf(NEWFS_PRIVATE_DATA->logfile, format, ap);
}

// Simple error handler which cleans up and quits
void error_handler(int rc) {
    if (rc != UNQLITE_OK) {
        const char* zBuf;
        int iLen;
        unqlite_config(pDb, UNQLITE_CONFIG_ERR_LOG, &zBuf, &iLen);
        if (iLen > 0) {
            perror("error_handler: ");
            perror(zBuf);
        }
        if (rc != UNQLITE_BUSY && rc != UNQLITE_NOTIMPLEMENTED) {
            /* Rollback */
            unqlite_rollback(pDb);
        }
        exit(rc);
    }
}




/** Caching that does not work. */
/*  Comment and uncomment the whole code by toggling the first of the forward slashes on this line.
typedef struct _cache_entry {
    long key_size;
    long value_size;
    char value[MY_MAX_FILE_SIZE];
    char key[CONST_MKS];
    unsigned char reference_bits;
} cache_entry;

#define CACHE_ENTRY_SIZE (sizeof(cache_entry))
#define MAX_CACHE_SIZE 10
cache_entry cache[MAX_CACHE_SIZE];
int cache_size = 0;

int is_cache_full() {
    return cache_size == MAX_CACHE_SIZE;
}
//*/
/**
 * Retrieves the data associated with the given key from the cache.
 * Stores the data in the buffer buf and returns the index of the key in the cache.
 * If the buffer is NULL (in which case data_length can be 0) only the index is returned.
 *
 * @param k the key of the data
 * @param ks the size of the key
 * @param buf the buffer where to put the data or NULL
 * @param data_length the length of the data to read
 * @return the index of the cache entry
 */
/*
int get_from_cache(void* k, size_t ks, void* buf, size_t* data_length) {
    int iLog = 1;
    LOG_FUNC("\t\t\t\tGET from CACHE\n");
    LOG_GENERAL("\t\t\t\t\tKey:\"");
    for (int i = 0; i < ks; ++i) {
        LOG_GENERAL("%c", ((char*) k)[i]);
    }
    LOG_GENERAL("\"\n");
    int i;
    for (i = 0; i < cache_size; ++i) {
        // Find entry of this key.
        if (!memcmp(cache[i].key, k, ks))
            break;
    }
    if (i == cache_size) return -1;
    LOG_CLARIFY("\t\t\t\t\ti=%d\n", i);
    if (buf != NULL) {
        LOG_CLARIFY("\t\t\t\t\tbuf is not NULL");
        if (data_length != NULL) {
            LOG_CLARIFY("\t\t\t\t\tdata length is not NULL   data_length=%d\n", *data_length);
            *data_length = (size_t) cache[i].value_size;
        }

        memcpy(buf, cache[i].value, (size_t) cache[i].value_size);
        cache[i].reference_bits |= (1 << (sizeof(cache[i].reference_bits) - 1));
    }
    return i;
}

void put_in_cache(void* k, size_t ks, void* v, size_t vs) {
    int iLog = 1;
    int index = get_from_cache(k, ks, NULL, NULL);
    if (index == -1) {
        index = cache_size;
        cache_size++;
    }

    LOG_FUNC("\t\t\t\tPUT in CACHE i=%d\n", index);
    memcpy(cache[index].key, k, ks);
    cache[index].key_size = ks;
    LOG_GENERAL("\t\t\t\t\tKey put:\"");
    for (int i = 0; i < ks; ++i) {
        LOG_GENERAL("%c", cache[index].key[i]);
    }
    LOG_GENERAL("\"\n");
    cache_entry* ce = cache+index;
    LOG_GENERAL("\t\t\t\t\tCache entry obtained\n");
    char * cache_location = ce->value;
    LOG_GENERAL("\t\t\t\t\tGot address=%p\n", cache_location);
    memcpy(cache_location, v, vs);
//    memcpy(cache[index].value, v, vs);
    cache[index].value_size = vs;
    LOG_CLARIFY("\t\t\t\t\tValue put\n");

}

void shift_cache_refbits() {
    for (int i = 0; i < cache_size; ++i)
        cache[i].reference_bits >>= 1;
}

int get_least_used_cache(cache_entry* entry) {
    int iLog = 1;
    LOG_FUNC("\t\t\t\tGET LAST USED CACHE\n");
    int index = 0;
    for (int i = 1; i < cache_size; ++i)
        if (cache[i].reference_bits < cache[index].reference_bits)
            index = i;
    LOG_GENERAL("\t\t\t\t\tindex=%d\n", index);
    LOG_GENERAL("\t\t\t\t\tcache+index=%d\n", cache + index);
    LOG_GENERAL("\t\t\t\t\tcache+index*CACHE_ENTRY_SIZE=%d\n", cache + index * CACHE_ENTRY_SIZE);

    memcpy(entry, cache + index, CACHE_ENTRY_SIZE);
    return index;
}

void remove_from_cache(int index) {
    int iLog = 1;
    LOG_FUNC("\t\t\t\tREMOVE from CACHE  index=%d\n", index);
    LOG_FUNC("\t\t\t\t\tcache_size=%d\n", cache_size);
    cache_size--;
    for (int i = index; i < cache_size; ++i) {
//        cache_entry ce = cache[i];
//        LOG_GENERAL("\t\t\t\t\tce.key=\"");
//        for (int j = 0; j < ce.key_size; ++j) {
//            LOG_GENERAL("%c", ce.key[j]);
//        }
//        LOG_GENERAL("\"\n");
        cache[i] = cache[i+1];
//        memcpy(cache+i*CACHE_ENTRY_SIZE, cache+(i+1)*CACHE_ENTRY_SIZE, CACHE_ENTRY_SIZE);
//        memcpy(cache+i, cache+(i+1), 1);
//        cache[i].key_size = cache[i + 1].key_size;
//        LOG_GENERAL("\t\t\t\t\tkey_size moved\n");
//        memcpy(cache[i].key, cache[i + 1].key, CONST_MKS);
//        LOG_GENERAL("\t\t\t\t\tkey moved\n");
//        cache[i].value_size = cache[i + 1].value_size;
//        memcpy(cache[i].value, cache[i + 1].value, CONST_MKS);
//        LOG_GENERAL("\t\t\t\t\tvalue moved\n");
    }
}
//*/