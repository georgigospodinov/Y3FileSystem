//<editor-fold desc="Definitions">
/*
  MyFS. One directory, one file, 1000 bytes of storage. What more do you need?
  This Fuse file system is based largely on the HelloWorld example by Miklos Szeredi <miklos@szeredi.hu> (http://fuse.sourceforge.net/helloworld.html). Additional inspiration was taken from Joseph J. Pfeiffer's "Writing a FUSE Filesystem: a Tutorial" (http://www.cs.nmsu.edu/~pfeiffer/fuse-tutorial/).
  And now further implemented by 150009974.
*/

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <errno.h>

#include "myfs.h"

// The root fcb. Often updated in tests.
myfcb the_root_fcb;

// This is the pointer to the database we will use to store all our files
unqlite* pDb;
uuid_t zero_uuid;
//</editor-fold>

/** ============================= Helper functions ============================= */
/** ============================= Helper functions ============================= */
/** ============================= Helper functions ============================= */
static int store(void* key, int key_len, void* data, unqlite_int64 data_len) {
    int iLog = 0;
    LOG_FUNC("\tSTORE key=\"");
    for (int i = 0; i < key_len; ++i) {
        LOG_FUNC("%c", ((char*) key)[i]);
    }
    LOG_FUNC("\"\n");

    int rc = unqlite_kv_store(pDb, key, key_len, data, data_len);
    TEST_CONDITION(rc, "\tstore - failed", -EIO);

    return 0;
}

static int fetch(void* key, int key_len, void* data, unqlite_int64* data_len) {
    int iLog = 0;
    LOG_FUNC("\tFETCH key=\"");
    for (int i = 0; i < key_len; ++i) {
        LOG_FUNC("%c", ((char*) key)[i]);
    }
    LOG_FUNC("\"\n");

    int rc = unqlite_kv_fetch(pDb, key, key_len, data, data_len);
    TEST_CONDITION(rc == UNQLITE_NOTFOUND, "\tfetch - entry not found", -ENOENT);
    TEST_CONDITION(rc == UNQLITE_IOERR, "\tfetch - os error", -EIO);
    TEST_CONDITION(rc == UNQLITE_NOMEM, "\tfetch - out of memory", -ENOMEM);

    return 0;
}

static int get_meta(uuid_t data, meta_data* md) {
    int iLog = 0;
    LOG_FUNC("\tGET META data=\"");
    for (int i = 0; i < KEY_SIZE; ++i) {
        LOG_FUNC("%c", data[i]);
    }
    LOG_FUNC("\"\n");

    char key[META_KEY_SIZE];
    memcpy(key, META_PREFIX, META_PREFIX_SIZE);
    memcpy(key + META_PREFIX_SIZE, data, KEY_SIZE);
    unqlite_int64 size = META_DATA_SIZE;
    int CHECKED_CALL(fetch, key, META_KEY_SIZE, md, &size);

    return 0;
}

static int set_meta(uuid_t data, meta_data* md) {
    int iLog = 0;
    LOG_FUNC("\tSET META data=\"");
    for (int i = 0; i < KEY_SIZE; ++i) {
        LOG_FUNC("%c", data[i]);
    }
    LOG_FUNC("\"\n");

    char key[META_KEY_SIZE];
    memcpy(key, META_PREFIX, META_PREFIX_SIZE);
    memcpy(key + META_PREFIX_SIZE, data, KEY_SIZE);
    md->ctime = time(NULL);  // Update time of last change
    int CHECKED_CALL(store, key, META_KEY_SIZE, md, META_DATA_SIZE);

    return 0;
}

static int remove_meta(uuid_t data) {
    int iLog = 0;
    LOG_FUNC("\tREMOVE META data=\"");
    for (int i = 0; i < KEY_SIZE; ++i) {
        LOG_FUNC("%c", data[i]);
    }
    LOG_FUNC("\"\n");

    char key[META_KEY_SIZE];
    memcpy(key, META_PREFIX, META_PREFIX_SIZE);
    memcpy(key + META_PREFIX_SIZE, data, KEY_SIZE);

    int rc = unqlite_kv_delete(pDb, key, META_KEY_SIZE);
    TEST_CONDITION(rc, "\tremove_meta - failed to remove entry", -EIO);

    return 0;
}

static int set_nlinks(uuid_t data, __nlink_t n) {
    int iLog = 0;
    LOG_FUNC("\tSET NUMBER OF LINKS data=\"");
    for (int i = 0; i < KEY_SIZE; ++i) {
        LOG_FUNC("%c", data[i]);
    }
    LOG_FUNC("\"  n=%lld\n", n);

    meta_data md;
    int CHECKED_CALL(get_meta, data, &md);
    md.nlinks = n;
    LOG_GENERAL("\t\treceived\n");
    CHECKED_CALL(set_meta, data, &md);
    return 0;
}

static int get_nlinks(uuid_t data, __nlink_t* n) {
    int iLog = 0;
    LOG_FUNC("\tGET NUMBER OF LINKS data=\"");
    for (int i = 0; i < KEY_SIZE; ++i) {
        LOG_FUNC("%c", data[i]);
    }
    LOG_FUNC("\"\n");

    meta_data md;
    int CHECKED_CALL(get_meta, data, &md);
    *n = md.nlinks;
    return 0;
}

static int index_of_last_dash(const char* path) {
    int iLog = 0;
    LOG_FUNC("\tINDEX OF LAST DASH  path=\"%s\"\n", path);
    int ind = -1, i;
    for (i = 0; path[i] != '\0'; i++)
        if (path[i] == '/' && path[i + 1] != '\0')
            ind = i;
    LOG_GENERAL("\t\tind=%d\n", ind);
    return ind;
}

static void get_parent_path(const char* path, char* parent_path_buff, size_t n) {
    int iLog = 0;
    LOG_FUNC("\t\tGET PARENT PATH  path=\"%s\"\n", path);
    int ind = index_of_last_dash(path);
    if (ind == -1) ind = 0;
    memcpy(parent_path_buff, path, ind);
    for (int i = ind; i < n; ++i)
        parent_path_buff[i] = '\0';

    LOG_FUNC("\t\t\tparent path=\"%s\"\n", parent_path_buff);
}

/**
 * Fetches the data pointed to by the fcb's data
 * and stores it in the memory pointed to by data.
 *
 * @param fcb a pointer to the fcb with the data to fetch
 * @param data the location where to put the data
 * @return 0 on success, an appropriate error code otherwise
 *                       (this code is to be returned to the OS)
 */
static int get_data(myfcb fcb, void* data) {
    int iLog = 0;
    LOG_FUNC("\tGET DATA fcb.path=\"%s\"\n", fcb.path);
    LOG_FCB(fcb);
    LOG_GENERAL("\t\tfcb.data:\"");
    for (int i = 0; i < KEY_SIZE; ++i) {
        LOG_GENERAL("%c", fcb.data[i]);
    }
    LOG_GENERAL("\"\n");

    meta_data md;
    int CHECKED_CALL(get_meta, fcb.data, &md);
    unqlite_int64 size_of_data = md.size * MY_DENTRY_SIZE;
    CHECKED_CALL(fetch, fcb.data, KEY_SIZE, data, &size_of_data);

    return 0;
}

/**
 * Given a path, finds the FCB for it and stores it in the given buffer.
 *
 * @param path the path to look for
 * @param fcb_buff the location to store the found fcb
 * @return 0 on success, an appropriate error code otherwise
 *                       (this code is to be returned to the OS)
 */
static int get_fcb(const char* path, myfcb* fcb_buff) {
    int iLog = 0;
    LOG_FUNC("\tGET FCB path=\"%s\"\n", path);

    if (strcmp(path, the_root_fcb.path) == 0 || strcmp(path, "/") == 0) {
        LOG_CLARIFY("\t\treached root\n");
        unqlite_int64 size_of_buff = MYFCB_SIZE;

        int CHECKED_CALL(fetch, ROOT_OBJECT_KEY, ROOT_OBJECT_KEY_SIZE, fcb_buff, &size_of_buff);
        return 0;
    }

    myfcb parent;
    size_t n = strlen(path);
    char parent_path[n + 1];
    get_parent_path(path, parent_path, n);
    int CHECKED_CALL(get_fcb, parent_path, &parent);
    TEST_CONDITION(!S_ISDIR(parent.mode), "\t---get_fcb part of path is not directory", -ENOTDIR);

    LOG_CLARIFY("\t\tparent:\n");
    LOG_FCB(parent);

    meta_data md;
    CHECKED_CALL(get_meta, parent.data, &md);
    LOG_META(md);

    char data[md.size * MY_DENTRY_SIZE];
    CHECKED_CALL(get_data, parent, data);

    LOG_CLARIFY("\t\tlooking for path = \"%s\"\n", path);
    LOG_GENERAL("\t\tin this data:\n");
    for (int i = 0; i < md.size; ++i) {
        LOG_GENERAL("\t\t\t\"%s\"\n", data + i * MY_DENTRY_SIZE + KEY_SIZE);
    }
    char* child_path;
    int i;
    for (i = 0; i < md.size; ++i) {
        child_path = data + i * MY_DENTRY_SIZE + KEY_SIZE;

        LOG_CLARIFY("\t\tchild_path = \"%s\"\n", child_path);

        // If this is the child we are looking for, great.
        if (!strcmp(child_path, path)) break;
        if (!strcmp(child_path + 1, path)) break;  // ignoring leading /
    }

    TEST_CONDITION((i == md.size), "\t---get_fcb failed to find child", -ENOENT);

    uuid_t id;
    memcpy(id, data + i * MY_DENTRY_SIZE, KEY_SIZE);
    LOG_CLARIFY("\t\tid=\"%s\"\n", id);

    unqlite_int64 buff_size = MYFCB_SIZE;
    CHECKED_CALL(fetch, id, KEY_SIZE, fcb_buff, &buff_size);

    return 0;
}

static int get_parent_fcb(const char* path, myfcb* parent_fcb) {
    int iLog = 0;
    LOG_FUNC("\tGET PARENT FCB  path=\"%s\"\n", path);

    size_t len = strlen(path);
    char parent_path[len];
    get_parent_path(path, parent_path, len);
    LOG_GENERAL("\t\tparen path=\"%s\"\n", parent_path);

    int CHECKED_CALL(get_fcb, parent_path, parent_fcb);
    return 0;
}

static int get_child_fcb(const char* child_path, char* data, off_t number_of_files, myfcb* child_fcb, int* index) {
    int iLog = 0;
    LOG_FUNC("\tGET CHILD FCB  child=\"%s\"\n", child_path);

    int i;
    for (i = 0; i < number_of_files; ++i) {
        char* current_child = data + i * MY_DENTRY_SIZE + KEY_SIZE;
        LOG_CLARIFY("\t\tcurrent_child=\"%s\"\n", current_child);
        if (!strcmp(child_path, current_child)) break;
    }
    TEST_CONDITION(i == number_of_files, "get_child_fcb - could not find path in data", -ENONET);

    char child_uuid[KEY_SIZE];
    memcpy(child_uuid, data + i * MY_DENTRY_SIZE, KEY_SIZE);
    LOG_GENERAL("\t\tchild_uuid=\"%s\"\n", child_uuid);

    unqlite_int64 buff_size = MYFCB_SIZE;
    int CHECKED_CALL(fetch, child_uuid, KEY_SIZE, child_fcb, &buff_size);
    *index = i;

    return 0;
}

static int get_fcb_and_meta(const char* path, myfcb* fcb, meta_data* md) {
    int iLog = 0;
    LOG_FUNC("\tGET FCB & META path=\"%s\"\n", path);
    int CHECKED_CALL(get_fcb, path, fcb);
    CHECKED_CALL(get_meta, fcb->data, md);
}

/**
 * Creates and attaches a new FCB to the tree hierarchy of the file system.
 *
 * @param path the path of the new FCB
 * @param mode the mode of the new FCB
 * @param fcb  a memory location to put the fcb data to avoid a call to 'get_fcb'
 *          ignored if NULL is passed
 * @return 0 on success, or an appropriate error code
 *                          (this code is to be returned to the OS)
 */
static int attach_fcb_to_tree(const char* path, mode_t mode, myfcb* fcb, meta_data* md) {
    int iLog = 0;
    LOG_FUNC("\tATTACH TO TREE path=\"%s\"  mode=0%03o\n", path, mode);

    // Check if path length is of acceptable length
    TEST_CONDITION(strlen(path) >= MY_MAX_PATH, "attach_fcb_to_tree - path too long", -ENAMETOOLONG);

    // Get parent
    myfcb parent_fcb;
    meta_data parent_md;
    int CHECKED_CALL(get_parent_fcb, path, &parent_fcb);
    CHECKED_CALL(get_meta, parent_fcb.data, &parent_md);
    int size_of_data = parent_md.size * MY_DENTRY_SIZE;
    char data[size_of_data + MY_DENTRY_SIZE];
    CHECKED_CALL(get_data, parent_fcb, data);

    // Create new FCB
    struct fuse_context* context = fuse_get_context();
    myfcb new_fcb = create_fcb(path, context->uid, context->gid, mode);
    LOG_FCB(new_fcb);
    meta_data new_md = create_meta_data();
    LOG_GENERAL("\tcreated path:%s\n", new_fcb.path);

    // Store the new_fcb in the database.
    CHECKED_CALL(store, new_fcb.file_data_id, KEY_SIZE, &new_fcb, MYFCB_SIZE);
    CHECKED_CALL(set_meta, new_fcb.data, &new_md);

    // Add new FCB to parent's data & FCB
    memcpy(data + size_of_data, new_fcb.file_data_id, KEY_SIZE);
    memcpy(data + size_of_data + KEY_SIZE, new_fcb.path, MY_MAX_PATH);

    // Update parent in DB
    CHECKED_CALL(store, parent_fcb.data, KEY_SIZE, data, size_of_data + MY_DENTRY_SIZE);
    parent_md.size++;
    parent_md.mtime = time(0);
    CHECKED_CALL(set_meta, parent_fcb.data, &parent_md);

    LOG_FCB(new_fcb);

    if (fcb != NULL) memcpy(fcb, &new_fcb, MYFCB_SIZE);
    if (md != NULL) memcpy(md, &new_md, META_DATA_SIZE);

    return 0;
}

static int detach_fcb_from_tree(myfcb child_fcb, myfcb parent_fcb, int index, void* data) {
    int iLog = 0;
    LOG_FUNC("\tDETACH FROM TREE  child.path=\"%s\"\n", child_fcb.path);

    // Remove child from DB
    __nlink_t nlinks;
    int CHECKED_CALL(get_nlinks, child_fcb.data, &nlinks);
    nlinks--;
    if (nlinks == 0) {
        LOG_CLARIFY("\t\tRemoving data because no more links!\n");
        // remove data
        rc = unqlite_kv_delete(pDb, child_fcb.file_data_id, KEY_SIZE);
        TEST_CONDITION(rc, "myfs_unlink failed to delete child data from DB", -EIO);
        CHECKED_CALL(remove_meta, child_fcb.data);
    }
    else {  // save less link
        LOG_CLARIFY("\t\tReducing number of links to data!\n");
        CHECKED_CALL(set_nlinks, child_fcb.data, nlinks);
    }

    // Remove child from parent_fcb's data
    meta_data parent_md;
    CHECKED_CALL(get_meta, parent_fcb.data, &parent_md);
    parent_md.size--;
    for (int i = index; i < parent_md.size; ++i) {
        memcpy(data + i * MY_DENTRY_SIZE, data + (i + 1) * MY_DENTRY_SIZE, MY_DENTRY_SIZE);
    }

    CHECKED_CALL(store, parent_fcb.data, KEY_SIZE, data, parent_md.size * MY_DENTRY_SIZE);
    CHECKED_CALL(store, parent_fcb.file_data_id, KEY_SIZE, &parent_fcb, MYFCB_SIZE);
    parent_md.mtime = time(0);
    CHECKED_CALL(set_meta, parent_fcb.data, &parent_md);

    return 0;
}

static int set_permissions(struct fuse_file_info* fi, myfcb fcb) {
    int iLog = 0;
    int flags = fi->flags;
    LOG_FUNC("\tACCESS ALLOWED ?  flags=0%03o  myfcb.mode=0%03o\n", flags, fcb.mode);
    if (S_ISDIR(flags)) {
        LOG_CLARIFY("\t\tflags is directory\n");
        TEST_CONDITION(!S_ISDIR(fcb.mode), "attempted to access a non-directory as if its a directory", -ENOTDIR);
        flags ^= S_IFDIR;
    }
    else if (S_ISREG(flags)) {
        LOG_CLARIFY("\t\tflags is regular\n");
        TEST_CONDITION(!S_ISREG(fcb.mode), "attempted to access a non-regular file as if its a regular file", -EISDIR);
        flags ^= S_IFREG;
    }
    else if (S_ISLNK(flags)) {
        LOG_CLARIFY("\t\tflags is symlink\n");
        TEST_CONDITION(!S_ISLNK(fcb.mode), "attempted to access a non-symlink as if its a symlink", -ENOLINK);
        flags ^= S_IFLNK;
    }

    LOG_CLARIFY("\t\tpassed initial checks  flags=0%03o\n", flags);


    uid_t uid = fuse_get_context()->uid;
    gid_t gid = fuse_get_context()->gid;
    LOG_GENERAL("\t\tuid=0%03o  gid=0%03o\n", uid, gid);

    // Take the appropriate permissions.
    int permissions[3];
    int R = 0, W = 1, X = 2;
    if (uid == fcb.uid) {
        LOG_CLARIFY("\t\towner accessing file\n");
        permissions[R] = fcb.mode & S_IRUSR;
        permissions[W] = fcb.mode & S_IWUSR;
        permissions[X] = fcb.mode & S_IXUSR;
    }
    else if (gid == fcb.gid) {
        LOG_CLARIFY("\t\tgroup accessing file\n");
        permissions[R] = fcb.mode & S_IRGRP;
        permissions[W] = fcb.mode & S_IWGRP;
        permissions[X] = fcb.mode & S_IXGRP;
    }
    else {
        LOG_CLARIFY("\t\tother accessing file\n");
        permissions[R] = fcb.mode & S_IROTH;
        permissions[W] = fcb.mode & S_IWOTH;
        permissions[X] = fcb.mode & S_IXOTH;
    }

    fi->fh = (uint64_t) (O_ACCMODE & flags);  // rdwr==10,wronly=01, rdonly=00  (binary)
    TEST_CONDITION(fi->fh % 2 == 0 && !permissions[R], "no read permissions", -EACCES);  // read is even
    TEST_CONDITION(fi->fh > 0 && !permissions[W], "no write permissions", -EACCES);  // write is > 0

    if (flags & O_APPEND && permissions[W]) fi->nonseekable = 1;
    TEST_CONDITION((flags & O_APPEND && !permissions[W]), "no append permissions", -EACCES);

    if (flags & O_CREAT && permissions[W]) fi->fh |= O_CREAT;
    TEST_CONDITION((flags & O_CREAT && !permissions[W]), "no create permissions", -EACCES);

    fi->fh_old = OPEN_CALLED;

    return 0;
}





/** ============================ Required functions ============================ */
/** ============================ Required functions ============================ */
/** ============================ Required functions ============================ */

/** ======================== Any type of File functions ======================== */

// Get file and directory attributes (meta-data).
// Read 'man 2 stat' and 'man 2 chmod'.
static int myfs_getattr(const char* path, struct stat* stbuf) {
    int iLog = 0;
    LOG_FUNC("GET ATTRIBUTES path=\"%s\"\n", path);

    memset(stbuf, 0, sizeof(struct stat));
    myfcb fcb;
    meta_data md;
    int CHECKED_CALL(get_fcb_and_meta, path, &fcb, &md);
    LOG_FCB(fcb);
    if (!strcmp(path, "/")) {
        LOG_CLARIFY("\tmatches /\n");  // Update root fcb?
        CHECKED_CALL(get_fcb, the_root_fcb.path, &the_root_fcb);
    }
    LOG_META(md);

    stbuf->st_mode = fcb.mode;
    stbuf->st_uid = fcb.uid;
    stbuf->st_gid = fcb.gid;

    stbuf->st_size = md.size;
    stbuf->st_nlink = md.nlinks;
    stbuf->st_atime = md.atime;
    stbuf->st_mtime = md.mtime;
    stbuf->st_ctime = md.ctime;

    return 0;
}

// Set update the times (actime, modtime) for a file. This FS only supports modtime.
// Read 'man 2 utime'.
static int myfs_utime(const char* path, struct utimbuf* ubuf) {
    int iLog = 0;
    LOG_FUNC("UTIME path=\"%s\"\n", path);

    meta_data md;
    myfcb fcb;
    int CHECKED_CALL(get_fcb_and_meta, path, &fcb, &md);
    if (ubuf == NULL) {
        md.atime = time(0);
        md.mtime = time(0);
    }
    else {
        md.atime = ubuf->actime;
        md.mtime = ubuf->modtime;
    }
    CHECKED_CALL(set_meta, fcb.data, &md);

    return 0;
}

// Set permissions.
// Read 'man 2 chmod'.
static int myfs_chmod(const char* path, mode_t mode) {
    int iLog = 0;
    LOG_FUNC("CHMOD path=\"%s\"  mode=0%03o \n", path, mode);
    myfcb fcb;
    meta_data md;
    int CHECKED_CALL(get_fcb_and_meta, path, &fcb, &md);
    fcb.mode = mode;

    LOG_FCB(fcb);
    CHECKED_CALL(store, fcb.file_data_id, KEY_SIZE, &fcb, MYFCB_SIZE);
    CHECKED_CALL(set_meta, fcb.data, &md);

    return 0;
}

// Set ownership.
// Read 'man 2 chown'.
static int myfs_chown(const char* path, uid_t uid, gid_t gid) {
    int iLog = 0;
    LOG_FUNC("CHOWN path=\"%s\"  uid=%d  gid=%d\n", path, uid, gid);
    myfcb fcb;
    meta_data md;
    int CHECKED_CALL(get_fcb_and_meta, path, &fcb, &md);
    fcb.uid = uid;
    fcb.gid = gid;

    LOG_FCB(fcb);
    CHECKED_CALL(store, fcb.file_data_id, KEY_SIZE, &fcb, MYFCB_SIZE);
    CHECKED_CALL(set_meta, fcb.data, &md);

    return 0;
}

// Open a file. Open should check if the operation is permitted for the given flags (fi->flags).
// Read 'man 2 open'.
static int myfs_open(const char* path, struct fuse_file_info* fi) {
    int iLog = 1;
    LOG_FUNC("OPEN  path=\"%s\"  fi->flags=0%03o\n", path, fi->flags);
    myfcb fcb;
    int CHECKED_CALL(get_fcb, path, &fcb);
    CHECKED_CALL(set_permissions, fi, fcb);

    return 0;
}

/** ======================== Regular File functions ======================== */
// Read a file.
// Read 'man 2 read'.
static int myfs_read(const char* path, char* buf, size_t size, off_t offset, struct fuse_file_info* fi) {
    int iLog = 1;
    LOG_FUNC("READ path=\"%s\"  size=%d  offset=%lld  fi->flags=0%03o\n", path, size, offset, fi->flags);

    TEST_CONDITION((fi->fh_old & OPEN_CALLED) && (fi->fh % 2), "myfs_read no read permissions", -EACCES);

    size_t len;
    (void) fi;

    uint8_t data_block[MY_MAX_FILE_SIZE];
    memset(&data_block, 0, MY_MAX_FILE_SIZE);
    meta_data md;
    myfcb fcb;
    int CHECKED_CALL(get_fcb_and_meta, path, &fcb, &md);

    len = (size_t) md.size;
    // Is there a data block?
    if (len) {
        LOG_CLARIFY("\tthere is data\n");
        unqlite_int64 nBytes;  //Data length.
        CHECKED_CALL(fetch, fcb.data, KEY_SIZE, NULL, &nBytes);
        LOG_GENERAL("\tsize = %d\n", nBytes);
        error_handler(rc);
        TEST_CONDITION(nBytes != MY_MAX_FILE_SIZE, "myfs_read - EIO", -EIO);

        // Fetch the fcb the root data block from the store.
        CHECKED_CALL(fetch, fcb.data, KEY_SIZE, &data_block, &nBytes);
    }

    if (offset < len) {
        if (offset + size > len)  // Can't read beyond the end of the file.
            size = len - offset;
        memcpy(buf, &data_block + offset, size);
        md.atime = time(0);
        CHECKED_CALL(set_meta, fcb.data, &md);
    }
    else size = 0;  // Can't read beyond the end of the file.

    return size;
}

// Read 'man 2 creat'.
static int myfs_create(const char* path, mode_t mode, struct fuse_file_info* fi) {
    int iLog = 1;
    LOG_FUNC("CREATE path=\"%s\"\n", path);
    TEST_CONDITION(fi->fh_old & OPEN_CALLED && (fi->fh & O_CREAT),
                   "myfs_create - no create permissions", -EACCES);

    return attach_fcb_to_tree(path, mode | S_IFREG, NULL, NULL);
}

// Write to a file.
// Read 'man 2 write'
static int myfs_write(const char* path, const char* buf, size_t size, off_t offset, struct fuse_file_info* fi) {
    int iLog = 1;
    LOG_FUNC("WRITE path=\"%s\"  data=\"%d\"  size=%d  offset=%lld  fi->flags=0%03o\n", path, buf, size, offset,
             fi->flags);

    int permission = (int) (fi->fh % 4);
    TEST_CONDITION(fi->fh_old & OPEN_CALLED && (permission == 0 || permission == 3),
                   "myfs_write - no write permissions", -EACCES);
    TEST_CONDITION(size >= MY_MAX_FILE_SIZE, "myfs_write - input size exceeds max", -EFBIG);
    TEST_CONDITION(offset >= MY_MAX_FILE_SIZE, "myfs_write - input offset exceeds max", -EFBIG);

    uint8_t data_block[MY_MAX_FILE_SIZE];
    memset(&data_block, 0, MY_MAX_FILE_SIZE);
    meta_data md;
    myfcb fcb;
    int CHECKED_CALL(get_fcb_and_meta, path, &fcb, &md);

    TEST_CONDITION(fi->nonseekable && offset < md.size,
                   "myfs_write - no permission to write before the end of the file", -EACCES);
    //<editor-fold desc="Read previously stored data">
    if (md.size) {
        LOG_CLARIFY("\tdata size = %d\n", md.size);
        // Check object size, to prevent overflow
        unqlite_int64 nBytes;  // Data length.
        CHECKED_CALL(fetch, fcb.data, KEY_SIZE, NULL, &nBytes);
        TEST_CONDITION(nBytes != MY_MAX_FILE_SIZE, "myfs_write - bad file size", -EIO);

        // Fetch the data block from the store.
        CHECKED_CALL(fetch, fcb.data, KEY_SIZE, &data_block, &nBytes);
    }
    //</editor-fold>

    // Can't write beyond the end of the file.
    if (offset + size > MY_MAX_FILE_SIZE)
        size = (size_t) (MY_MAX_FILE_SIZE - offset);

    // Get the data from memory.
    // Copy all the bytes (including \0's)
    for (int i = 0; i < size; ++i)
        *(data_block + offset + i) = (uint8_t) buf[i];

    // Write the data to the store.
    CHECKED_CALL(store, fcb.data, KEY_SIZE, &data_block, MY_MAX_FILE_SIZE);

    // Update the meta data in storage.
    md.size = (md.size > offset + size ? md.size : offset + size);
    md.mtime = time(0);
    md.atime = time(0);
    CHECKED_CALL(set_meta, fcb.data, &md);
    LOG_META(md);

    return size;
}

// Delete a file.
// Read 'man 2 unlink'.
static int myfs_unlink(const char* path) {
    int iLog = 0;
    LOG_FUNC("UNLINK path=\"%s\"\n", path);

    myfcb parent_fcb;
    meta_data parent_md;
    int CHECKED_CALL(get_parent_fcb, path, &parent_fcb);
    CHECKED_CALL(get_meta, parent_fcb.data, &parent_md);
    char data[parent_md.size * MY_DENTRY_SIZE];
    CHECKED_CALL(get_data, parent_fcb, data);
    myfcb child_fcb;
    int index;
    CHECKED_CALL(get_child_fcb, path, data, parent_md.size, &child_fcb, &index);
    CHECKED_CALL(detach_fcb_from_tree, child_fcb, parent_fcb, index, data);

    return 0;
}

// Set the size of a file.
// Read 'man 2 truncate'.
static int myfs_truncate(const char* path, off_t newsize) {
    int iLog = 0;
    LOG_FUNC("TRUNCATE path=\"%s\"  newsize=%lld\n", path, newsize);
    TEST_CONDITION(newsize >= MY_MAX_FILE_SIZE, "myfs_truncate - new file size too big", -EFBIG);

    myfcb fcb;
    meta_data md;
    int CHECKED_CALL(get_fcb_and_meta, path, &fcb, &md);
    if (newsize == md.size) return 0;

    // The OS checks if this is a regular file.
    md.size = newsize;
    md.mtime = time(0);
    CHECKED_CALL(set_meta, fcb.data, &md);

    return 0;
}


/** ======================== Directory functions ======================== */
// Read a directory.
// Read 'man 2 readdir'.
static int myfs_readdir(const char* path, void* buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info* fi) {
    int iLog = 1;
    LOG_FUNC("READ DIR path=\"%s\"  offset=%lld  fi->flags=0%03o\n", path, offset, fi->flags);
    TEST_CONDITION((fi->fh_old & OPEN_CALLED) && (fi->fh % 2), "myfs_readdir - no read permissions", -EACCES);
    (void) offset;  // This prevents compiler warnings
    (void) fi;

    myfcb fcb;
    meta_data md;
    const char* p = (strcmp(path, "/") ? path : "");
    int CHECKED_CALL(get_fcb_and_meta, p, &fcb, &md);

    // We always output . and .. first, by convention. See documentation for more info on filler()
    int frc;
    frc = filler(buf, ".", NULL, 0);
    TEST_CONDITION(frc, "myfs_readdir - buffer full after adding .", -EIO);
    frc = filler(buf, "..", NULL, 0);
    TEST_CONDITION(frc, "myfs_readdir - buffer full after adding ..", -EIO);

    // if there is no data, return
    if (md.size == 0) return 0;
    LOG_FCB(fcb);

    LOG_CLARIFY("\tdata found, size=%d\n", md.size);
    char data[md.size * MY_DENTRY_SIZE];
    CHECKED_CALL(get_data, fcb, data);
    char* child;
    for (int i = 0; i < md.size; ++i) {
        child = data + i * MY_DENTRY_SIZE + KEY_SIZE;
        int ind = index_of_last_dash(child);
        child += ind + 1;
        LOG_CLARIFY("\tchild=\"%s\"\n", child);
        frc = filler(buf, child, NULL, 0);
        TEST_CONDITION(frc, "myfs_readdir - buffer full after adding a child", -EIO);
    }

    md.atime = time(0);
    CHECKED_CALL(set_meta, fcb.data, &md);

    return 0;
}

// Create a directory.
// Read 'man 2 mkdir'.
static int myfs_mkdir(const char* path, mode_t mode) {
    int iLog = 0;
    LOG_FUNC("MK DIR path=\"%s\"\n", path);

    myfcb fcb;
    int CHECKED_CALL(attach_fcb_to_tree, path, mode | S_IFDIR, &fcb, NULL);
    LOG_FCB(fcb);
    void* empty = 0;
    CHECKED_CALL(store, fcb.data, KEY_SIZE, empty, 0);

    return 0;
}

// Delete a directory.
// Read 'man 2 rmdir'.
static int myfs_rmdir(const char* path) {
    int iLog = 0;
    LOG_FUNC("RM DIR path=\"%s\"\n", path);

    myfcb fcb, parent_fcb;
    meta_data md, parent_md;
    int index_in_parent;
    int CHECKED_CALL(get_parent_fcb, path, &parent_fcb);
    CHECKED_CALL(get_meta, parent_fcb.data, &parent_md);
    char parent_data[parent_md.size * MY_DENTRY_SIZE];
    CHECKED_CALL(get_data, parent_fcb, parent_data);
    CHECKED_CALL(get_child_fcb, path, parent_data, parent_md.size, &fcb, &index_in_parent);
    CHECKED_CALL(get_meta, fcb.data, &md);
    LOG_FCB(fcb);
    LOG_META(md);
    if (md.size) return -ENOTEMPTY;
    LOG_CLARIFY("\tDetaching this directory.\n");
    CHECKED_CALL(detach_fcb_from_tree, fcb, parent_fcb, index_in_parent, parent_data);

    return 0;
}







/** =========================== Extension functions =========================== */
/** =========================== Extension functions =========================== */
/** =========================== Extension functions =========================== */
/**
 * Creates a hard link from a new file to an old file's data.
 *
 * @param from the path where the hard link will be placed
 * @param to the path of the currently existing data
 * @return 0 on success, error code otherwise
 */
static int myfs_link(const char* existing, const char* new) {
    int iLog = 0;
    LOG_FUNC("LINK existing=\"%s\", new=\"%s\"\n", existing, new);

    // Get 'existing' FCB
    myfcb existing_fcb;
    meta_data existing_md;
    int CHECKED_CALL(get_fcb_and_meta, existing, &existing_fcb, &existing_md);

    myfcb new_fcb;
    CHECKED_CALL(attach_fcb_to_tree, new, existing_fcb.mode, &new_fcb, NULL);
    LOG_GENERAL("----returned from ATTACH\n");

    // Set new_fcb.data to be the same as existing_fcb.data (now they both point to the same DB entry)
    memcpy(new_fcb.data, existing_fcb.data, KEY_SIZE);
    LOG_FCB(existing_fcb);
    LOG_FCB(new_fcb);
    CHECKED_CALL(store, new_fcb.file_data_id, KEY_SIZE, &new_fcb, MYFCB_SIZE);
    LOG_GENERAL("----returned from STORE\n");

    // Increment hard links count to that entry by one
    __nlink_t nlinks;
    CHECKED_CALL(get_nlinks, new_fcb.data, &nlinks);
    LOG_GENERAL("\tnlinks=%lld\n", nlinks);
    CHECKED_CALL(set_nlinks, new_fcb.data, nlinks + 1);

    return 0;

}

/**
 * Reads a symbolic (soft) link.
 *
 * @param path the path of the link
 * @param buf the buffer for the result of following the link
 * @param size the size of the buffer
 * @return 0 on success, an appropriate error code otherwise
 */
static int myfs_readlink(const char* path, char* buf, size_t size) {
    int iLog = 0;
    LOG_FUNC("READ LINK path=\"%s\"  buf=\"%s\"  size=%lld\n", path, buf, size);

    myfcb link;
    int CHECKED_CALL(get_fcb, path, &link);
    LOG_GENERAL("\t\tlink:\n");
    LOG_FCB(link);

    unqlite_int64 expected_size = (unqlite_int64) size;
    CHECKED_CALL(fetch, link.data, KEY_SIZE, buf, &expected_size);

    LOG_GENERAL("\tbuf=\"%s\"\n", buf);

    return 0;
}

/**
 * Creates a new symbolic link to the existing path.
 *
 * @param existing the path of the existing file (target)
 * @param new the path of the symbolic link to be created
 * @return 0 on success, an appropriate error code otehrwise
 */
static int myfs_symlink(const char* existing, const char* new) {
    int iLog = 0;
    LOG_FUNC("SYMLINK  existing=\"%s\"  new=\"%s\"\n", existing, new);

    myfcb new_fcb;
    meta_data md;
    int mode = S_IRUSR | S_IWUSR | S_IFLNK;
    int CHECKED_CALL(attach_fcb_to_tree, new, mode, &new_fcb, &md);
    LOG_FCB(new_fcb);

    // Copy the id of the existing FCB to the data of the new one.
    md.size = strlen(existing);
    CHECKED_CALL(store, new_fcb.data, KEY_SIZE, (void*) existing, md.size);
    CHECKED_CALL(store, new_fcb.file_data_id, KEY_SIZE, &new_fcb, MYFCB_SIZE);
    CHECKED_CALL(set_meta, new_fcb.data, &md);
    LOG_GENERAL("----returned from store\n");
    LOG_FCB(new_fcb);

    return 0;
}

static int myfs_rename(const char* from, const char* to) {
    int iLog = 0;
    LOG_FUNC("RENAME from=\"%s\"  to=\"%s\"\n", from, to);

    myfcb to_fcb;
    int rc = get_fcb(to, &to_fcb);
    if (!rc) {  // If destination does exist, unlink it?
        LOG_CLARIFY("\texists:\n");
        LOG_FCB(to_fcb);
        CHECKED_CALL(myfs_unlink, to);
    }
    CHECKED_CALL(myfs_link, from, to);
    LOG_GENERAL("---returned from LINK\n");

    myfcb from_fcb;
    CHECKED_CALL(get_fcb, from, &from_fcb);
    LOG_FCB(from_fcb);
    __nlink_t n;
    // Update from_fcb
    unqlite_int64 fcb_size = MYFCB_SIZE;
    CHECKED_CALL(fetch, from_fcb.file_data_id, KEY_SIZE, &from_fcb, &fcb_size);
    CHECKED_CALL(get_nlinks, from_fcb.data, &n);
    LOG_GENERAL("\tnlinks=%lld\n", n);
    CHECKED_CALL(myfs_unlink, from);
    LOG_GENERAL("---returned from UNLINK\n");
    CHECKED_CALL(get_fcb, to, &to_fcb);
    LOG_FCB(to_fcb);
    CHECKED_CALL(get_nlinks, to_fcb.data, &n);
    LOG_GENERAL("\tnlinks=%lld\n", n);


    // this should fail
    rc = get_fcb(from, &from_fcb);
    LOG_GENERAL("\trc=%lld\n", rc);


    return 0;
}
























/** ======================== OPTIONAL, not implemented ======================== */
// OPTIONAL - included as an example
// Flush any cached data.
static int myfs_flush(const char* path, struct fuse_file_info* fi) {
    int iLog = 0;
    LOG_FUNC("FLUSH path=\"%s\"\n", path);

    return 0;
}

// OPTIONAL - included as an example
// Release the file. There will be one call to release for each call to open.
static int myfs_release(const char* path, struct fuse_file_info* fi) {
    int iLog = 0;

    int retstat = 0;
    LOG_FUNC("RELEASE path=\"%s\"\n", path);

    return retstat;
}











/** ======================== FUSE setup ======================== */
/** ======================== FUSE setup ======================== */
/** ======================== FUSE setup ======================== */

// This struct contains pointers to all the functions defined above
// It is used to pass the function pointers to fuse
// fuse will then execute the methods as required 
static struct fuse_operations myfs_oper = {
        .getattr    = myfs_getattr,

        .mkdir      = myfs_mkdir,
        .readdir    = myfs_readdir,
        .rmdir      = myfs_rmdir,

        .create     = myfs_create,
        .open       = myfs_open,
        .read       = myfs_read,
        .write      = myfs_write,
        .truncate   = myfs_truncate,
        .flush      = myfs_flush,
        .release    = myfs_release,

        .unlink     = myfs_unlink,
        .link       = myfs_link,
        .readlink   = myfs_readlink,
        .symlink    = myfs_symlink,

        .utime      = myfs_utime,
        .chmod      = myfs_chmod,
        .chown      = myfs_chown,
        .rename     = myfs_rename,

};


// Initialise the in-memory data structures from the store. If the root object (from the store) is empty then create a root fcb (directory)
// and write it to the store. Note that this code is executed outide of fuse. If there is a failure then we have failed toi initlaise the 
// file system so exit with an error code.
void init_fs() {
    int rc;
    printf("init_fs\n");
    //Initialise the store.

    uuid_clear(zero_uuid);

    // Open the database.
    rc = unqlite_open(&pDb, DATABASE_NAME, UNQLITE_OPEN_CREATE);
    if (rc != UNQLITE_OK) error_handler(rc);

    unqlite_int64 nBytes;  // Data length

    // Try to fetch the root element
    // The last parameter is a pointer to a variable which will hold the number of bytes actually read
    rc = unqlite_kv_fetch(pDb, ROOT_OBJECT_KEY, ROOT_OBJECT_KEY_SIZE, &the_root_fcb, &nBytes);

    // if it doesn't exist, we need to create one and put it into the database. This will be the root
    // directory of our filesystem i.e. "/"
    if (rc == UNQLITE_NOTFOUND) {

        printf("init_store: root object was not found\n");

        // clear everything in the_root_fcb
        memset(&the_root_fcb, 0, MYFCB_SIZE);

        // Sensible initialisation for the root FCB
        //See 'man 2 stat' and 'man 2 chmod'.
        the_root_fcb.mode |= S_IFDIR |
                             S_IRUSR | S_IWUSR | S_IXUSR |
                             S_IRGRP | S_IWGRP | S_IXGRP |
                             S_IROTH | S_IWOTH | S_IXOTH;
        the_root_fcb.uid = getuid();
        the_root_fcb.gid = getgid();
        memcpy(the_root_fcb.file_data_id, ROOT_OBJECT_KEY, ROOT_OBJECT_KEY_SIZE);
        memcpy(the_root_fcb.data, "root_direntries", KEY_SIZE);

        char key[META_KEY_SIZE];
        memcpy(key, META_PREFIX, META_PREFIX_SIZE);
        memcpy(key + META_PREFIX_SIZE, the_root_fcb.data, KEY_SIZE);
        meta_data md = create_meta_data();
        unqlite_kv_store(pDb, key, META_KEY_SIZE, &md, META_DATA_SIZE);

        // Write the root FCB
        printf("init_fs: writing root fcb\n");
        rc = unqlite_kv_store(pDb, ROOT_OBJECT_KEY, ROOT_OBJECT_KEY_SIZE, &the_root_fcb, MYFCB_SIZE);
        if (rc != UNQLITE_OK) error_handler(rc);

        void* empty = 0;
        rc = unqlite_kv_store(pDb, the_root_fcb.data, KEY_SIZE, empty, 0);
        if (rc) {
            printf("init_fs could not store empty block for root data\n");
            exit(-1);
        }
        else printf("initial empty storage is successful\n");

    }
    else {
        if (rc == UNQLITE_OK) {
            printf("init_store: root object was found\n");
        }
        if (nBytes != sizeof(myfcb)) {
            printf("Data object has unexpected size. Doing nothing.\n");
            exit(-1);
        }
    }
}

void shutdown_fs() {
    unqlite_close(pDb);
}

int main(int argc, char* argv[]) {
    int fuserc;
    struct myfs_state* myfs_internal_state;

    //Setup the log file and store the FILE* in the private data object for the file system.
    myfs_internal_state = malloc(sizeof(struct myfs_state));
    myfs_internal_state->logfile = init_log_file();

    //Initialise the file system. This is being done outside of fuse for ease of debugging.
    init_fs();

    // Now pass our function pointers over to FUSE, so they can be called whenever someone
    // tries to interact with our filesystem. The internal state contains a file handle
    // for the logging mechanism
    fuserc = fuse_main(argc, argv, &myfs_oper, myfs_internal_state);

    //Shutdown the file system.
    shutdown_fs();

    return fuserc;
}


/** Caching that does not work. */
/*
static int remove_unused_cache_entries(int to_remove) {
    int iLog = 1;
    LOG_FUNC("\t\tREMOVE UNUSED CACHE ENTRIES  to_remove=%d\n", to_remove);
    if (!is_cache_full()) {
        LOG_CLARIFY("\t\t\tcache not full\n");
        return 0;
    }

    shift_cache_refbits();
    for (int i = 0; i < to_remove; ++i) {
        cache_entry ce;
        int index = get_least_used_cache(&ce);
        LOG_GENERAL("\t\t\tce.key=\"");
        for (int j = 0; j < ce.key_size; ++j) {
            LOG_GENERAL("%c", ce.key[j]);
        }
        LOG_GENERAL("\"\n");
        LOG_GENERAL("\t\tputting in DB\n");
        int rc = unqlite_kv_store(pDb, ce.key, (int) ce.key_size, ce.value, ce.value_size);
        TEST_CONDITION(rc, "\tremove_unused_cache_entries - failed", -EIO);
        LOG_GENERAL("\t\tcalling remove %d from cache\n", index);
        remove_from_cache(index);

        LOG_GENERAL("\t\treturned from remove from cache\n");
    }

    return 0;
}

static int store2(void* key, int key_len, void* data, unqlite_int64 data_len) {
    int iLog = 1;
    LOG_FUNC("\tSTORE  key=\"");
    for (int i = 0; i < key_len; ++i) {
        LOG_FUNC("%c", ((char*) key)[i]);
    }
    LOG_FUNC("\"\n");

    if (get_from_cache(key, (size_t) key_len, NULL, NULL) == -1 && is_cache_full()) {
        LOG_CLARIFY("\t\tcache full\n");
        int CHECKED_CALL(remove_unused_cache_entries, MAX_CACHE_SIZE / 2 + 1);
    }
    put_in_cache(key, (size_t) key_len, data, (size_t) data_len);

    return 0;
}

static int fetch2(void* key, int key_len, void* data, unqlite_int64* data_len) {
    int iLog = 1;
    LOG_FUNC("\tFETCH key=\"");
    for (int i = 0; i < key_len; ++i) {
        LOG_FUNC("%c", ((char*) key)[i]);
    }
    LOG_FUNC("\"\n");


    size_t* data_received;
    int index = get_from_cache(key, (size_t) key_len, data, data_received);
    if (index == -1) {  // Get failed.
        LOG_CLARIFY("\t\tKey-Value pair is not in cache - get it from DB.\n");
        int rc = unqlite_kv_fetch(pDb, key, key_len, data, data_len);
        TEST_CONDITION(rc == UNQLITE_NOTFOUND, "\tfetch - entry not found", -ENOENT);
        TEST_CONDITION(rc == UNQLITE_IOERR, "\tfetch - os error", -EIO);
        TEST_CONDITION(rc == UNQLITE_NOMEM, "\tfetch - out of memory", -ENOMEM);

        if (cache_size == MAX_CACHE_SIZE) {
            CHECKED_CALL(remove_unused_cache_entries, MAX_CACHE_SIZE / 2 + 1);
        }
        put_in_cache(key, (size_t) key_len, data, (size_t) *data_len);
    }

    return 0;

}
//*/