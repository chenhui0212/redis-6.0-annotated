/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include <math.h>

/*-----------------------------------------------------------------------------
 * Hash type API
 *----------------------------------------------------------------------------*/

/* Check the length of a number of objects to see if we need to convert a
 * ziplist to a real hash. Note that we only check string encoded objects
 * as their string length can be queried in constant time. */
/* 检查指定范围内字符串参数的长度，看是否需要将哈希对象的编码方式由压缩列表改为哈希表。 */
void hashTypeTryConversion(robj *o, robj **argv, int start, int end) {
    int i;

    /* 如果对象的编码方式不是压缩列表，则直接返回。 */
    if (o->encoding != OBJ_ENCODING_ZIPLIST) return;

    /* 对要添加的参数长度进行检查（key 和 value 值），如果长度超过配置中限制的最大长度，
     * 则需要将哈希对象的编码方式转为哈希表。 */
    for (i = start; i <= end; i++) {
        if (sdsEncodedObject(argv[i]) &&
            sdslen(argv[i]->ptr) > server.hash_max_ziplist_value)
        {
            /* 转换哈希对象的编码方式为哈希表 */
            hashTypeConvert(o, OBJ_ENCODING_HT);
            break;
        }
    }
}

/* Get the value from a ziplist encoded hash, identified by field.
 * Returns -1 when the field cannot be found. */
/* 从一个编码方式为压缩列表的哈希对象中，获取关联指定 field 的值。
 * 如果 field 不存在，则返回 -1。 */
int hashTypeGetFromZiplist(robj *o, sds field,
                           unsigned char **vstr,
                           unsigned int *vlen,
                           long long *vll)
{
    unsigned char *zl, *fptr = NULL, *vptr = NULL;
    int ret;

    serverAssert(o->encoding == OBJ_ENCODING_ZIPLIST);

    zl = o->ptr;
    fptr = ziplistIndex(zl, ZIPLIST_HEAD);
    if (fptr != NULL) {
        /* 查询 filed 值所在的地址 */
        fptr = ziplistFind(fptr, (unsigned char*)field, sdslen(field), 1);
        if (fptr != NULL) {
            /* Grab pointer to the value (fptr points to the field) */
            /* 获取 value 值所在的地址 */
            vptr = ziplistNext(zl, fptr);
            serverAssert(vptr != NULL);
        }
    }

    if (vptr != NULL) {
        /* 获取 value 值 */
        ret = ziplistGet(vptr, vstr, vlen, vll);
        serverAssert(ret);
        return 0;
    }

    return -1;
}

/* Get the value from a hash table encoded hash, identified by field.
 * Returns NULL when the field cannot be found, otherwise the SDS value
 * is returned. */
/* 从一个编码方式为哈希表的哈希对象中，获取关联指定 field 的值。
 * 如果 field 不存在，则返回 NULL，否则，返回 sds 值。 */
sds hashTypeGetFromHashTable(robj *o, sds field) {
    dictEntry *de;

    serverAssert(o->encoding == OBJ_ENCODING_HT);

    de = dictFind(o->ptr, field);
    if (de == NULL) return NULL;
    return dictGetVal(de);
}

/* Higher level function of hashTypeGet*() that returns the hash value
 * associated with the specified field. If the field is found C_OK
 * is returned, otherwise C_ERR. The returned object is returned by
 * reference in either *vstr and *vlen if it's returned in string form,
 * or stored in *vll if it's returned as a number.
 *
 * If *vll is populated *vstr is set to NULL, so the caller
 * can always check the function return by checking the return value
 * for C_OK and checking if vll (or vstr) is NULL. */
/* 返回哈希表中关联指定 field 的值。
 * 如果 feild 存在，返回 OK，否则返回 ERR。
 * 如果返回值是字符串类型，则将返回值存放到 '*vstr' 和 '*vlen' 中，如果是数字类型，
 * 则将返回值存放到 '*vll' 中。 */
int hashTypeGetValue(robj *o, sds field, unsigned char **vstr, unsigned int *vlen, long long *vll) {
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        *vstr = NULL;
        /* 从压缩列表中获取 field 关联值 */
        if (hashTypeGetFromZiplist(o, field, vstr, vlen, vll) == 0)
            return C_OK;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        sds value;
        /* 从哈希表中获取 field 关联值 */
        if ((value = hashTypeGetFromHashTable(o, field)) != NULL) {
            *vstr = (unsigned char*) value;
            *vlen = sdslen(value);
            return C_OK;
        }
    } else {
        serverPanic("Unknown hash encoding");
    }
    return C_ERR;
}

/* Like hashTypeGetValue() but returns a Redis object, which is useful for
 * interaction with the hash type outside t_hash.c.
 * The function returns NULL if the field is not found in the hash. Otherwise
 * a newly allocated string object with the value is returned. */
robj *hashTypeGetValueObject(robj *o, sds field) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;

    if (hashTypeGetValue(o,field,&vstr,&vlen,&vll) == C_ERR) return NULL;
    if (vstr) return createStringObject((char*)vstr,vlen);
    else return createStringObjectFromLongLong(vll);
}

/* Higher level function using hashTypeGet*() to return the length of the
 * object associated with the requested field, or 0 if the field does not
 * exist. */
/* 返回哈希对象中关联指定 field 的值的长度，如果 field 不存在，返回 0。 */
size_t hashTypeGetValueLength(robj *o, sds field) {
    size_t len = 0;
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0)
            len = vstr ? vlen : sdigits10(vll);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        sds aux;

        if ((aux = hashTypeGetFromHashTable(o, field)) != NULL)
            len = sdslen(aux);
    } else {
        serverPanic("Unknown hash encoding");
    }
    return len;
}

/* Test if the specified field exists in the given hash. Returns 1 if the field
 * exists, and 0 when it doesn't. */
/* 检查给定的哈希对象中指定 field 是否存在， */
int hashTypeExists(robj *o, sds field) {
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        if (hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll) == 0) return 1;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        if (hashTypeGetFromHashTable(o, field) != NULL) return 1;
    } else {
        serverPanic("Unknown hash encoding");
    }
    return 0;
}

/* Add a new field, overwrite the old with the new value if it already exists.
 * Return 0 on insert and 1 on update.
 *
 * By default, the key and value SDS strings are copied if needed, so the
 * caller retains ownership of the strings passed. However this behavior
 * can be effected by passing appropriate flags (possibly bitwise OR-ed):
 *
 * HASH_SET_TAKE_FIELD -- The SDS field ownership passes to the function.
 * HASH_SET_TAKE_VALUE -- The SDS value ownership passes to the function.
 *
 * When the flags are used the caller does not need to release the passed
 * SDS string(s). It's up to the function to use the string to create a new
 * entry or to free the SDS string before returning to the caller.
 *
 * HASH_SET_COPY corresponds to no flags passed, and means the default
 * semantics of copying the values if needed.
 *
 */
/* 添加或更新哈希对象中指定 field 值，插入数据返回 0，更新数据返回 1。
 *
 * 默认情况下，函数创建并使用 field 和 value 值的副本进行保存，调用方可以通过传递合适的
 * flags 值来达到想要的行为。
 * 
 * HASH_SET_TAKE_FIELD 表示直接使用 field 参数值进行保存。
 * HASH_SET_TAKE_VALUE 表示直接使用 value 参数值进行保存。
 * 
 * HASH_SET_COPY 对应未设置的 flags 参数，表示默认是使用参数值的副本进行保存的。
 * */
#define HASH_SET_TAKE_FIELD (1<<0)
#define HASH_SET_TAKE_VALUE (1<<1)
#define HASH_SET_COPY 0
int hashTypeSet(robj *o, sds field, sds value, int flags) {
    int update = 0;

    /* 编码方式为压缩列表 */
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr, *vptr;

        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            /* 查询 field 所在地址 */
            fptr = ziplistFind(fptr, (unsigned char*)field, sdslen(field), 1);
            if (fptr != NULL) {
                /* Grab pointer to the value (fptr points to the field) */
                /* 获取 value 所在地址 */
                vptr = ziplistNext(zl, fptr);
                serverAssert(vptr != NULL);
                update = 1;

                /* Delete value */
                /* 删除 value 值 */
                zl = ziplistDelete(zl, &vptr);

                /* Insert new value */
                /* 插入新的 value 值 */
                zl = ziplistInsert(zl, vptr, (unsigned char*)value,
                        sdslen(value));
            }
        }

        /* 如果 field 不存在 */
        if (!update) {
            /* Push new field/value pair onto the tail of the ziplist */
            /* 将新的 field 和 value 值依次保存到 ziplist 结尾 */
            zl = ziplistPush(zl, (unsigned char*)field, sdslen(field),
                    ZIPLIST_TAIL);
            zl = ziplistPush(zl, (unsigned char*)value, sdslen(value),
                    ZIPLIST_TAIL);
        }
        /* 考虑 zl 地址可能发生变化，重新赋值。 */
        o->ptr = zl;

        /* Check if the ziplist needs to be converted to a hash table */
        /* 检查是否需要编码方式由压缩列表改为哈希表 */
        if (hashTypeLength(o) > server.hash_max_ziplist_entries)
            hashTypeConvert(o, OBJ_ENCODING_HT);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        /* 编码方式为哈希表 */

        /* 查询 field 是否存在于哈希表中 */
        dictEntry *de = dictFind(o->ptr,field);
        if (de) {
            sdsfree(dictGetVal(de));
            if (flags & HASH_SET_TAKE_VALUE) {
                /* 直接将参数 value 保存到哈希表中 */
                dictGetVal(de) = value;
                value = NULL;
            } else {
                /* 将参数 value 值的副本保存到哈希表中 */
                dictGetVal(de) = sdsdup(value);
            }
            update = 1;
        } else {
            /* field 不存在 */
            sds f,v;
            /* 根据参数 flags 值，决定是直接使用参数值，还是创建参数值的副本，
             * 来保存到哈希表中。 */
            if (flags & HASH_SET_TAKE_FIELD) {
                f = field;
                field = NULL;
            } else {
                f = sdsdup(field);
            }
            if (flags & HASH_SET_TAKE_VALUE) {
                v = value;
                value = NULL;
            } else {
                v = sdsdup(value);
            }
            dictAdd(o->ptr,f,v);
        }
    } else {
        serverPanic("Unknown hash encoding");
    }

    /* Free SDS strings we did not referenced elsewhere if the flags
     * want this function to be responsible. */
    if (flags & HASH_SET_TAKE_FIELD && field) sdsfree(field);
    if (flags & HASH_SET_TAKE_VALUE && value) sdsfree(value);
    return update;
}

/* Delete an element from a hash.
 * Return 1 on deleted and 0 on not found. */
int hashTypeDelete(robj *o, sds field) {
    int deleted = 0;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl, *fptr;

        zl = o->ptr;
        fptr = ziplistIndex(zl, ZIPLIST_HEAD);
        if (fptr != NULL) {
            fptr = ziplistFind(fptr, (unsigned char*)field, sdslen(field), 1);
            if (fptr != NULL) {
                zl = ziplistDelete(zl,&fptr); /* Delete the key. */
                zl = ziplistDelete(zl,&fptr); /* Delete the value. */
                o->ptr = zl;
                deleted = 1;
            }
        }
    } else if (o->encoding == OBJ_ENCODING_HT) {
        if (dictDelete((dict*)o->ptr, field) == C_OK) {
            deleted = 1;

            /* Always check if the dictionary needs a resize after a delete. */
            if (htNeedsResize(o->ptr)) dictResize(o->ptr);
        }

    } else {
        serverPanic("Unknown hash encoding");
    }
    return deleted;
}

/* Return the number of elements in a hash. */
/* 返回哈希对象中元素的数量 */
unsigned long hashTypeLength(const robj *o) {
    unsigned long length = ULONG_MAX;

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        length = ziplistLen(o->ptr) / 2;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        length = dictSize((const dict*)o->ptr);
    } else {
        serverPanic("Unknown hash encoding");
    }
    return length;
}

/* 创建指定哈希对象的迭代器 */
hashTypeIterator *hashTypeInitIterator(robj *subject) {
    hashTypeIterator *hi = zmalloc(sizeof(hashTypeIterator));
    hi->subject = subject;
    hi->encoding = subject->encoding;

    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        hi->fptr = NULL;
        hi->vptr = NULL;
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        hi->di = dictGetIterator(subject->ptr);
    } else {
        serverPanic("Unknown hash encoding");
    }
    return hi;
}

void hashTypeReleaseIterator(hashTypeIterator *hi) {
    if (hi->encoding == OBJ_ENCODING_HT)
        dictReleaseIterator(hi->di);
    zfree(hi);
}

/* Move to the next entry in the hash. Return C_OK when the next entry
 * could be found and C_ERR when the iterator reaches the end. */
/* 移动迭代器中当前指针指向下一个数据项，如果下一个数据项存在，返回 OK，如果到达结尾，
 * 返回 ERR。 */
int hashTypeNext(hashTypeIterator *hi) {
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl;
        unsigned char *fptr, *vptr;

        zl = hi->subject->ptr;
        fptr = hi->fptr;
        vptr = hi->vptr;

        if (fptr == NULL) {
            /* Initialize cursor */
            /* 初始化指针 */
            serverAssert(vptr == NULL);
            fptr = ziplistIndex(zl, 0);
        } else {
            /* Advance cursor */
            /* 指向下一个数据项 */
            serverAssert(vptr != NULL);
            fptr = ziplistNext(zl, vptr);
        }
        if (fptr == NULL) return C_ERR;

        /* Grab pointer to the value (fptr points to the field) */
        /* 获取 vlaue 的地址 */
        vptr = ziplistNext(zl, fptr);
        serverAssert(vptr != NULL);

        /* fptr, vptr now point to the first or next pair */
        hi->fptr = fptr;
        hi->vptr = vptr;
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        if ((hi->de = dictNext(hi->di)) == NULL) return C_ERR;
    } else {
        serverPanic("Unknown hash encoding");
    }
    return C_OK;
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a ziplist. Prototype is similar to `hashTypeGetFromZiplist`. */
/* 从编码方式为压缩列表的哈希对象迭代器指针中，读取出 key 或者 value 值。 */
void hashTypeCurrentFromZiplist(hashTypeIterator *hi, int what,
                                unsigned char **vstr,
                                unsigned int *vlen,
                                long long *vll)
{
    int ret;

    serverAssert(hi->encoding == OBJ_ENCODING_ZIPLIST);

    if (what & OBJ_HASH_KEY) {
        /* 读取出 key 值 */
        ret = ziplistGet(hi->fptr, vstr, vlen, vll);
        serverAssert(ret);
    } else {
        /* 读取出 value 值 */
        ret = ziplistGet(hi->vptr, vstr, vlen, vll);
        serverAssert(ret);
    }
}

/* Get the field or value at iterator cursor, for an iterator on a hash value
 * encoded as a hash table. Prototype is similar to
 * `hashTypeGetFromHashTable`. */
/* 从编码方式为哈希表的哈希对象迭代器指针中，读取出 key 或者 value 值。 */
sds hashTypeCurrentFromHashTable(hashTypeIterator *hi, int what) {
    serverAssert(hi->encoding == OBJ_ENCODING_HT);

    if (what & OBJ_HASH_KEY) {
        return dictGetKey(hi->de);
    } else {
        return dictGetVal(hi->de);
    }
}

/* Higher level function of hashTypeCurrent*() that returns the hash value
 * at current iterator position.
 *
 * The returned element is returned by reference in either *vstr and *vlen if
 * it's returned in string form, or stored in *vll if it's returned as
 * a number.
 *
 * If *vll is populated *vstr is set to NULL, so the caller
 * can always check the function return by checking the return value
 * type checking if vstr == NULL. */
/* 返回哈希对象迭代器中当前位置的数据值。
 * 如果返回值类型为字符串，将其保存在 '*vstr' 和 '*vlen' 中，
 * 如果返回值类型是数字，则将其保存在 '*vll' 中。 */
void hashTypeCurrentObject(hashTypeIterator *hi, int what, unsigned char **vstr, unsigned int *vlen, long long *vll) {
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        *vstr = NULL;
        /* 从 ziplist 读取值 */
        hashTypeCurrentFromZiplist(hi, what, vstr, vlen, vll);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        /* 从 dict 读取值 */
        sds ele = hashTypeCurrentFromHashTable(hi, what);
        *vstr = (unsigned char*) ele;
        *vlen = sdslen(ele);
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* Return the key or value at the current iterator position as a new
 * SDS string. */
/* 从迭代器当前位置获取出 key 或 value 的字符串值 */
sds hashTypeCurrentObjectNewSds(hashTypeIterator *hi, int what) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vll;

    hashTypeCurrentObject(hi,what,&vstr,&vlen,&vll);
    if (vstr) return sdsnewlen(vstr,vlen);
    return sdsfromlonglong(vll);
}

/* 查询指定 key 的哈希对象，如果存在，直接返回该对象，
 * 如果不存在，则新建一个哈希对象，并保存入库，最后返回新建对象。 */
robj *hashTypeLookupWriteOrCreate(client *c, robj *key) {
    robj *o = lookupKeyWrite(c->db,key);
    if (o == NULL) {
        o = createHashObject();
        dbAdd(c->db,key,o);
    } else {
        if (o->type != OBJ_HASH) {
            addReply(c,shared.wrongtypeerr);
            return NULL;
        }
    }
    return o;
}

/* 转换编码方式为压缩列表的哈希对象的编码方式 */
void hashTypeConvertZiplist(robj *o, int enc) {
    serverAssert(o->encoding == OBJ_ENCODING_ZIPLIST);

    /* 如果转换后的编码方式仍为 ZIPLIST，则不做任何处理。 */
    if (enc == OBJ_ENCODING_ZIPLIST) {
        /* Nothing to do... */

    } else if (enc == OBJ_ENCODING_HT) {
        /* 编码方式转换为哈希表 */
        hashTypeIterator *hi;
        dict *dict;
        int ret;

        /* 初始化当前哈希对象的迭代器 */
        hi = hashTypeInitIterator(o);
        /* 新建哈希表 */
        dict = dictCreate(&hashDictType, NULL);

        while (hashTypeNext(hi) != C_ERR) {
            sds key, value;

            /* 从迭代器中读取出当前的 key 和 value 值 */
            key = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_KEY);
            value = hashTypeCurrentObjectNewSds(hi,OBJ_HASH_VALUE);
            /* 添加键值对到哈希表中 */
            ret = dictAdd(dict, key, value);
            if (ret != DICT_OK) {
                /* 当 ziplist 中存在相同 key 值的键值对时，抛出异常。 */
                serverLogHexDump(LL_WARNING,"ziplist with dup elements dump",
                    o->ptr,ziplistBlobLen(o->ptr));
                serverPanic("Ziplist corruption detected");
            }
        }
        hashTypeReleaseIterator(hi);
        zfree(o->ptr);
        o->encoding = OBJ_ENCODING_HT;
        o->ptr = dict;
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* 转换哈希对象的编码方式为指定编码方式 */
void hashTypeConvert(robj *o, int enc) {
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        hashTypeConvertZiplist(o, enc);
    } else if (o->encoding == OBJ_ENCODING_HT) {
        serverPanic("Not implemented");
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/*-----------------------------------------------------------------------------
 * Hash type commands
 *----------------------------------------------------------------------------*/

/* HSETNX 命令执行函数 */
void hsetnxCommand(client *c) {
    robj *o;
    /* 获取或新建哈希对象 */
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    /* 如果需要的话，转换哈希对象的编码方式 */
    hashTypeTryConversion(o,c->argv,2,3);

    /* 判断要添加的 field 值是否存在 */
    if (hashTypeExists(o, c->argv[2]->ptr)) {
        /* 存在，则返回 0 给客户端 */
        addReply(c, shared.czero);
    } else {
        /* 否则，将 field-value 对保存到哈希对象中 */
        hashTypeSet(o,c->argv[2]->ptr,c->argv[3]->ptr,HASH_SET_COPY);
        addReply(c, shared.cone);
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
        server.dirty++;
    }
}

/* HSET 命令执行函数 */
void hsetCommand(client *c) {
    int i, created = 0;
    robj *o;

    /* 检查 field-value 参数是否成对出现 */
    if ((c->argc % 2) == 1) {
        addReplyError(c,"wrong number of arguments for HMSET");
        return;
    }

    /* 获取或新建哈希对象 */
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    /* 如果需要的话，转换哈希对象的编码方式 */
    hashTypeTryConversion(o,c->argv,2,c->argc-1);

    /* 依次添加所有的 field-value 对 */
    for (i = 2; i < c->argc; i += 2)
        created += !hashTypeSet(o,c->argv[i]->ptr,c->argv[i+1]->ptr,HASH_SET_COPY);

    /* HMSET (deprecated) and HSET return value is different. */
    /* HMSET（废弃） 和 HSET 命令返回的内容不同 */
    char *cmdname = c->argv[0]->ptr;
    if (cmdname[1] == 's' || cmdname[1] == 'S') {
        /* HSET */
        addReplyLongLong(c, created);
    } else {
        /* HMSET */
        addReply(c, shared.ok);
    }
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hset",c->argv[1],c->db->id);
    server.dirty++;
}

/* HINCRBY 命令执行函数
 *
 * 增加指定 key 的哈希表中指定 field 的数值，如果 key 不存在，会创建一个新的哈希表并与 key 关联。
 * 如果 field 不存在，则 field 的值在命令执行前被设置为 0。 */
void hincrbyCommand(client *c) {
    long long value, incr, oldvalue;
    robj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;

    /* 获取 incr 参数的值 */
    if (getLongLongFromObjectOrReply(c,c->argv[3],&incr,NULL) != C_OK) return;
    /* 获取或新建哈希对象 */
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    /* 获取 field 关联值 */
    if (hashTypeGetValue(o,c->argv[2]->ptr,&vstr,&vlen,&value) == C_OK) {
        /* 查询结果是字符串 */
        if (vstr) {
            /* 将字符串值转换为整型值，保存到 value 中。 */
            if (string2ll((char*)vstr,vlen,&value) == 0) {
                addReplyError(c,"hash value is not an integer");
                return;
            }
        } /* Else hashTypeGetValue() already stored it into &value */
    } else {
        /* field 不存在，设置 value 为 0。 */
        value = 0;
    }

    oldvalue = value;
    /* 判断计算结果是否会溢出 */
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }
    /* 计算并保存结果 */
    value += incr;
    new = sdsfromlonglong(value);
    hashTypeSet(o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE);
    /* 返回结果值给客户端 */
    addReplyLongLong(c,value);
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrby",c->argv[1],c->db->id);
    server.dirty++;
}

/* HINCRBYFLOAT 命令执行函数 */
void hincrbyfloatCommand(client *c) {
    long double value, incr;
    long long ll;
    robj *o;
    sds new;
    unsigned char *vstr;
    unsigned int vlen;

    /* 获取 incr 参数的值 */
    if (getLongDoubleFromObjectOrReply(c,c->argv[3],&incr,NULL) != C_OK) return;
    /* 获取或新建哈希对象 */
    if ((o = hashTypeLookupWriteOrCreate(c,c->argv[1])) == NULL) return;
    /* 获取 field 关联值 */
    if (hashTypeGetValue(o,c->argv[2]->ptr,&vstr,&vlen,&ll) == C_OK) {
        /* 如果查询结果是字符串，则将其值转换为整型值后，保存到 value 中。 */
        if (vstr) {
            if (string2ld((char*)vstr,vlen,&value) == 0) {
                addReplyError(c,"hash value is not a float");
                return;
            }
        } else {
            value = (long double)ll;
        }
    } else {
        /* field 不存在，设置 value 为 0。 */
        value = 0;
    }

    /* 计算结果，并判断是否溢出。 */
    value += incr;
    if (isnan(value) || isinf(value)) {
        addReplyError(c,"increment would produce NaN or Infinity");
        return;
    }

    /* 保存结果，并将结果值返回给客户端。 */
    char buf[MAX_LONG_DOUBLE_CHARS];
    int len = ld2string(buf,sizeof(buf),value,LD_STR_HUMAN);
    new = sdsnewlen(buf,len);
    hashTypeSet(o,c->argv[2]->ptr,new,HASH_SET_TAKE_VALUE);
    addReplyBulkCBuffer(c,buf,len);
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_HASH,"hincrbyfloat",c->argv[1],c->db->id);
    server.dirty++;

    /* Always replicate HINCRBYFLOAT as an HSET command with the final value
     * in order to make sure that differences in float pricision or formatting
     * will not create differences in replicas or after an AOF restart. */
    /* 在传播 HINCRBYFLOAT 命令时，总是用 HSET 命令来替换 HINCRBYFLOAT 命令，
     * 从而防止因为不同的浮点精度或格式化导致副本之间或 AOF 重启后的数据不一致问题。 */
    robj *aux, *newobj;
    aux = createStringObject("HSET",4);
    newobj = createRawStringObject(buf,len);
    rewriteClientCommandArgument(c,0,aux);
    decrRefCount(aux);
    rewriteClientCommandArgument(c,3,newobj);
    decrRefCount(newobj);
}

/* 查询哈希对象中指定 field 的关联值，并返回给客户端。 */
static void addHashFieldToReply(client *c, robj *o, sds field) {
    int ret;

    /* 哈希对象为空 */
    if (o == NULL) {
        addReplyNull(c);
        return;
    }

    /* 编码方式为压缩列表 */
    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        /* 从压缩列表中获取指定 field 关联值，并返回给客户端。 */
        ret = hashTypeGetFromZiplist(o, field, &vstr, &vlen, &vll);
        if (ret < 0) {
            addReplyNull(c);
        } else {
            if (vstr) {
                addReplyBulkCBuffer(c, vstr, vlen);
            } else {
                addReplyBulkLongLong(c, vll);
            }
        }

    } else if (o->encoding == OBJ_ENCODING_HT) {
        /* 从哈希表中获取指定 field 关联值，并返回给客户端。 */
        sds value = hashTypeGetFromHashTable(o, field);
        if (value == NULL)
            addReplyNull(c);
        else
            addReplyBulkCBuffer(c, value, sdslen(value));
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* HGET 命令执行函数 */
void hgetCommand(client *c) {
    robj *o;

    /* 获取出哈希对象，并进行类型检查。 */
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp])) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    /* 获取 field 关联值，并返回给客户端。 */
    addHashFieldToReply(c, o, c->argv[2]->ptr);
}

/* HMGET 命令执行函数 */
void hmgetCommand(client *c) {
    robj *o;
    int i;

    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * hashes, where HMGET should respond with a series of null bulks. */
    /* 当 key 不存在时，不会停止查询，而是应该返回一系列的空值。 */
    o = lookupKeyRead(c->db, c->argv[1]);
    if (o != NULL && o->type != OBJ_HASH) {
        addReply(c, shared.wrongtypeerr);
        return;
    }

    /* 依次将查询结果值返回给客户端 */
    addReplyArrayLen(c, c->argc-2);
    for (i = 2; i < c->argc; i++) {
        addHashFieldToReply(c, o, c->argv[i]->ptr);
    }
}

/* HDEL 命令执行函数 */
void hdelCommand(client *c) {
    robj *o;
    int j, deleted = 0, keyremoved = 0;

    /* 获取出哈希对象，并进行类型检查。 */
    if ((o = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    /* 依次删除指定 field 的 field-value 对 */
    for (j = 2; j < c->argc; j++) {
        if (hashTypeDelete(o,c->argv[j]->ptr)) {
            deleted++;
            /* 移除空的哈希 */
            if (hashTypeLength(o) == 0) {
                dbDelete(c->db,c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }
    if (deleted) {
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_HASH,"hdel",c->argv[1],c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],
                                c->db->id);
        server.dirty += deleted;
    }
    /* 将成功移除的 field-value 对的数量返回给客户端 */
    addReplyLongLong(c,deleted);
}

/* HLEN 命令执行函数 */
void hlenCommand(client *c) {
    robj *o;

    /* 获取出哈希对象，并进行类型检查。 */
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    /* 返回哈希对象中字段数量给客户端 */
    addReplyLongLong(c,hashTypeLength(o));
}

/* HSTRLEN 命令执行函数 */
void hstrlenCommand(client *c) {
    robj *o;

    /* 获取出哈希对象，并进行类型检查。 */
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;
    /* 返回指定 field 的 value 的字符串长度 */
    addReplyLongLong(c,hashTypeGetValueLength(o,c->argv[2]->ptr));
}

/* 返回迭代器当前指针位置数据值返回给客户端 */
static void addHashIteratorCursorToReply(client *c, hashTypeIterator *hi, int what) {
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        /* 从 ziplist 中读取值 */
        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr)
            addReplyBulkCBuffer(c, vstr, vlen);
        else
            addReplyBulkLongLong(c, vll);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        /* 从 dict 中读取值 */
        sds value = hashTypeCurrentFromHashTable(hi, what);
        addReplyBulkCBuffer(c, value, sdslen(value));
    } else {
        serverPanic("Unknown hash encoding");
    }
}

/* HGETALL 命令通用执行函数 */
void genericHgetallCommand(client *c, int flags) {
    robj *o;
    hashTypeIterator *hi;
    int length, count = 0;

    /* 获取出哈希对象，并进行类型检查。 */
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptymap[c->resp]))
        == NULL || checkType(c,o,OBJ_HASH)) return;

    /* We return a map if the user requested keys and values, like in the
     * HGETALL case. Otherwise to use a flat array makes more sense. */
    /* 当前用户请求 key 和 value 值时，返回一个 map 对象，否则返回一个数组对象。 */
    length = hashTypeLength(o);
    if (flags & OBJ_HASH_KEY && flags & OBJ_HASH_VALUE) {
        addReplyMapLen(c, length);
    } else {
        addReplyArrayLen(c, length);
    }

    /* 迭代全部元素，并将取出的值返回。 */
    hi = hashTypeInitIterator(o);
    while (hashTypeNext(hi) != C_ERR) {
        /* 如果指定 key，返回 key 值。 */
        if (flags & OBJ_HASH_KEY) {
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_KEY);
            count++;
        }
        /* 如果指定 value，返回 value 值。 */
        if (flags & OBJ_HASH_VALUE) {
            addHashIteratorCursorToReply(c, hi, OBJ_HASH_VALUE);
            count++;
        }
    }

    hashTypeReleaseIterator(hi);

    /* Make sure we returned the right number of elements. */
    /* 确保正确数量的返回值数量 */
    if (flags & OBJ_HASH_KEY && flags & OBJ_HASH_VALUE) count /= 2;
    serverAssert(count == length);
}

/* HKEYS 命令执行函数 */
void hkeysCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_KEY);
}

/* HVALS 命令执行函数 */
void hvalsCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_VALUE);
}

/* HGETALL 命令执行函数 */
void hgetallCommand(client *c) {
    genericHgetallCommand(c,OBJ_HASH_KEY|OBJ_HASH_VALUE);
}

/* HEXISTS 命令执行函数 */
void hexistsCommand(client *c) {
    robj *o;
    /* 获取出哈希对象，并进行类型检查。 */
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;

    /* 返回哈希对象是否存在的结果给客户端 */
    addReply(c, hashTypeExists(o,c->argv[2]->ptr) ? shared.cone : shared.czero);
}

/* HSCAN 命令执行函数 */
void hscanCommand(client *c) {
    robj *o;
    unsigned long cursor;

    /* 解析游标参数 */
    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == C_ERR) return;
    /* 获取出哈希对象，并进行类型检查。 */
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,o,OBJ_HASH)) return;
    /* 返回哈希中指定游标位置的数据 */
    scanGenericCommand(c,o,cursor);
}
