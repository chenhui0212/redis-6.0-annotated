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

/*-----------------------------------------------------------------------------
 * Set Commands
 *----------------------------------------------------------------------------*/

void sunionDiffGenericCommand(client *c, robj **setkeys, int setnum,
                              robj *dstkey, int op);

/* Factory method to return a set that *can* hold "value". When the object has
 * an integer-encodable value, an intset will be returned. Otherwise a regular
 * hash table. */
/* 根据指定的 value 值，新建一个整型集合对象，或者集合对象，并返回。 */
robj *setTypeCreate(sds value) {
    /* 如果 value 值能转换为整型，则返回新建一个整型集合对象，
     * 否则，返回一个集合对象。 */
    if (isSdsRepresentableAsLongLong(value,NULL) == C_OK)
        return createIntsetObject();
    return createSetObject();
}

/* Add the specified value into a set.
 *
 * If the value was already member of the set, nothing is done and 0 is
 * returned, otherwise the new element is added and 1 is returned. */
/* 添加指定的值到集合中。
 * 
 * 如果指定的值已经存在于集合中，则不做任何事，并返回 0，
 * 否则，将指定值添加到集合中，并返回 1。 */
int setTypeAdd(robj *subject, sds value) {
    long long llval;
    /* 如果集合的编码类型为哈希表 */
    if (subject->encoding == OBJ_ENCODING_HT) {
        dict *ht = subject->ptr;
        /* 添加字典节点 */
        dictEntry *de = dictAddRaw(ht,value,NULL);
        /* 不为空，说明指定值不存在于集合中。 */
        if (de) {
            /* 设置 key 和 value 值 */
            dictSetKey(ht,de,sdsdup(value));
            dictSetVal(ht,de,NULL);
            return 1;
        }
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        /* 如果集合的编码类型为整型集合 */

        /* 如果指定值可以转换为整型的话，则将转换后的整型值保存到整型集合中。 */
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {
            uint8_t success = 0;
            subject->ptr = intsetAdd(subject->ptr,llval,&success);
            if (success) {
                /* Convert to regular set when the intset contains
                 * too many entries. */
                /* 如果整型集合包含过多的元素，则将其转换为一般集合。 */
                if (intsetLen(subject->ptr) > server.set_max_intset_entries)
                    setTypeConvert(subject,OBJ_ENCODING_HT);
                return 1;
            }
        } else {
            /* Failed to get integer from object, convert to regular set. */
            /* 指定值不能转换为整型值，只能将整型集合转换为一般集合。 */
            setTypeConvert(subject,OBJ_ENCODING_HT);

            /* The set *was* an intset and this value is not integer
             * encodable, so dictAdd should always work. */
            /* 由于之前是整型集合，而当前添加的值并不能转换为整型，
             * 所以可以安全的将其添加到字典中。 */
            serverAssert(dictAdd(subject->ptr,sdsdup(value),NULL) == DICT_OK);
            return 1;
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}

/* 移除集合对象中指定元素 */
int setTypeRemove(robj *setobj, sds value) {
    long long llval;
    if (setobj->encoding == OBJ_ENCODING_HT) {
        /* 从字典中移除指定 key 的节点 */
        if (dictDelete(setobj->ptr,value) == DICT_OK) {
            /* 判断移除节点后的字典是否需要缩小大小 */
            if (htNeedsResize(setobj->ptr)) dictResize(setobj->ptr);
            return 1;
        }
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {
        /* 对于整型集合，只有当 key 可以转换为整型时，才会进行移除操作。 */
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {
            int success;
            setobj->ptr = intsetRemove(setobj->ptr,llval,&success);
            if (success) return 1;
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}

/* 判断指定元素是否存在于集合中 */
int setTypeIsMember(robj *subject, sds value) {
    long long llval;
    if (subject->encoding == OBJ_ENCODING_HT) {
        /* 字典中查询 */
        return dictFind((dict*)subject->ptr,value) != NULL;
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        /* 如果元素值可以装换为整型，则在整型集合中查询。 */
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {
            return intsetFind((intset*)subject->ptr,llval);
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}

/* 初始化集合的迭代器 */
setTypeIterator *setTypeInitIterator(robj *subject) {
    setTypeIterator *si = zmalloc(sizeof(setTypeIterator));
    si->subject = subject;
    si->encoding = subject->encoding;
    if (si->encoding == OBJ_ENCODING_HT) {
        si->di = dictGetIterator(subject->ptr);
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
        si->ii = 0;
    } else {
        serverPanic("Unknown set encoding");
    }
    return si;
}

/* 释放指定集合的迭代器 */
void setTypeReleaseIterator(setTypeIterator *si) {
    if (si->encoding == OBJ_ENCODING_HT)
        dictReleaseIterator(si->di);
    zfree(si);
}

/* Move to the next entry in the set. Returns the object at the current
 * position.
 *
 * Since set elements can be internally be stored as SDS strings or
 * simple arrays of integers, setTypeNext returns the encoding of the
 * set object you are iterating, and will populate the appropriate pointer
 * (sdsele) or (llele) accordingly.
 *
 * Note that both the sdsele and llele pointers should be passed and cannot
 * be NULL since the function will try to defensively populate the non
 * used field with values which are easy to trap if misused.
 *
 * When there are no longer elements -1 is returned. */
/* 返回当前迭代器的指向的元素，并将迭代器指针移向下一个元素。 */
int setTypeNext(setTypeIterator *si, sds *sdsele, int64_t *llele) {
    if (si->encoding == OBJ_ENCODING_HT) {
        dictEntry *de = dictNext(si->di);
        if (de == NULL) return -1;
        *sdsele = dictGetKey(de);
        *llele = -123456789; /* Not needed. Defensive. */
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
        if (!intsetGet(si->subject->ptr,si->ii++,llele))
            return -1;
        *sdsele = NULL; /* Not needed. Defensive. */
    } else {
        serverPanic("Wrong set encoding in setTypeNext");
    }
    return si->encoding;
}

/* The not copy on write friendly version but easy to use version
 * of setTypeNext() is setTypeNextObject(), returning new SDS
 * strings. So if you don't retain a pointer to this object you should call
 * sdsfree() against it.
 *
 * This function is the way to go for write operations where COW is not
 * an issue. */
/* 是 setTypeNext() 函数的非 copy-on-write 的版本，总是返回一个新的 sds 字符串对象。 */
sds setTypeNextObject(setTypeIterator *si) {
    int64_t intele;
    sds sdsele;
    int encoding;

    /* 获取值 */
    encoding = setTypeNext(si,&sdsele,&intele);
    switch(encoding) {
        case -1:    return NULL;
        case OBJ_ENCODING_INTSET:
            return sdsfromlonglong(intele);
        case OBJ_ENCODING_HT:
            return sdsdup(sdsele);
        default:
            serverPanic("Unsupported encoding");
    }
    return NULL; /* just to suppress warnings */
}

/* Return random element from a non empty set.
 * The returned element can be a int64_t value if the set is encoded
 * as an "intset" blob of integers, or an SDS string if the set
 * is a regular set.
 *
 * The caller provides both pointers to be populated with the right
 * object. The return value of the function is the object->encoding
 * field of the object and is used by the caller to check if the
 * int64_t pointer or the redis object pointer was populated.
 *
 * Note that both the sdsele and llele pointers should be passed and cannot
 * be NULL since the function will try to defensively populate the non
 * used field with values which are easy to trap if misused. */
/* 从非空集合中返回一个随机元素 */
int setTypeRandomElement(robj *setobj, sds *sdsele, int64_t *llele) {
    if (setobj->encoding == OBJ_ENCODING_HT) {
        dictEntry *de = dictGetFairRandomKey(setobj->ptr);
        *sdsele = dictGetKey(de);
        *llele = -123456789; /* Not needed. Defensive. */
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {
        *llele = intsetRandom(setobj->ptr);
        *sdsele = NULL; /* Not needed. Defensive. */
    } else {
        serverPanic("Unknown set encoding");
    }
    return setobj->encoding;
}

/* 返回集合中元素数量 */
unsigned long setTypeSize(const robj *subject) {
    if (subject->encoding == OBJ_ENCODING_HT) {
        return dictSize((const dict*)subject->ptr);
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        return intsetLen((const intset*)subject->ptr);
    } else {
        serverPanic("Unknown set encoding");
    }
}

/* Convert the set to specified encoding. The resulting dict (when converting
 * to a hash table) is presized to hold the number of elements in the original
 * set. */
/* 将集合对象转换为指定编码方式 */
void setTypeConvert(robj *setobj, int enc) {
    setTypeIterator *si;
    serverAssertWithInfo(NULL,setobj,setobj->type == OBJ_SET &&
                             setobj->encoding == OBJ_ENCODING_INTSET);

    if (enc == OBJ_ENCODING_HT) {
        int64_t intele;
        dict *d = dictCreate(&setDictType,NULL);
        sds element;

        /* Presize the dict to avoid rehashing */
        /* 预先调整字典大小 */
        dictExpand(d,intsetLen(setobj->ptr));

        /* To add the elements we extract integers and create redis objects */
        /* 添加全部的元素到字典中 */
        si = setTypeInitIterator(setobj);
        while (setTypeNext(si,&element,&intele) != -1) {
            element = sdsfromlonglong(intele);
            serverAssert(dictAdd(d,element,NULL) == DICT_OK);
        }
        setTypeReleaseIterator(si);

        setobj->encoding = OBJ_ENCODING_HT;
        zfree(setobj->ptr);
        setobj->ptr = d;
    } else {
        serverPanic("Unsupported set conversion");
    }
}

/* SADD 命令执行函数 */
void saddCommand(client *c) {
    robj *set;
    int j, added = 0;

    /* 获取出集合对象 */
    set = lookupKeyWrite(c->db,c->argv[1]);
    if (set == NULL) {
        /* 对象为空，则新建集合对象，并保存入库。 */
        set = setTypeCreate(c->argv[2]->ptr);
        dbAdd(c->db,c->argv[1],set);
    } else {
        /* 检查对象类型 */
        if (set->type != OBJ_SET) {
            addReply(c,shared.wrongtypeerr);
            return;
        }
    }

    /* 依次将参数中的值添加到集合中 */
    for (j = 2; j < c->argc; j++) {
        /* 已存在的值不参与统计 */
        if (setTypeAdd(set,c->argv[j]->ptr)) added++;
    }
    if (added) {
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_SET,"sadd",c->argv[1],c->db->id);
    }
    server.dirty += added;
    /* 返回添加的元素数量给客户端 */
    addReplyLongLong(c,added);
}

/* SREM 命令执行函数 */
void sremCommand(client *c) {
    robj *set;
    int j, deleted = 0, keyremoved = 0;

    /* 获取出集合对象，并进行类型检查。 */
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,set,OBJ_SET)) return;

    /* 依次移除参数中指定需要移除的元素 */
    for (j = 2; j < c->argc; j++) {
        if (setTypeRemove(set,c->argv[j]->ptr)) {
            deleted++;
            /* 如果集合为空，则从库中移除集合对象。 */
            if (setTypeSize(set) == 0) {
                dbDelete(c->db,c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }
    if (deleted) {
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_SET,"srem",c->argv[1],c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],
                                c->db->id);
        server.dirty += deleted;
    }
    /* 返回成功移除元素的数量给客户端 */
    addReplyLongLong(c,deleted);
}

/* SMOVE 命令执行函数 */
void smoveCommand(client *c) {
    robj *srcset, *dstset, *ele;
    /* 获取源集合、目标集合和要移动的元素 */
    srcset = lookupKeyWrite(c->db,c->argv[1]);
    dstset = lookupKeyWrite(c->db,c->argv[2]);
    ele = c->argv[3];

    /* If the source key does not exist return 0 */
    /* 如果源集合不存在，返回 0 给客户端。 */
    if (srcset == NULL) {
        addReply(c,shared.czero);
        return;
    }

    /* If the source key has the wrong type, or the destination key
     * is set and has the wrong type, return with an error. */
    /* 如果源集合类型错误，或目标集合存在，且类型错误，返回错误信息给客户端。 */
    if (checkType(c,srcset,OBJ_SET) ||
        (dstset && checkType(c,dstset,OBJ_SET))) return;

    /* If srcset and dstset are equal, SMOVE is a no-op */
    /* 如果源集合和目标集合是同一个集合对象，不执行移动操作。 */
    if (srcset == dstset) {
        /* 如果元素存在，返回 1，不存在，返回 0。 */
        addReply(c,setTypeIsMember(srcset,ele->ptr) ?
            shared.cone : shared.czero);
        return;
    }

    /* If the element cannot be removed from the src set, return 0. */
    /* 如果元素不存在于集合中，返回 0 给客户端。 */
    if (!setTypeRemove(srcset,ele->ptr)) {
        addReply(c,shared.czero);
        return;
    }
    notifyKeyspaceEvent(NOTIFY_SET,"srem",c->argv[1],c->db->id);

    /* Remove the src set from the database when empty */
    /* 如果源集合为空，则从库中移除集合对象。 */
    if (setTypeSize(srcset) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    /* Create the destination set when it doesn't exist */
    /* 如果目标集合不存在，则新建目标集合。 */
    if (!dstset) {
        dstset = setTypeCreate(ele->ptr);
        dbAdd(c->db,c->argv[2],dstset);
    }

    signalModifiedKey(c,c->db,c->argv[1]);
    signalModifiedKey(c,c->db,c->argv[2]);
    server.dirty++;

    /* An extra key has changed when ele was successfully added to dstset */
    /* 将取出元素添加到目标集合中 */
    if (setTypeAdd(dstset,ele->ptr)) {
        server.dirty++;
        notifyKeyspaceEvent(NOTIFY_SET,"sadd",c->argv[2],c->db->id);
    }
    /* 返回 1 给客户端 */
    addReply(c,shared.cone);
}

/* SISMEMBER 命令执行函数 */
void sismemberCommand(client *c) {
    robj *set;

    /* 获取出集合对象，并进行类型检查。 */
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,set,OBJ_SET)) return;

    /* 如果指定元素存在于集合中，则返回 1 给客户端，否则，返回 0 给客户端。 */
    if (setTypeIsMember(set,c->argv[2]->ptr))
        addReply(c,shared.cone);
    else
        addReply(c,shared.czero);
}

/* SCARD 命令执行函数 */
void scardCommand(client *c) {
    robj *o;

    /* 获取出集合对象，并进行类型检查。 */
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_SET)) return;

    /* 返回集合全部元素的数量给客户端 */
    addReplyLongLong(c,setTypeSize(o));
}

/* Handle the "SPOP key <count>" variant. The normal version of the
 * command is handled by the spopCommand() function itself. */

/* How many times bigger should be the set compared to the remaining size
 * for us to use the "create new set" strategy? Read later in the
 * implementation for more info. */
#define SPOP_MOVE_STRATEGY_MUL 5

/* 从存储在 key 的集合中移除并返回指定数量的随机元素 */
void spopWithCountCommand(client *c) {
    long l;
    unsigned long count, size;
    robj *set;

    /* Get the count argument */
    /* 获取 count 参数值 */
    if (getLongFromObjectOrReply(c,c->argv[2],&l,NULL) != C_OK) return;
    if (l >= 0) {
        count = (unsigned long) l;
    } else {
        /* 如果 count 值小于 0，返回异常信息给客户端。 */
        addReply(c,shared.outofrangeerr);
        return;
    }

    /* Make sure a key with the name inputted exists, and that it's type is
     * indeed a set. Otherwise, return nil */
    /* 获取指定 key 关联的集合对象，并检查其类型。 */
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.emptyset[c->resp]))
        == NULL || checkType(c,set,OBJ_SET)) return;

    /* If count is zero, serve an empty set ASAP to avoid special
     * cases later. */
    /* 如果 count 值为 0，返回一个空集合给客户端。 */
    if (count == 0) {
        addReply(c,shared.emptyset[c->resp]);
        return;
    }

    size = setTypeSize(set);

    /* Generate an SPOP keyspace notification */
    notifyKeyspaceEvent(NOTIFY_SET,"spop",c->argv[1],c->db->id);
    server.dirty += count;

    /* CASE 1:
     * The number of requested elements is greater than or equal to
     * the number of elements inside the set: simply return the whole set. */
    /* 情形 1：
     * 请求元素数量大于集合中元素数量，直接返回整个集合给客户端。 */
    if (count >= size) {
        /* We just return the entire set */
        /* 返回整个集合给客户端 */
        sunionDiffGenericCommand(c,c->argv+1,1,NULL,SET_OP_UNION);

        /* Delete the set as it is now empty */
        /* 从库中移除集合对象 */
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);

        /* Propagate this command as an DEL operation */
        /* 将命令转换为 DEL 命令来进行传播 */
        rewriteClientCommandVector(c,2,shared.del,c->argv[1]);
        signalModifiedKey(c,c->db,c->argv[1]);
        server.dirty++;
        return;
    }

    /* Case 2 and 3 require to replicate SPOP as a set of SREM commands.
     * Prepare our replication argument vector. Also send the array length
     * which is common to both the code paths. */
    /* 情形 2 和 3 需要将命令转换为 SREM 命令进行传播，需要准备命令执行时需要的参数。 */
    robj *propargv[3];
    propargv[0] = createStringObject("SREM",4);
    propargv[1] = c->argv[1];
    addReplySetLen(c,count);

    /* Common iteration vars. */
    sds sdsele;
    robj *objele;
    int encoding;
    int64_t llele;
    unsigned long remaining = size-count; /* Elements left after SPOP. */

    /* If we are here, the number of requested elements is less than the
     * number of elements inside the set. Also we are sure that count < size.
     * Use two different strategies.
     *
     * CASE 2: The number of elements to return is small compared to the
     * set size. We can just extract random elements and return them to
     * the set. */
    /* 此时请求元素数量必定小于集合中元素数量，可以分为以下两个情况进行处理。
     *
     * 情形 2：
     * 如果处理后剩余的元素数量与返回元素的数量之比超过指定值时（剩余与返回比不低于20%。），
     * 可以随机移除多个元素，并将其全部返回给客户端。 */
    if (remaining*SPOP_MOVE_STRATEGY_MUL > count) {
        while(count--) {
            /* Emit and remove. */
            /* 从集合中随机获取一个元素，返回给客户端，之后移除该元素。 */
            encoding = setTypeRandomElement(set,&sdsele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
                objele = createStringObjectFromLongLong(llele);
                set->ptr = intsetRemove(set->ptr,llele,NULL);
            } else {
                addReplyBulkCBuffer(c,sdsele,sdslen(sdsele));
                objele = createStringObject(sdsele,sdslen(sdsele));
                setTypeRemove(set,sdsele);
            }

            /* Replicate/AOF this command as an SREM operation */
            /* 将命令转换为 SREM 命令进行传播或者 AOF 重写 */
            propargv[2] = objele;
            alsoPropagate(server.sremCommand,c->db->id,propargv,3,
                PROPAGATE_AOF|PROPAGATE_REPL);
            decrRefCount(objele);
        }
    } else {
    /* CASE 3: The number of elements to return is very big, approaching
     * the size of the set itself. After some time extracting random elements
     * from such a set becomes computationally expensive, so we use
     * a different strategy, we extract random elements that we don't
     * want to return (the elements that will remain part of the set),
     * creating a new set as we do this (that will be stored as the original
     * set). Then we return the elements left in the original set and
     * release it. */
    /* 情形 3：
     * 此时返回的元素数量很大了，很接近集合本身的大小，移除的方式变得十分的昂贵，
     * 所以这里使用一种新的策略，将需要保留的元素移到新的集合中，然后将原始集合
     * 返回给客户端。 */
        robj *newset = NULL;

        /* Create a new set with just the remaining elements. */
        /* 创建一个新的集合，保存要留下的元素。 */
        while(remaining--) {
            /* 获取一个随机元素 */
            encoding = setTypeRandomElement(set,&sdsele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                sdsele = sdsfromlonglong(llele);
            } else {
                sdsele = sdsdup(sdsele);
            }
            if (!newset) newset = setTypeCreate(sdsele);
            setTypeAdd(newset,sdsele);
            setTypeRemove(set,sdsele);
            sdsfree(sdsele);
        }

        /* Transfer the old set to the client. */
        /* 将旧的集合中元素返回给客户端 */
        setTypeIterator *si;
        si = setTypeInitIterator(set);
        while((encoding = setTypeNext(si,&sdsele,&llele)) != -1) {
            if (encoding == OBJ_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
                objele = createStringObjectFromLongLong(llele);
            } else {
                addReplyBulkCBuffer(c,sdsele,sdslen(sdsele));
                objele = createStringObject(sdsele,sdslen(sdsele));
            }

            /* Replicate/AOF this command as an SREM operation */
            /* 将命令转换为 SREM 命令进行传播或者 AOF 重写 */
            propargv[2] = objele;
            alsoPropagate(server.sremCommand,c->db->id,propargv,3,
                PROPAGATE_AOF|PROPAGATE_REPL);
            decrRefCount(objele);
        }
        setTypeReleaseIterator(si);

        /* Assign the new set as the key value. */
        /* 将新的集合关联到 key 上 */
        dbOverwrite(c->db,c->argv[1],newset);
    }

    /* Don't propagate the command itself even if we incremented the
     * dirty counter. We don't want to propagate an SPOP command since
     * we propagated the command as a set of SREMs operations using
     * the alsoPropagate() API. */
    /* 阻止当前命令进行传播 */
    decrRefCount(propargv[0]);
    preventCommandPropagation(c);
    signalModifiedKey(c,c->db,c->argv[1]);
    server.dirty++;
}

/* SPOP 命令执行函数 */
void spopCommand(client *c) {
    robj *set, *ele, *aux;
    sds sdsele;
    int64_t llele;
    int encoding;

    if (c->argc == 3) {
        /* 如果参数中有 count 值 */
        spopWithCountCommand(c);
        return;
    } else if (c->argc > 3) {
        addReply(c,shared.syntaxerr);
        return;
    }

    /* Make sure a key with the name inputted exists, and that it's type is
     * indeed a set */
    /* 确保指定 key 关联的集合对象存在，并对其进行类型检查。 */
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.null[c->resp]))
         == NULL || checkType(c,set,OBJ_SET)) return;

    /* Get a random element from the set */
    /* 从集合获取一个随机的元素 */
    encoding = setTypeRandomElement(set,&sdsele,&llele);

    /* Remove the element from the set */
    /* 从集合中移除该随机元素 */
    if (encoding == OBJ_ENCODING_INTSET) {
        ele = createStringObjectFromLongLong(llele);
        set->ptr = intsetRemove(set->ptr,llele,NULL);
    } else {
        ele = createStringObject(sdsele,sdslen(sdsele));
        setTypeRemove(set,ele->ptr);
    }

    notifyKeyspaceEvent(NOTIFY_SET,"spop",c->argv[1],c->db->id);

    /* Replicate/AOF this command as an SREM operation */
    /* 将命令转换为 SREM 命令进行传播或者 AOF 重写 */
    aux = createStringObject("SREM",4);
    rewriteClientCommandVector(c,3,aux,c->argv[1],ele);
    decrRefCount(aux);

    /* Add the element to the reply */
    /* 将元素值返回给客户端 */
    addReplyBulk(c,ele);
    decrRefCount(ele);

    /* Delete the set if it's empty */
    /* 如果集合为空，从库中移除该集合对象。 */
    if (setTypeSize(set) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    /* Set has been modified */
    signalModifiedKey(c,c->db,c->argv[1]);
    server.dirty++;
}

/* handle the "SRANDMEMBER key <count>" variant. The normal version of the
 * command is handled by the srandmemberCommand() function itself. */

/* How many times bigger should be the set compared to the requested size
 * for us to don't use the "remove elements" strategy? Read later in the
 * implementation for more info. */
#define SRANDMEMBER_SUB_STRATEGY_MUL 3

/* 从集合中随机指定数量的元素返回给客户端 */
void srandmemberWithCountCommand(client *c) {
    long l;
    unsigned long count, size;
    int uniq = 1;   /* 返回集合中元素是否都唯一 */
    robj *set;
    sds ele;
    int64_t llele;
    int encoding;

    dict *d;

    /* 获取 count 参数值 */
    if (getLongFromObjectOrReply(c,c->argv[2],&l,NULL) != C_OK) return;
    if (l >= 0) {
        count = (unsigned long) l;
    } else {
        /* A negative count means: return the same elements multiple times
         * (i.e. don't remove the extracted element after every extraction). */
        /* count 值为负数时，表示... */
        count = -l;
        uniq = 0;
    }

    /* 获取指定 key 关联的集合对象，并检查其类型。 */
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptyset[c->resp]))
        == NULL || checkType(c,set,OBJ_SET)) return;
    size = setTypeSize(set);

    /* If count is zero, serve it ASAP to avoid special cases later. */
    /* 如果 count 值为 0，返回一个空集合给客户端。 */
    if (count == 0) {
        addReply(c,shared.emptyset[c->resp]);
        return;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. */
    /* 情形 1：
     * count 值为负值，将返回包含 count 绝对值数量的随机元素集合给客户端，
     * 如果 count 绝对值大于集合中元素数量时，返回结果集中会出现一个元素出现多次的情况。 */
    if (!uniq) {
        addReplySetLen(c,count);
        while(count--) {
            encoding = setTypeRandomElement(set,&ele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
            } else {
                addReplyBulkCBuffer(c,ele,sdslen(ele));
            }
        }
        return;
    }

    /* CASE 2:
     * The number of requested elements is greater than the number of
     * elements inside the set: simply return the whole set. */
    /* 情形 2：
     * 如果请求的元素数量超过集合中元素总数量，返回整个集合给客户端。 */
    if (count >= size) {
        /* 将当前集合和空集合合并（通过该方法将集合中全部元素返回给客户端） */
        sunionDiffGenericCommand(c,c->argv+1,1,NULL,SET_OP_UNION);
        return;
    }

    /* For CASE 3 and CASE 4 we need an auxiliary dictionary. */
    /* 对于情形 3 和情形 4，我们需要一个字典对象来进行辅助处理。 */
    d = dictCreate(&objectKeyPointerValueDictType,NULL);

    /* CASE 3:
     * The number of elements inside the set is not greater than
     * SRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
     * In this case we create a set from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * This is done because if the number of requsted elements is just
     * a bit less than the number of elements in the set, the natural approach
     * used into CASE 3 is highly inefficient. */
    /* 情形 3：
     * 如果集合中元素数量不超过请求元素数量的 SRANDMEMBER_SUB_STRATEGY_MUL 倍时，
     * 说明请求元素数量占比较高（超过33%），这种情况下，我们可以创建一个集合的副本，
     * 然后随机删除副本中元素，直到其中元素数量等于请求元素的数量。 */
    if (count*SRANDMEMBER_SUB_STRATEGY_MUL > size) {
        setTypeIterator *si;

        /* Add all the elements into the temporary dictionary. */
        /* 将集合中的元素全部添加到临时字典中 */
        si = setTypeInitIterator(set);
        while((encoding = setTypeNext(si,&ele,&llele)) != -1) {
            int retval = DICT_ERR;

            if (encoding == OBJ_ENCODING_INTSET) {
                retval = dictAdd(d,createStringObjectFromLongLong(llele),NULL);
            } else {
                retval = dictAdd(d,createStringObject(ele,sdslen(ele)),NULL);
            }
            serverAssert(retval == DICT_OK);
        }
        setTypeReleaseIterator(si);
        serverAssert(dictSize(d) == size);

        /* Remove random elements to reach the right count. */
        /* 随机删除元素直至字典中元素数量等于请求元素的数量 */
        while(size > count) {
            dictEntry *de;

            de = dictGetRandomKey(d);
            dictDelete(d,dictGetKey(de));
            size--;
        }
    }

    /* CASE 4: We have a big set compared to the requested number of elements.
     * In this case we can simply get random elements from the set and add
     * to the temporary set, trying to eventually get enough unique elements
     * to reach the specified count. */
    /* 情形 4：
     * 请求元素的数量占比较少（小于33%），可以简单的从集合中随机获取指定数量的元素，
     * 存放到临时字典中。 */
    else {
        unsigned long added = 0;
        robj *objele;

        while(added < count) {
            encoding = setTypeRandomElement(set,&ele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                objele = createStringObjectFromLongLong(llele);
            } else {
                objele = createStringObject(ele,sdslen(ele));
            }
            /* Try to add the object to the dictionary. If it already exists
             * free it, otherwise increment the number of objects we have
             * in the result dictionary. */
            if (dictAdd(d,objele,NULL) == DICT_OK)
                added++;
            else
                decrRefCount(objele);
        }
    }

    /* CASE 3 & 4: send the result to the user. */
    /* 情形 3 和 4，将结果返回给客户端。 */
    {
        dictIterator *di;
        dictEntry *de;

        addReplySetLen(c,count);
        di = dictGetIterator(d);
        while((de = dictNext(di)) != NULL)
            addReplyBulk(c,dictGetKey(de));
        dictReleaseIterator(di);
        dictRelease(d);
    }
}

/* SRANDMEMBER 命令执行函数 */
void srandmemberCommand(client *c) {
    robj *set;
    sds ele;
    int64_t llele;
    int encoding;

    /* 如果请求带有 count 参数，调用 srandmemberWithCountCommand() 函数处理。 */
    if (c->argc == 3) {
        srandmemberWithCountCommand(c);
        return;
    } else if (c->argc > 3) {
        addReply(c,shared.syntaxerr);
        return;
    }

    /* 取出单个随机元素 */

    /* 确保指定 key 关联的集合对象存在，并对其进行类型检查。 */
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp]))
        == NULL || checkType(c,set,OBJ_SET)) return;

    /* 获取随机元素值，并返回给客户端。 */
    encoding = setTypeRandomElement(set,&ele,&llele);
    if (encoding == OBJ_ENCODING_INTSET) {
        addReplyBulkLongLong(c,llele);
    } else {
        addReplyBulkCBuffer(c,ele,sdslen(ele));
    }
}

int qsortCompareSetsByCardinality(const void *s1, const void *s2) {
    if (setTypeSize(*(robj**)s1) > setTypeSize(*(robj**)s2)) return 1;
    if (setTypeSize(*(robj**)s1) < setTypeSize(*(robj**)s2)) return -1;
    return 0;
}

/* This is used by SDIFF and in this case we can receive NULL that should
 * be handled as empty sets. */
int qsortCompareSetsByRevCardinality(const void *s1, const void *s2) {
    robj *o1 = *(robj**)s1, *o2 = *(robj**)s2;
    unsigned long first = o1 ? setTypeSize(o1) : 0;
    unsigned long second = o2 ? setTypeSize(o2) : 0;

    if (first < second) return 1;
    if (first > second) return -1;
    return 0;
}

/* SINTER 命令通用执行函数 */
void sinterGenericCommand(client *c, robj **setkeys,
                          unsigned long setnum, robj *dstkey) {
    /* 集合对象地址数组 */
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    setTypeIterator *si;
    robj *dstset = NULL;
    sds elesds;
    int64_t intobj;
    void *replylen = NULL;
    unsigned long j, cardinality = 0;
    int encoding;

    /* 设置集合对象地址 */
    for (j = 0; j < setnum; j++) {
        /* 如果 dstKey 有传值，需要将其保存在数组首位。 */
        robj *setobj = dstkey ?
            lookupKeyWrite(c->db,setkeys[j]) :
            lookupKeyRead(c->db,setkeys[j]);

        /* 如果有任何一个集合为空，那么交集肯定为空，不用再继续执行，并进行清理。 */
        if (!setobj) {
            zfree(sets);
            /* 如果目标集合存在，则将其从库中移除。 */
            if (dstkey) {
                if (dbDelete(c->db,dstkey)) {
                    signalModifiedKey(c,c->db,dstkey);
                    server.dirty++;
                }
                addReply(c,shared.czero);
            } else {
                addReply(c,shared.emptyset[c->resp]);
            }
            return;
        }
        /* 类型检查 */
        if (checkType(c,setobj,OBJ_SET)) {
            zfree(sets);
            return;
        }
        sets[j] = setobj;
    }
    /* Sort sets from the smallest to largest, this will improve our
     * algorithm's performance */
    /* 按照集合元素数量从小到大对集合进行排序，有助于之后提高算法的效率。 */
    qsort(sets,setnum,sizeof(robj*),qsortCompareSetsByCardinality);

    /* The first thing we should output is the total number of elements...
     * since this is a multi-bulk write, but at this stage we don't know
     * the intersection set size, so we use a trick, append an empty object
     * to the output list and save the pointer to later modify it with the
     * right length */
    /* 首先我们需要返回交集中元素的数量值，但是当前没有办法知道结果，所以这里使用了一个小技巧，
     * 添加一个空对象到 BUFF 列表中，并保存指向其的指针，用于之后将结果值写入其中。 */
    if (!dstkey) {
        replylen = addReplyDeferredLen(c);
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with an empty set inside */
        /* 创建一个空的集合用于保存结果集 */
        dstset = createIntsetObject();
    }

    /* Iterate all the elements of the first (smallest) set, and test
     * the element against all the other sets, if at least one set does
     * not include the element it is discarded */
    /* 遍历元素最少的第一个集合中的元素，并在剩余的集合中进行检查，如果有一个集合不包含当前元素，
     * 那么当前元素就不属于交集。 */
    si = setTypeInitIterator(sets[0]);
    while((encoding = setTypeNext(si,&elesds,&intobj)) != -1) {
        /* 遍历其它的集合，检查当前元素是否存在与这些集合中。 */
        for (j = 1; j < setnum; j++) {
            /* 如果与首集合相同（指向相同的集合地址），则跳过。 */
            if (sets[j] == sets[0]) continue;
            if (encoding == OBJ_ENCODING_INTSET) {
                /* intset with intset is simple... and fast */
                if (sets[j]->encoding == OBJ_ENCODING_INTSET &&
                    !intsetFind((intset*)sets[j]->ptr,intobj))
                {
                    break;
                /* in order to compare an integer with an object we
                 * have to use the generic function, creating an object
                 * for this */
                } else if (sets[j]->encoding == OBJ_ENCODING_HT) {
                    elesds = sdsfromlonglong(intobj);
                    if (!setTypeIsMember(sets[j],elesds)) {
                        sdsfree(elesds);
                        break;
                    }
                    sdsfree(elesds);
                }
            } else if (encoding == OBJ_ENCODING_HT) {
                if (!setTypeIsMember(sets[j],elesds)) {
                    break;
                }
            }
        }

        /* Only take action when all sets contain the member */
        /* 当所有的集合都包含当前元素时，执行以下逻辑。 */
        if (j == setnum) {
            /* 目标集合不存在，直接将元素返回给客户端，
             * 否则将其添加到目标集合中。 */
            if (!dstkey) {
                if (encoding == OBJ_ENCODING_HT)
                    addReplyBulkCBuffer(c,elesds,sdslen(elesds));
                else
                    addReplyBulkLongLong(c,intobj);
                cardinality++;
            } else {
                if (encoding == OBJ_ENCODING_INTSET) {
                    elesds = sdsfromlonglong(intobj);
                    setTypeAdd(dstset,elesds);
                    sdsfree(elesds);
                } else {
                    setTypeAdd(dstset,elesds);
                }
            }
        }
    }
    setTypeReleaseIterator(si);

    if (dstkey) {
        /* Store the resulting set into the target, if the intersection
         * is not an empty set. */
        /* 删除可能存在的目标集合，如果结果集不为空，则将其关联入库。 */
        int deleted = dbDelete(c->db,dstkey);
        if (setTypeSize(dstset) > 0) {
            dbAdd(c->db,dstkey,dstset);
            addReplyLongLong(c,setTypeSize(dstset));
            notifyKeyspaceEvent(NOTIFY_SET,"sinterstore",
                dstkey,c->db->id);
        } else {
            decrRefCount(dstset);
            addReply(c,shared.czero);
            if (deleted)
                notifyKeyspaceEvent(NOTIFY_GENERIC,"del",
                    dstkey,c->db->id);
        }
        signalModifiedKey(c,c->db,dstkey);
        server.dirty++;
    } else {
        setDeferredSetLen(c,replylen,cardinality);
    }
    zfree(sets);
}

/* SINTER 命令执行函数 */
void sinterCommand(client *c) {
    sinterGenericCommand(c,c->argv+1,c->argc-1,NULL);
}

/* SINTERSTORE 命令执行函数 */
void sinterstoreCommand(client *c) {
    sinterGenericCommand(c,c->argv+2,c->argc-2,c->argv[1]);
}

#define SET_OP_UNION 0
#define SET_OP_DIFF 1
#define SET_OP_INTER 2

/* 交集、并集和差集命令通用执行函数 */
void sunionDiffGenericCommand(client *c, robj **setkeys, int setnum,
                              robj *dstkey, int op) {
    /* 集合对象地址数组 */
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    setTypeIterator *si;
    robj *dstset = NULL;
    sds ele;
    int j, cardinality = 0;
    int diff_algo = 1;

    /* 设置集合对象地址 */
    for (j = 0; j < setnum; j++) {
        /* 如果 dstKey 有传值，需要将其保存在数组首位。 */
        robj *setobj = dstkey ?
            lookupKeyWrite(c->db,setkeys[j]) :
            lookupKeyRead(c->db,setkeys[j]);
        if (!setobj) {
            sets[j] = NULL;
            continue;
        }
        /* 类型检查 */
        if (checkType(c,setobj,OBJ_SET)) {
            zfree(sets);
            return;
        }
        sets[j] = setobj;
    }

    /* Select what DIFF algorithm to use.
     *
     * Algorithm 1 is O(N*M) where N is the size of the element first set
     * and M the total number of sets.
     *
     * Algorithm 2 is O(N) where N is the total number of elements in all
     * the sets.
     *
     * We compute what is the best bet with the current input here. */
    /* 如果是要计算差集，且目标集合存在的情况下，需要通过计算输入的参数值，
     * 并根据结果决定最终使用哪种算法进行处理。 */
    if (op == SET_OP_DIFF && sets[0]) {
        long long algo_one_work = 0, algo_two_work = 0;

        /* 遍历全部的集合 */
        for (j = 0; j < setnum; j++) {
            if (sets[j] == NULL) continue;

            /* 目标集合大小 * 集合个数 */
            algo_one_work += setTypeSize(sets[0]);
            /* 全部集合中元素总数 */
            algo_two_work += setTypeSize(sets[j]);
        }

        /* Algorithm 1 has better constant times and performs less operations
         * if there are elements in common. Give it some advantage. */
        /* 决定使用的算法 */
        algo_one_work /= 2;
        diff_algo = (algo_one_work <= algo_two_work) ? 1 : 2;

        if (diff_algo == 1 && setnum > 1) {
            /* With algorithm 1 it is better to order the sets to subtract
             * by decreasing size, so that we are more likely to find
             * duplicated elements ASAP. */
            /* 如果使用的是算法 1，那么最好对目标集合以外的其他集合进行排序，
             * 这样有助于优化算法的性能。 */
            qsort(sets+1,setnum-1,sizeof(robj*),
                qsortCompareSetsByRevCardinality);
        }
    }

    /* We need a temp set object to store our union. If the dstkey
     * is not NULL (that is, we are inside an SUNIONSTORE operation) then
     * this set object will be the resulting object to set into the target key*/
    /* 使用一个临时集合来保存结果集，如果程序执行的是 SUNIONSTORE 命令，
     * 那么这个结果集将会被关联入库。 */
    dstset = createIntsetObject();

    /* 并集 */
    if (op == SET_OP_UNION) {
        /* Union is trivial, just add every element of every set to the
         * temporary set. */
        /* 遍历所有的集合，将其中的全部元素都添加到临时集合中。 */
        for (j = 0; j < setnum; j++) {
            if (!sets[j]) continue; /* non existing keys are like empty sets */

            si = setTypeInitIterator(sets[j]);
            while((ele = setTypeNextObject(si)) != NULL) {
                if (setTypeAdd(dstset,ele)) cardinality++;
                sdsfree(ele);
            }
            setTypeReleaseIterator(si);
        }
    } else if (op == SET_OP_DIFF && sets[0] && diff_algo == 1) {
        /* DIFF Algorithm 1:
         *
         * We perform the diff by iterating all the elements of the first set,
         * and only adding it to the target set if the element does not exist
         * into all the other sets.
         *
         * This way we perform at max N*M operations, where N is the size of
         * the first set, and M the number of sets. */
        /* 差集算法 1：
         * 
         * 遍历目标集合中所有的元素，并检查是否存在于其它集合中，如果当前元素不存在于任何集合中，
         * 则将其添加到结果集中。 */
        si = setTypeInitIterator(sets[0]);
        while((ele = setTypeNextObject(si)) != NULL) {
            for (j = 1; j < setnum; j++) {
                /* 跳过空对象，跳过相同集合，判断是否存在于当前集合。 */
                if (!sets[j]) continue; /* no key is an empty set. */
                if (sets[j] == sets[0]) break; /* same set! */
                if (setTypeIsMember(sets[j],ele)) break;
            }
            /* 当前元素不存在于其他任何集合中时，才将其添加到结果集中。 */
            if (j == setnum) {
                /* There is no other set with this element. Add it. */
                setTypeAdd(dstset,ele);
                cardinality++;
            }
            sdsfree(ele);
        }
        setTypeReleaseIterator(si);
    } else if (op == SET_OP_DIFF && sets[0] && diff_algo == 2) {
        /* DIFF Algorithm 2:
         *
         * Add all the elements of the first set to the auxiliary set.
         * Then remove all the elements of all the next sets from it.
         *
         * This is O(N) where N is the sum of all the elements in every
         * set. */
        /* 差集算法 2：
         * 
         * 将目标集合中的所有元素都添加到临时集合中，然后遍历其他所有集合，
         * 并将相同的元素从结果集中删除。 */
        for (j = 0; j < setnum; j++) {
            /* 跳过空对象 */
            if (!sets[j]) continue; /* non existing keys are like empty sets */

            /*  */
            si = setTypeInitIterator(sets[j]);
            while((ele = setTypeNextObject(si)) != NULL) {
                if (j == 0) {
                    /* 将目标集合中所有元素添加到结果集中 */
                    if (setTypeAdd(dstset,ele)) cardinality++;
                } else {
                    /* 将源集合中元素从结果集中移除 */
                    if (setTypeRemove(dstset,ele)) cardinality--;
                }
                sdsfree(ele);
            }
            setTypeReleaseIterator(si);

            /* Exit if result set is empty as any additional removal
             * of elements will have no effect. */
            /* 如果结果集为空，则立即结束。 */
            if (cardinality == 0) break;
        }
    }

    /* Output the content of the resulting set, if not in STORE mode */
    /* 如果执行的不是 STORE 模式命令，则将结果返回给客户端。 */
    if (!dstkey) {
        addReplySetLen(c,cardinality);
        si = setTypeInitIterator(dstset);
        while((ele = setTypeNextObject(si)) != NULL) {
            addReplyBulkCBuffer(c,ele,sdslen(ele));
            sdsfree(ele);
        }
        setTypeReleaseIterator(si);
        server.lazyfree_lazy_server_del ? freeObjAsync(dstset) :
                                          decrRefCount(dstset);
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with the result set inside */
        /* 删除可能存在的目标集合，如果结果集不为空，则将其关联入库。 */
        int deleted = dbDelete(c->db,dstkey);
        if (setTypeSize(dstset) > 0) {
            dbAdd(c->db,dstkey,dstset);
            addReplyLongLong(c,setTypeSize(dstset));
            notifyKeyspaceEvent(NOTIFY_SET,
                op == SET_OP_UNION ? "sunionstore" : "sdiffstore",
                dstkey,c->db->id);
        } else {
            decrRefCount(dstset);
            addReply(c,shared.czero);
            if (deleted)
                notifyKeyspaceEvent(NOTIFY_GENERIC,"del",
                    dstkey,c->db->id);
        }
        signalModifiedKey(c,c->db,dstkey);
        server.dirty++;
    }
    zfree(sets);
}

/* SUNION 命令执行函数 */
void sunionCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,SET_OP_UNION);
}

/* SUNIONSTORE 命令执行函数 */
void sunionstoreCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],SET_OP_UNION);
}

/* SDIFF 命令执行函数 */
void sdiffCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,SET_OP_DIFF);
}

/* SDIFFSTORE 命令执行函数 */
void sdiffstoreCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],SET_OP_DIFF);
}

/* SSCAN 命令执行函数 */
void sscanCommand(client *c) {
    robj *set;
    unsigned long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == C_ERR) return;
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,set,OBJ_SET)) return;
    scanGenericCommand(c,set,cursor);
}
