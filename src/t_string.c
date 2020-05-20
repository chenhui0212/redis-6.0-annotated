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
#include <math.h> /* isnan(), isinf() */

/*-----------------------------------------------------------------------------
 * String Commands
 * 字符串相关命令
 *----------------------------------------------------------------------------*/

/* 检查字符串是否超过允许的最大长度（512MB） */
static int checkStringLength(client *c, long long size) {
    if (size > 512*1024*1024) {
        /* 超过则返回异常信息给客户端 */
        addReplyError(c,"string exceeds maximum allowed size (512MB)");
        return C_ERR;
    }
    return C_OK;
}

/* The setGenericCommand() function implements the SET operation with different
 * options and variants. This function is called in order to implement the
 * following commands: SET, SETEX, PSETEX, SETNX.
 * setGenericCommand() 函数实现了 SET、SETEX、PSETEX 和 SETNX 命令。
 *
 * 'flags' changes the behavior of the command (NX or XX, see below).
 * 'flags' 是包含了各类 'NX' 或者 'XX' 等额外参数的汇总。
 *
 * 'expire' represents an expire to set in form of a Redis object as passed
 * by the user. It is interpreted according to the specified 'unit'.
 * 'expire' 表示 Redis 对象的过期时间，而过期时间单位由 'unit' 决定。
 *
 * 'ok_reply' and 'abort_reply' is what the function will reply to the client
 * if the operation is performed, or when it is not because of NX or
 * XX flags.
 * 'ok_reply' 和 'abort_reply' 是否函数根据执行情况，返回给客户端的信息。
 *
 * If ok_reply is NULL "+OK" is used.
 * If abort_reply is NULL, "$-1" is used. */

/* 常用代码说明：
 * 
 * 1. server.dirty++;
 * 增加服务器脏值个数。
 * 
 * 2. notifyKeyspaceEvent(NOTIFY_STRING,"set",key,c->db->id);
 * 发送事件通知。
 * 
 * 3. signalModifiedKey(c,c->db,c->argv[1]);
 * 发送 key 被修改的信号。
 * 
 * 4. tryObjectEncoding(c->argv[2]);
 * 尝试编码对象（压缩空间）。
 * 
 * 5. setKey(c,c->db,c->argv[1],c->argv[2]);
 * 设置数据库中 key 值（不论 key 值存不存在）。
 * 
 * 6. dbUnshareStringValue(c->db,c->argv[1],o);
 * 当值对象处于共享状态或者编码模式时，为对象创建一个副本。
 */

/* SET 命令参数标识变量 */
#define OBJ_SET_NO_FLAGS 0
/* 当 key 不存在时才会设置 key 的值 */
#define OBJ_SET_NX (1<<0)          /* Set if key not exists. */
/* 当 key 存在时才会设置 key 的值 */
#define OBJ_SET_XX (1<<1)          /* Set if key exists. */
/* 设置 key 的过期时间（单位秒） */
#define OBJ_SET_EX (1<<2)          /* Set if time in seconds is given */
/* 设置 key 的过期时间（单位毫秒） */
#define OBJ_SET_PX (1<<3)          /* Set if time in ms in given */
/* 保留与 key 关联的过期时间 */
#define OBJ_SET_KEEPTTL (1<<4)     /* Set and keep the ttl */

/* SET 命令通用执行函数 */
void setGenericCommand(client *c, int flags, robj *key, robj *val, robj *expire, int unit, robj *ok_reply, robj *abort_reply) {
    long long milliseconds = 0; /* initialized to avoid any harmness warning */

    /* 如果设置了过期时长 */
    if (expire) {
        /* 取出过期时长 */
        if (getLongLongFromObjectOrReply(c, expire, &milliseconds, NULL) != C_OK)
            return;
        if (milliseconds <= 0) {
            addReplyErrorFormat(c,"invalid expire time in %s",c->cmd->name);
            return;
        }
        if (unit == UNIT_SECONDS) milliseconds *= 1000;
    }

    /* 如果设置了 'NX' 或 'XX' 参数，且 key 值的存在状况与参数不符，
     * 返回取消信息给客户端。 */
    if ((flags & OBJ_SET_NX && lookupKeyWrite(c->db,key) != NULL) ||
        (flags & OBJ_SET_XX && lookupKeyWrite(c->db,key) == NULL))
    {
        addReply(c, abort_reply ? abort_reply : shared.null[c->resp]);
        return;
    }
    /* 保存键值对值到库中 */
    genericSetKey(c,c->db,key,val,flags & OBJ_SET_KEEPTTL,1);
    /* 增加服务器脏值个数 */
    server.dirty++;
    /* 设置 key 的过期时间 */
    if (expire) setExpire(c,c->db,key,mstime()+milliseconds);
    /* 发送事件通知 */
    notifyKeyspaceEvent(NOTIFY_STRING,"set",key,c->db->id);
    if (expire) notifyKeyspaceEvent(NOTIFY_GENERIC,
        "expire",key,c->db->id);
    addReply(c, ok_reply ? ok_reply : shared.ok);
}

/* SET key value [NX] [XX] [KEEPTTL] [EX <seconds>] [PX <milliseconds>] */
/* SET 命令执行函数 */
void setCommand(client *c) {
    int j;
    robj *expire = NULL;
    int unit = UNIT_SECONDS;
    int flags = OBJ_SET_NO_FLAGS;

    /* 解析从第三个参数开始及之后的全部参数，并据此设置 flag 字段值。 */
    for (j = 3; j < c->argc; j++) {
        /* 取出指定参数值和其下一个参数值 */
        char *a = c->argv[j]->ptr;
        robj *next = (j == c->argc-1) ? NULL : c->argv[j+1];

        if ((a[0] == 'n' || a[0] == 'N') &&
            (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
            !(flags & OBJ_SET_XX))
        /* 当前参数为 'NX'，且未设置 'XX'。 */
        {
            flags |= OBJ_SET_NX;
        } else if ((a[0] == 'x' || a[0] == 'X') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
                   !(flags & OBJ_SET_NX))
        /* 当前参数为 'XX'，且未设置 'NX'。 */
        {
            flags |= OBJ_SET_XX;
        } else if (!strcasecmp(c->argv[j]->ptr,"KEEPTTL") &&
                   !(flags & OBJ_SET_EX) && !(flags & OBJ_SET_PX))
        /* 当前参数为 'KEEPTTL'，且未设置 'EX' 和 'PX'。 */
        {
            flags |= OBJ_SET_KEEPTTL;
        } else if ((a[0] == 'e' || a[0] == 'E') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
                   !(flags & OBJ_SET_KEEPTTL) &&
                   !(flags & OBJ_SET_PX) && next)
        /* 当前参数为 'EX'，且未设置 'KEEPTTL' 和 'PX'，以及存在下一个参数。 */
        {
            flags |= OBJ_SET_EX;
            unit = UNIT_SECONDS;
            expire = next;
            j++;
        } else if ((a[0] == 'p' || a[0] == 'P') &&
                   (a[1] == 'x' || a[1] == 'X') && a[2] == '\0' &&
                   !(flags & OBJ_SET_KEEPTTL) &&
                   !(flags & OBJ_SET_EX) && next)
        /* 当前参数为 'PX'，且未设置 'KEEPTTL' 和 'EX'，以及存在下一个参数。 */
        {
            flags |= OBJ_SET_PX;
            unit = UNIT_MILLISECONDS;
            expire = next;
            j++;
        } else {
            /* 返回语法异常信息给客户端 */
            addReply(c,shared.syntaxerr);
            return;
        }
    }

    /* 尝试编码 value 对象（压缩空间） */
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,flags,c->argv[1],c->argv[2],expire,unit,NULL,NULL);
}

/* SETNX 命令执行函数 */
void setnxCommand(client *c) {
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    setGenericCommand(c,OBJ_SET_NX,c->argv[1],c->argv[2],NULL,0,shared.cone,shared.czero);
}

/* SETEX 命令执行函数 */
void setexCommand(client *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,OBJ_SET_NO_FLAGS,c->argv[1],c->argv[3],c->argv[2],UNIT_SECONDS,NULL,NULL);
}

/* PSETEX 命令执行函数 */
void psetexCommand(client *c) {
    c->argv[3] = tryObjectEncoding(c->argv[3]);
    setGenericCommand(c,OBJ_SET_NO_FLAGS,c->argv[1],c->argv[3],c->argv[2],UNIT_MILLISECONDS,NULL,NULL);
}

/* GET 命令通用执行函数 */
int getGenericCommand(client *c) {
    robj *o;

    /* 尝试获取库中 key 对应的 value 对象 */
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp])) == NULL)
        return C_OK;

    /* 检查 value 的数据类型是否是字符串 */
    if (o->type != OBJ_STRING) {
        /* 不是，返回类型错误消息给客户端。 */
        addReply(c,shared.wrongtypeerr);
        return C_ERR;
    } else {
        /* 是，返回 value 值给客户端。 */
        addReplyBulk(c,o);
        return C_OK;
    }
}

/* GET 命令执行函数 */
void getCommand(client *c) {
    getGenericCommand(c);
}

/* GETSET 命令执行函数 */
void getsetCommand(client *c) {
    /* 查询并返回可能存在 key 的旧值给客户端 */
    if (getGenericCommand(c) == C_ERR) return;
    /* 尝试编码新的 value 对象 */
    c->argv[2] = tryObjectEncoding(c->argv[2]);
    /* 入库 */
    setKey(c,c->db,c->argv[1],c->argv[2]);
    notifyKeyspaceEvent(NOTIFY_STRING,"set",c->argv[1],c->db->id);
    server.dirty++;
}

/* SETRANGE 命令执行函数。
 * 从指定的 offset 位置开始，用指定字符串覆盖给定 key 所存储的字符串值。 */
void setrangeCommand(client *c) {
    robj *o;
    long offset;
    sds value = c->argv[3]->ptr; /*  */

    /* 取出 offset 参数值 */
    if (getLongFromObjectOrReply(c,c->argv[2],&offset,NULL) != C_OK)
        return;

    if (offset < 0) {
        addReplyError(c,"offset is out of range");
        return;
    }

    /* 查询 key 的值对象 */
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        /* key 不存在 */

        /* Return 0 when setting nothing on a non-existing string */
        /* 如果 value 为空，则返回 0 给客户端。 */
        if (sdslen(value) == 0) {
            addReply(c,shared.czero);
            return;
        }

        /* Return when the resulting string exceeds allowed size */
        /* 检查最终的字符串长度是否超过允许的最大长度 */
        if (checkStringLength(c,offset+sdslen(value)) != C_OK)
            return;

        /* 创建一个包含指定长度空字符串的对象 */
        o = createObject(OBJ_STRING,sdsnewlen(NULL, offset+sdslen(value)));
        /* 入库 */
        dbAdd(c->db,c->argv[1],o);
    } else {
        /* key 存在 */
        size_t olen;

        /* Key exists, check type */
        /* 检查值对象类型 */
        if (checkType(c,o,OBJ_STRING))
            return;

        /* Return existing string length when setting nothing */
        /* 如果 value 为空，则返回已存在的字符串长度给客户端。 */
        olen = stringObjectLen(o);
        if (sdslen(value) == 0) {
            addReplyLongLong(c,olen);
            return;
        }

        /* Return when the resulting string exceeds allowed size */
        /* 检查最终的字符串长度是否超过允许的最大长度 */
        if (checkStringLength(c,offset+sdslen(value)) != C_OK)
            return;

        /* Create a copy when the object is shared or encoded. */
        /* 当值对象处于共享状态或者编码模式时，为值对象创建一个副本。 */
        o = dbUnshareStringValue(c->db,c->argv[1],o);
    }

    /* 可以省略条件判断，因为之前已经有判断了。 */
    if (sdslen(value) > 0) {
        /* 扩展字符串 */
        o->ptr = sdsgrowzero(o->ptr,offset+sdslen(value));
        /* 将 value 值复制到字符串中的指定位置 */
        memcpy((char*)o->ptr+offset,value,sdslen(value));
        /* 发送 key 被修改的信号 */
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_STRING,
            "setrange",c->argv[1],c->db->id);
        server.dirty++;
    }
    /* 返回新的字符串长度给客户端 */
    addReplyLongLong(c,sdslen(o->ptr));
}

/* GETRAGE 命令执行函数（支持负数索引） */
void getrangeCommand(client *c) {
    robj *o;
    long long start, end;
    char *str, llbuf[32];
    size_t strlen;

    /* 取出 start 和 end 的值 */
    if (getLongLongFromObjectOrReply(c,c->argv[2],&start,NULL) != C_OK)
        return;
    if (getLongLongFromObjectOrReply(c,c->argv[3],&end,NULL) != C_OK)
        return;
    /* 检查 key 是否存在，且值对象类型是否是字符串。 */
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptybulk)) == NULL ||
        checkType(c,o,OBJ_STRING)) return;

    /* 根据不同的编码方式，取出值对象中的字符串。 */
    if (o->encoding == OBJ_ENCODING_INT) {
        str = llbuf;
        strlen = ll2string(llbuf,sizeof(llbuf),(long)o->ptr);
    } else {
        str = o->ptr;
        strlen = sdslen(str);
    }

    /* Convert negative indexes */
    /* 转换负数索引 */
    if (start < 0 && end < 0 && start > end) {
        addReply(c,shared.emptybulk);
        return;
    }
    if (start < 0) start = strlen+start;
    if (end < 0) end = strlen+end;
    if (start < 0) start = 0;
    if (end < 0) end = 0;
    if ((unsigned long long)end >= strlen) end = strlen-1;

    /* Precondition: end >= 0 && end < strlen, so the only condition where
     * nothing can be returned is: start > end. */
    /* 判断索引范围是否为空，或字符串是否为空。*/
    if (start > end || strlen == 0) {
        addReply(c,shared.emptybulk);
    } else {
        /* 返回给定范围内的字符串内容给客户端 */
        addReplyBulkCBuffer(c,(char*)str+start,end-start+1);
    }
}

/* MGET 命令执行函数 */
void mgetCommand(client *c) {
    int j;

    addReplyArrayLen(c,c->argc-1);
    for (j = 1; j < c->argc; j++) {
        /* 查询 key 值 */
        robj *o = lookupKeyRead(c->db,c->argv[j]);
        /* 根据 key 是否存在，及类型是否匹配做相应返回。 */
        if (o == NULL) {
            addReplyNull(c);
        } else {
            if (o->type != OBJ_STRING) {
                addReplyNull(c);
            } else {
                addReplyBulk(c,o);
            }
        }
    }
}

/* MSET 命令通用执行函数 */
void msetGenericCommand(client *c, int nx) {
    int j;

    /* 参数个数必须是奇数，考虑命令本身会占用一个参数。 */
    if ((c->argc % 2) == 0) {
        addReplyError(c,"wrong number of arguments for MSET");
        return;
    }

    /* Handle the NX flag. The MSETNX semantic is to return zero and don't
     * set anything if at least one key alerady exists. */
    /* 处理 NX 标志。
     * MSETNX 命令语法含义是，只要有一个 key 是存在的，则返回 0 给客户端，
     * 并不做任何后续处理。 */
    if (nx) {
        for (j = 1; j < c->argc; j += 2) {
            if (lookupKeyWrite(c->db,c->argv[j]) != NULL) {
                addReply(c, shared.czero);
                return;
            }
        }
    }

    /* 依次将所有的键值对入库 */
    for (j = 1; j < c->argc; j += 2) {
        c->argv[j+1] = tryObjectEncoding(c->argv[j+1]);
        setKey(c,c->db,c->argv[j],c->argv[j+1]);
        notifyKeyspaceEvent(NOTIFY_STRING,"set",c->argv[j],c->db->id);
    }
    server.dirty += (c->argc-1)/2;
    addReply(c, nx ? shared.cone : shared.ok);
}

/* MSET 命令执行函数 */
void msetCommand(client *c) {
    msetGenericCommand(c,0);
}

/* MSETNX 命令执行函数 */
void msetnxCommand(client *c) {
    msetGenericCommand(c,1);
}

/* INCR/DECR 命令执行函数 */
void incrDecrCommand(client *c, long long incr) {
    long long value, oldvalue;
    robj *o, *new;

    /* 查询 key 的值对象是否存在，并验证对象数据类型。 */
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o != NULL && checkType(c,o,OBJ_STRING)) return;
    /* 取出整型值存放到 value 变量中 */
    if (getLongLongFromObjectOrReply(c,o,&value,NULL) != C_OK) return;

    oldvalue = value;
    /* 验证计算结果是否会溢出 */
    if ((incr < 0 && oldvalue < 0 && incr < (LLONG_MIN-oldvalue)) ||
        (incr > 0 && oldvalue > 0 && incr > (LLONG_MAX-oldvalue))) {
        addReplyError(c,"increment or decrement would overflow");
        return;
    }
    value += incr;

    /* 如果对象处于非共享状态、编码类型为整型、非共享整型且值在 long 类型范围内，
     * 则直接更新原有对象中的值。 */
    if (o && o->refcount == 1 && o->encoding == OBJ_ENCODING_INT &&
        (value < 0 || value >= OBJ_SHARED_INTEGERS) &&
        value >= LONG_MIN && value <= LONG_MAX)
    {
        new = o;
        o->ptr = (void*)((long)value);
    } else {
        /* 否则，创建新的对象保存新值。 */
        new = createStringObjectFromLongLongForValue(value);
        /* 入库 */
        if (o) {
            dbOverwrite(c->db,c->argv[1],new);
        } else {
            dbAdd(c->db,c->argv[1],new);
        }
    }
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"incrby",c->argv[1],c->db->id);
    server.dirty++;
    addReply(c,shared.colon);
    addReply(c,new);
    addReply(c,shared.crlf);
}

/* INCR 命令执行函数 */
void incrCommand(client *c) {
    incrDecrCommand(c,1);
}

/* DECR 命令执行函数 */
void decrCommand(client *c) {
    incrDecrCommand(c,-1);
}

/* INCRBY 命令执行函数 */
void incrbyCommand(client *c) {
    long long incr;

    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != C_OK) return;
    incrDecrCommand(c,incr);
}

/* DECRBY 命令执行函数 */
void decrbyCommand(client *c) {
    long long incr;

    if (getLongLongFromObjectOrReply(c, c->argv[2], &incr, NULL) != C_OK) return;
    incrDecrCommand(c,-incr);
}

/* INCRBYFLOAT 命令执行函数 */
void incrbyfloatCommand(client *c) {
    long double incr, value;
    robj *o, *new, *aux1, *aux2;

    /* 查询 key 的值对象是否存在，并验证对象数据类型。 */
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o != NULL && checkType(c,o,OBJ_STRING)) return;
    /* 获取对象中浮点值和请求中参数浮点值 */
    if (getLongDoubleFromObjectOrReply(c,o,&value,NULL) != C_OK ||
        getLongDoubleFromObjectOrReply(c,c->argv[2],&incr,NULL) != C_OK)
        return;

    value += incr;
    /* 判断计算结果是否溢出 */
    if (isnan(value) || isinf(value)) {
        addReplyError(c,"increment would produce NaN or Infinity");
        return;
    }
    /* 创建新的对象，并入库。 */
    new = createStringObjectFromLongDouble(value,1);
    if (o)
        dbOverwrite(c->db,c->argv[1],new);
    else
        dbAdd(c->db,c->argv[1],new);
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"incrbyfloat",c->argv[1],c->db->id);
    server.dirty++;
    /* 返回新值给客户端 */
    addReplyBulk(c,new);

    /* Always replicate INCRBYFLOAT as a SET command with the final value
     * in order to make sure that differences in float precision or formatting
     * will not create differences in replicas or after an AOF restart. */
    /* 在传播 INCRBYFLOAT 命令时，总是用 SET 命令来替换 INCRBYFLOAT 命令，
     * 从而防止因为不同的浮点精度或格式化导致副本之间或 AOF 重启后的数据不一致问题。 */
    aux1 = createStringObject("SET",3);
    rewriteClientCommandArgument(c,0,aux1);
    decrRefCount(aux1);
    rewriteClientCommandArgument(c,2,new);
    aux2 = createStringObject("KEEPTTL",7);
    rewriteClientCommandArgument(c,3,aux2);
    decrRefCount(aux2);
}

/* APPEND 命令执行函数 */
void appendCommand(client *c) {
    size_t totlen;
    robj *o, *append;

    /* 查询 key 值是否存在 */
    o = lookupKeyWrite(c->db,c->argv[1]);
    if (o == NULL) {
        /* Create the key */
        /* key 不存在，直接新增入库。 */
        c->argv[2] = tryObjectEncoding(c->argv[2]);
        dbAdd(c->db,c->argv[1],c->argv[2]);
        incrRefCount(c->argv[2]);
        totlen = stringObjectLen(c->argv[2]);
    } else {
        /* Key exists, check type */
        /* key 存在，检查值对象数据类型。 */
        if (checkType(c,o,OBJ_STRING))
            return;

        /* "append" is an argument, so always an sds */
        /* 检查最终的字符串长度是否超过允许的最大长度 */
        append = c->argv[2];
        totlen = stringObjectLen(o)+sdslen(append->ptr);
        if (checkStringLength(c,totlen) != C_OK)
            return;

        /* Append the value */
        /* 追加值到值对象中 */
        o = dbUnshareStringValue(c->db,c->argv[1],o);
        o->ptr = sdscatlen(o->ptr,append->ptr,sdslen(append->ptr));
        totlen = sdslen(o->ptr);
    }
    signalModifiedKey(c,c->db,c->argv[1]);
    notifyKeyspaceEvent(NOTIFY_STRING,"append",c->argv[1],c->db->id);
    server.dirty++;
    /* 返回新值的长度给客户端 */
    addReplyLongLong(c,totlen);
}

/* STRLEN 命令执行函数 */
void strlenCommand(client *c) {
    robj *o;
    /* 取出值对象，并进行类型检查。 */
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_STRING)) return;
    /* 返回字符串长度给客户端 */
    addReplyLongLong(c,stringObjectLen(o));
}


/* STRALGO -- Implement complex algorithms on strings.
 * STRALGO -- 实现字符串复杂算法处理。
 *
 * STRALGO <algorithm> ... arguments ... */
/* 实现查询公共最长子串（longest common substring） */
void stralgoLCS(client *c);     /* This implements the LCS algorithm. */

/* STRALGO 命令执行函数 */
void stralgoCommand(client *c) {
    /* Select the algorithm. */
    /* 目前只支持 lcs 算法 */
    if (!strcasecmp(c->argv[1]->ptr,"lcs")) {
        stralgoLCS(c);
    } else {
        addReply(c,shared.syntaxerr);
    }
}

/* STRALGO <algo> [IDX] [MINMATCHLEN <len>] [WITHMATCHLEN]
 *     STRINGS <string> <string> | KEYS <keya> <keyb>
 */
void stralgoLCS(client *c) {
    uint32_t i, j;
    long long minmatchlen = 0;
    sds a = NULL, b = NULL;
    int getlen = 0, getidx = 0, withmatchlen = 0;
    robj *obja = NULL, *objb = NULL;

    for (j = 2; j < (uint32_t)c->argc; j++) {
        char *opt = c->argv[j]->ptr;
        int moreargs = (c->argc-1) - j;

        if (!strcasecmp(opt,"IDX")) {
            getidx = 1;
        } else if (!strcasecmp(opt,"LEN")) {
            getlen = 1;
        } else if (!strcasecmp(opt,"WITHMATCHLEN")) {
            withmatchlen = 1;
        } else if (!strcasecmp(opt,"MINMATCHLEN") && moreargs) {
            if (getLongLongFromObjectOrReply(c,c->argv[j+1],&minmatchlen,NULL)
                != C_OK) return;
            if (minmatchlen < 0) minmatchlen = 0;
            j++;
        } else if (!strcasecmp(opt,"STRINGS")) {
            if (a != NULL) {
                addReplyError(c,"Either use STRINGS or KEYS");
                return;
            }
            a = c->argv[j+1]->ptr;
            b = c->argv[j+2]->ptr;
            j += 2;
        } else if (!strcasecmp(opt,"KEYS")) {
            if (a != NULL) {
                addReplyError(c,"Either use STRINGS or KEYS");
                return;
            }
            obja = lookupKeyRead(c->db,c->argv[j+1]);
            objb = lookupKeyRead(c->db,c->argv[j+2]);
            obja = obja ? getDecodedObject(obja) : createStringObject("",0);
            objb = objb ? getDecodedObject(objb) : createStringObject("",0);
            a = obja->ptr;
            b = objb->ptr;
            j += 2;
        } else {
            addReply(c,shared.syntaxerr);
            return;
        }
    }

    /* Complain if the user passed ambiguous parameters. */
    if (a == NULL) {
        addReplyError(c,"Please specify two strings: "
                        "STRINGS or KEYS options are mandatory");
        return;
    } else if (getlen && getidx) {
        addReplyError(c,
            "If you want both the length and indexes, please "
            "just use IDX.");
        return;
    }

    /* Compute the LCS using the vanilla dynamic programming technique of
     * building a table of LCS(x,y) substrings. */
    uint32_t alen = sdslen(a);
    uint32_t blen = sdslen(b);

    /* Setup an uint32_t array to store at LCS[i,j] the length of the
     * LCS A0..i-1, B0..j-1. Note that we have a linear array here, so
     * we index it as LCS[j+(blen+1)*j] */
    uint32_t *lcs = zmalloc((alen+1)*(blen+1)*sizeof(uint32_t));
    #define LCS(A,B) lcs[(B)+((A)*(blen+1))]

    /* Start building the LCS table. */
    for (uint32_t i = 0; i <= alen; i++) {
        for (uint32_t j = 0; j <= blen; j++) {
            if (i == 0 || j == 0) {
                /* If one substring has length of zero, the
                 * LCS length is zero. */
                LCS(i,j) = 0;
            } else if (a[i-1] == b[j-1]) {
                /* The len LCS (and the LCS itself) of two
                 * sequences with the same final character, is the
                 * LCS of the two sequences without the last char
                 * plus that last char. */
                LCS(i,j) = LCS(i-1,j-1)+1;
            } else {
                /* If the last character is different, take the longest
                 * between the LCS of the first string and the second
                 * minus the last char, and the reverse. */
                uint32_t lcs1 = LCS(i-1,j);
                uint32_t lcs2 = LCS(i,j-1);
                LCS(i,j) = lcs1 > lcs2 ? lcs1 : lcs2;
            }
        }
    }

    /* Store the actual LCS string in "result" if needed. We create
     * it backward, but the length is already known, we store it into idx. */
    uint32_t idx = LCS(alen,blen);
    sds result = NULL;        /* Resulting LCS string. */
    void *arraylenptr = NULL; /* Deffered length of the array for IDX. */
    uint32_t arange_start = alen, /* alen signals that values are not set. */
             arange_end = 0,
             brange_start = 0,
             brange_end = 0;

    /* Do we need to compute the actual LCS string? Allocate it in that case. */
    int computelcs = getidx || !getlen;
    if (computelcs) result = sdsnewlen(SDS_NOINIT,idx);

    /* Start with a deferred array if we have to emit the ranges. */
    uint32_t arraylen = 0;  /* Number of ranges emitted in the array. */
    if (getidx) {
        addReplyMapLen(c,2);
        addReplyBulkCString(c,"matches");
        arraylenptr = addReplyDeferredLen(c);
    }

    i = alen, j = blen;
    while (computelcs && i > 0 && j > 0) {
        int emit_range = 0;
        if (a[i-1] == b[j-1]) {
            /* If there is a match, store the character and reduce
             * the indexes to look for a new match. */
            result[idx-1] = a[i-1];

            /* Track the current range. */
            if (arange_start == alen) {
                arange_start = i-1;
                arange_end = i-1;
                brange_start = j-1;
                brange_end = j-1;
            } else {
                /* Let's see if we can extend the range backward since
                 * it is contiguous. */
                if (arange_start == i && brange_start == j) {
                    arange_start--;
                    brange_start--;
                } else {
                    emit_range = 1;
                }
            }
            /* Emit the range if we matched with the first byte of
             * one of the two strings. We'll exit the loop ASAP. */
            if (arange_start == 0 || brange_start == 0) emit_range = 1;
            idx--; i--; j--;
        } else {
            /* Otherwise reduce i and j depending on the largest
             * LCS between, to understand what direction we need to go. */
            uint32_t lcs1 = LCS(i-1,j);
            uint32_t lcs2 = LCS(i,j-1);
            if (lcs1 > lcs2)
                i--;
            else
                j--;
            if (arange_start != alen) emit_range = 1;
        }

        /* Emit the current range if needed. */
        uint32_t match_len = arange_end - arange_start + 1;
        if (emit_range) {
            if (minmatchlen == 0 || match_len >= minmatchlen) {
                if (arraylenptr) {
                    addReplyArrayLen(c,2+withmatchlen);
                    addReplyArrayLen(c,2);
                    addReplyLongLong(c,arange_start);
                    addReplyLongLong(c,arange_end);
                    addReplyArrayLen(c,2);
                    addReplyLongLong(c,brange_start);
                    addReplyLongLong(c,brange_end);
                    if (withmatchlen) addReplyLongLong(c,match_len);
                    arraylen++;
                }
            }
            arange_start = alen; /* Restart at the next match. */
        }
    }

    /* Signal modified key, increment dirty, ... */

    /* Reply depending on the given options. */
    if (arraylenptr) {
        addReplyBulkCString(c,"len");
        addReplyLongLong(c,LCS(alen,blen));
        setDeferredArrayLen(c,arraylenptr,arraylen);
    } else if (getlen) {
        addReplyLongLong(c,LCS(alen,blen));
    } else {
        addReplyBulkSds(c,result);
        result = NULL;
    }

    /* Cleanup. */
    if (obja) decrRefCount(obja);
    if (objb) decrRefCount(objb);
    sdsfree(result);
    zfree(lcs);
    return;
}

