/* Rax -- A radix tree implementation.
 *
 * Copyright (c) 2017-2018, Salvatore Sanfilippo <antirez at gmail dot com>
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

#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <errno.h>
#include <math.h>
#include "rax.h"

#ifndef RAX_MALLOC_INCLUDE
#define RAX_MALLOC_INCLUDE "rax_malloc.h"
#endif

#include RAX_MALLOC_INCLUDE

/* This is a special pointer that is guaranteed to never have the same value
 * of a radix tree node. It's used in order to report "not found" error without
 * requiring the function to have multiple return values. */
/* 用于报告"未找到"错误，从而函数可以只有一个返回值。
 * 这是一个特殊的指针，Redis 保证该值永远不会与任何基数树节点有相同的值。 */
void *raxNotFound = (void*)"rax-not-found-pointer";

/* -------------------------------- Debugging ------------------------------ */

void raxDebugShowNode(const char *msg, raxNode *n);

/* Turn debugging messages on/off by compiling with RAX_DEBUG_MSG macro on.
 * When RAX_DEBUG_MSG is defined by default Rax operations will emit a lot
 * of debugging info to the standard output, however you can still turn
 * debugging on/off in order to enable it only when you suspect there is an
 * operation causing a bug using the function raxSetDebugMsg(). */
#ifdef RAX_DEBUG_MSG
#define debugf(...)                                                            \
    if (raxDebugMsg) {                                                         \
        printf("%s:%s:%d:\t", __FILE__, __FUNCTION__, __LINE__);               \
        printf(__VA_ARGS__);                                                   \
        fflush(stdout);                                                        \
    }

#define debugnode(msg,n) raxDebugShowNode(msg,n)
#else
#define debugf(...)
#define debugnode(msg,n)
#endif

/* By default log debug info if RAX_DEBUG_MSG is defined. */
static int raxDebugMsg = 1;

/* When debug messages are enabled, turn them on/off dynamically. By
 * default they are enabled. Set the state to 0 to disable, and 1 to
 * re-enable. */
void raxSetDebugMsg(int onoff) {
    raxDebugMsg = onoff;
}

/* ------------------------- raxStack functions --------------------------
 * The raxStack is a simple stack of pointers that is capable of switching
 * from using a stack-allocated array to dynamic heap once a given number of
 * items are reached. It is used in order to retain the list of parent nodes
 * while walking the radix tree in order to implement certain operations that
 * need to navigate the tree upward.
 *
 * raxStack 是一个简单的指针堆栈，一旦达到给定数量的项目，它会从使用堆栈分配的数组切换到动态堆。
 * 它用于在遍历基数树时保留父节点列表，以实现某些需要向上遍历树的操作。
 * ------------------------------------------------------------------------- */

/* Initialize the stack. */
/* 初始化指针栈 */
static inline void raxStackInit(raxStack *ts) {
    ts->stack = ts->static_items;
    ts->items = 0;
    ts->maxitems = RAX_STACK_STATIC_ITEMS;
    ts->oom = 0;
}

/* Push an item into the stack, returns 1 on success, 0 on out of memory. */
/* 将指定指针压入栈，成功返回 1，内存不足返回 0。 */
static inline int /**/raxStackPush(raxStack *ts, void *ptr) {
    /* 如果栈已满，则需要扩展栈空间。 */
    if (ts->items == ts->maxitems) {
        /* 如果使用的是结构体内部的数组 */
        if (ts->stack == ts->static_items) {
            /* 申请新的内存空间，将内部数组中数据拷贝到新的内存空间中。 */
            ts->stack = rax_malloc(sizeof(void*)*ts->maxitems*2);
            if (ts->stack == NULL) {
                ts->stack = ts->static_items;
                ts->oom = 1;
                errno = ENOMEM;
                return 0;
            }
            memcpy(ts->stack,ts->static_items,sizeof(void*)*ts->maxitems);
        } else {
            /* 否则，重新重新分配更多内存空间，并更新指向其的指针。 */
            void **newalloc = rax_realloc(ts->stack,sizeof(void*)*ts->maxitems*2);
            if (newalloc == NULL) {
                ts->oom = 1;
                errno = ENOMEM;
                return 0;
            }
            ts->stack = newalloc;
        }
        ts->maxitems *= 2;
    }
    /* 指针入栈 */
    ts->stack[ts->items] = ptr;
    ts->items++;
    return 1;
}

/* Pop an item from the stack, the function returns NULL if there are no
 * items to pop. */
/* 从栈中弹出一个指针，如果栈为空，则函数返回 NULL。 */
static inline void *raxStackPop(raxStack *ts) {
    if (ts->items == 0) return NULL;
    ts->items--;
    return ts->stack[ts->items];
}

/* Return the stack item at the top of the stack without actually consuming
 * it. */
/* 返回栈顶的指针，但不弹出。 */
static inline void *raxStackPeek(raxStack *ts) {
    if (ts->items == 0) return NULL;
    return ts->stack[ts->items-1];
}

/* Free the stack in case we used heap allocation. */
/* 释放指针栈 */
static inline void raxStackFree(raxStack *ts) {
    if (ts->stack != ts->static_items) rax_free(ts->stack);
}

/* ----------------------------------------------------------------------------
 * Radix tree implementation
 * 基数树实现
 * --------------------------------------------------------------------------*/

/* Return the padding needed in the characters section of a node having size
 * 'nodesize'. The padding is needed to store the child pointers to aligned
 * addresses. Note that we add 4 to the node size because the node has a four
 * bytes header. */
/* 为了让字符长度为 'nodesize' 的节点中指向子节点的指针能够按地址对齐，可能需要对字符部分进行
 * 适当的填充，该函数返回该填充长度。
 * 对 nodesize 进行加 4，是因为节点有 4 个字节的头部。 */
#define raxPadding(nodesize) ((sizeof(void*)-((nodesize+4) % sizeof(void*))) & (sizeof(void*)-1))

/* Return the pointer to the last child pointer in a node. For the compressed
 * nodes this is the only child pointer. */
/* 返回指定节点中最后一个指向子节点的指针。
 * 对于压缩的节点，这是唯一的子指针。 */
#define raxNodeLastChildPtr(n) ((raxNode**) ( \
    ((char*)(n)) + \
    raxNodeCurrentLength(n) - \
    sizeof(raxNode*) - \
    (((n)->iskey && !(n)->isnull) ? sizeof(void*) : 0) \
))

/* Return the pointer to the first child pointer. */
/* 返回首个指向子节点的指针 */
#define raxNodeFirstChildPtr(n) ((raxNode**) ( \
    (n)->data + \
    (n)->size + \
    raxPadding((n)->size)))

/* Return the current total size of the node. Note that the second line
 * computes the padding after the string of characters, needed in order to
 * save pointers to aligned addresses. */
/* 返回当前节点总长度 */
#define raxNodeCurrentLength(n) ( \
    /* 4 字节 + 保存字符串长度 + 填充长度 + 子节点地址长度 + 辅助数据长度 */ \
    sizeof(raxNode)+(n)->size+ \
    raxPadding((n)->size)+ \
    ((n)->iscompr ? sizeof(raxNode*) : sizeof(raxNode*)*(n)->size)+ \
    (((n)->iskey && !(n)->isnull)*sizeof(void*)) \
)

/* Allocate a new non compressed node with the specified number of children.
 * If datafiled is true, the allocation is made large enough to hold the
 * associated data pointer.
 * Returns the new node pointer. On out of memory NULL is returned. */
/* 新建一个指定子节点数，且未压缩的基数节点。 */
raxNode *raxNewNode(size_t children, int datafield) {
    size_t nodesize = sizeof(raxNode)+children+raxPadding(children)+
                      sizeof(raxNode*)*children;
    if (datafield) nodesize += sizeof(void*);
    raxNode *node = rax_malloc(nodesize);
    if (node == NULL) return NULL;
    node->iskey = 0;
    node->isnull = 0;
    node->iscompr = 0;
    node->size = children;
    return node;
}

/* Allocate a new rax and return its pointer. On out of memory the function
 * returns NULL. */
/* 新建一个基数树 */
rax *raxNew(void) {
    rax *rax = rax_malloc(sizeof(*rax));
    if (rax == NULL) return NULL;
    rax->numele = 0;
    rax->numnodes = 1;
    rax->head = raxNewNode(0,0);
    if (rax->head == NULL) {
        rax_free(rax);
        return NULL;
    } else {
        return rax;
    }
}

/* realloc the node to make room for auxiliary data in order
 * to store an item in that node. On out of memory NULL is returned. */
/* 为了在节点中保存辅助数据，需要重新分配节点。在内存不足时返回 NULL。 */
raxNode *raxReallocForData(raxNode *n, void *data) {
    if (data == NULL) return n; /* No reallocation needed, setting isnull=1 */
    size_t curlen = raxNodeCurrentLength(n);
    return rax_realloc(n,curlen+sizeof(void*));
}

/* Set the node auxiliary data to the specified pointer. */
/* 将指定的数据指针保存到节点辅助数据中 */
void raxSetData(raxNode *n, void *data) {
    n->iskey = 1;
    if (data != NULL) {
        /* 如果数据指针不为 NULL，设置 isnull = 0。 */
        n->isnull = 0;
        /* 获取节点中辅助数据的地址 */
        void **ndata = (void**)
            ((char*)n+raxNodeCurrentLength(n)-sizeof(void*));
        /* 将数据指针保存到辅助数据中 */
        memcpy(ndata,&data,sizeof(data));
    } else {
        /* 否则，仅设置 isnull = 1。 */
        n->isnull = 1;
    }
}

/* Get the node auxiliary data. */
/* 获取节点的辅助数据地址（紧凑列表地址） */
void *raxGetData(raxNode *n) {
    if (n->isnull) return NULL;
    /* 获取节点中辅助数据的地址 */
    void **ndata =(void**)((char*)n+raxNodeCurrentLength(n)-sizeof(void*));
    void *data;
    memcpy(&data,ndata,sizeof(data));
    return data;
}

/* Add a new child to the node 'n' representing the character 'c' and return
 * its new pointer, as well as the child pointer by reference. Additionally
 * '***parentlink' is populated with the raxNode pointer-to-pointer of where
 * the new child was stored, which is useful for the caller to replace the
 * child pointer if it gets reallocated.
 *
 * On success the new parent node pointer is returned (it may change because
 * of the realloc, so the caller should discard 'n' and use the new value).
 * On out of memory NULL is returned, and the old node is still valid. */
/* 添加新的子节点到指定节点之下。
 * 如果成功，返回指定节点 'n' 的新地址（可能重分配），否则返回 NULL。 */
raxNode *raxAddChild(raxNode *n, unsigned char c, raxNode **childptr, raxNode ***parentlink) {
    assert(n->iscompr == 0);

    /* 获取当前节点长度 */
    size_t curlen = raxNodeCurrentLength(n);
    /* 增加节点 size 值，以便计算新节点的长度。 */
    n->size++;
    size_t newlen = raxNodeCurrentLength(n);
    /* 恢复原节点 size 值，之后成功执行后再更新。 */
    n->size--; /* For now restore the orignal size. We'll update it only on
                  success at the end. */

    /* Alloc the new child we will link to 'n'. */
    /* 分配一个新的子节点 */
    raxNode *child = raxNewNode(0,0);
    if (child == NULL) return NULL;

    /* Make space in the original node. */
    /* 为原节点重新分配更多空间 */
    raxNode *newn = rax_realloc(n,newlen);
    if (newn == NULL) {
        rax_free(child);
        return NULL;
    }
    n = newn;

    /* After the reallocation, we have up to 8/16 (depending on the system
     * pointer size, and the required node padding) bytes at the end, that is,
     * the additional char in the 'data' section, plus one pointer to the new
     * child, plus the padding needed in order to store addresses into aligned
     * locations.
     * 重新分配后，最后我们最多有 8/16（取决于系统指针大小和所需的节点填充）字节的增加，
     * 即节点 'data' 部分中增加的一个字符和为了将之后地址保存到对齐位置所需的填充，
     * 以及一个指向新子节点的指针。
     *
     * So if we start with the following node, having "abde" edges.
     * 假设我们从以下节点开始。
     *
     * Note:
     * - We assume 4 bytes pointer for simplicity.
     * - 系统使用 4 个字节保存指针
     * - Each space below corresponds to one byte
     * - 下面每一个字符都表示一个字节
     *
     * [HDR*][abde][Aptr][Bptr][Dptr][Eptr]|AUXP|
     *
     * After the reallocation we need: 1 byte for the new edge character
     * plus 4 bytes for a new child pointer (assuming 32 bit machine).
     * However after adding 1 byte to the edge char, the header + the edge
     * characters are no longer aligned, so we also need 3 bytes of padding.
     * In total the reallocation will add 1+4+3 bytes = 8 bytes:
     * 为了保存新的数据，需要额外 1 个字节保存字符，3 个字节用于填充，以及 4 个字节
     * 保存子节点的地址，也就是需要申请额外 8 个字节。
     *
     * (Blank bytes are represented by ".")
     * （空白的字节使用 '.' 来表示）
     *
     * [HDR*][abde][Aptr][Bptr][Dptr][Eptr]|AUXP|[....][....]
     *
     * Let's find where to insert the new child in order to make sure
     * it is inserted in-place lexicographically. Assuming we are adding
     * a child "c" in our case pos will be = 2 after the end of the following
     * loop. 
     * 首先，需要找到字符 'c' 需要插入到字符串部分中的位置，同时也是新的指针保存到
     * 指针部分中的位置。 */
    int pos;
    for (pos = 0; pos < n->size; pos++) {
        if (n->data[pos] > c) break;
    }

    /* Now, if present, move auxiliary data pointer at the end
     * so that we can mess with the other data without overwriting it.
     * We will obtain something like that:
     * 如果辅助数据存在，则将节点中指向其的指针移动到节点最后。
     *
     * [HDR*][abde][Aptr][Bptr][Dptr][Eptr][....][....]|AUXP|
     */
    unsigned char *src, *dst;
    if (n->iskey && !n->isnull) {
        src = ((unsigned char*)n+curlen-sizeof(void*));
        dst = ((unsigned char*)n+newlen-sizeof(void*));
        memmove(dst,src,sizeof(void*));
    }

    /* Compute the "shift", that is, how many bytes we need to move the
     * pointers section forward because of the addition of the new child
     * byte in the string section. Note that if we had no padding, that
     * would be always "1", since we are adding a single byte in the string
     * section of the node (where now there is "abde" basically).
     *
     * However we have padding, so it could be zero, or up to 8.
     *
     * Another way to think at the shift is, how many bytes we need to
     * move child pointers forward *other than* the obvious sizeof(void*)
     * needed for the additional pointer itself. */
    /* 计算之后为了保存新的字符 'c' 和可能的填充而将指针部分向后移动的距离。
     * 如果新的字符可以保存到填充中，就不需要移动指针部分了（shift=0）。 */
    size_t shift = newlen - curlen - sizeof(void*);

    /* We said we are adding a node with edge 'c'. The insertion
     * point is between 'b' and 'd', so the 'pos' variable value is
     * the index of the first child pointer that we need to move forward
     * to make space for our new pointer.
     *
     * To start, move all the child pointers after the insertion point
     * of shift+sizeof(pointer) bytes on the right, to obtain:
     * 因为要在 'b' 和 'd' 之间插入新的字符 'c'，对应的也需要在 'Bptr' 和 'Dptr'
     * 之间插入新的子节点指针，为此，首先得将 'Dptr'（第 pos 个指针）及之后的指针进行
     * 后移，移动的长度为 shift + sizeof(pointer)，得到如下：
     *
     * [HDR*][abde][Aptr][Bptr][....][....][Dptr][Eptr]|AUXP|
     */
    /* 获取节点中第 pos 个指向子节点指针的地址 */
    src = n->data+n->size+
          raxPadding(n->size)+
          sizeof(raxNode*)*pos;
    /* 节点后移 */
    memmove(src+shift+sizeof(raxNode*),src,sizeof(raxNode*)*(n->size-pos));

    /* Move the pointers to the left of the insertion position as well. Often
     * we don't need to do anything if there was already some padding to use. In
     * that case the final destination of the pointers will be the same, however
     * in our example there was no pre-existing padding, so we added one byte
     * plus thre bytes of padding. After the next memmove() things will look
     * like thata:
     * 移动新的子节点指针插入位置左侧的指针。如果节点中存在填充可以使用的话，则不需要再做任何移动。
     * 但是当前案例节点中不存在任何填充，所以指针需要向后移动 4 个字节（3 个为填充），结果如下：
     *
     * [HDR*][abde][....][Aptr][Bptr][....][Dptr][Eptr]|AUXP|
     */
    if (shift) {
        src = (unsigned char*) raxNodeFirstChildPtr(n);
        memmove(src+shift,src,sizeof(raxNode*)*pos);
    }

    /* Now make the space for the additional char in the data section,
     * but also move the pointers before the insertion point to the right
     * by shift bytes, in order to obtain the following:
     * 为新字符 'c' 保存到字符串部分中腾出空间。结果如下：
     *
     * [HDR*][ab.d][e...][Aptr][Bptr][....][Dptr][Eptr]|AUXP|
     */
    src = n->data+pos;
    memmove(src+1,src,n->size-pos);

    /* We can now set the character and its child node pointer to get:
     * 将字符和子节点地址保存到节点中。最终如下：
     *
     * [HDR*][abcd][e...][Aptr][Bptr][....][Dptr][Eptr]|AUXP|
     * [HDR*][abcd][e...][Aptr][Bptr][Cptr][Dptr][Eptr]|AUXP|
     */
    n->data[pos] = c;
    n->size++;
    src = (unsigned char*) raxNodeFirstChildPtr(n);
    raxNode **childfield = (raxNode**)(src+sizeof(raxNode*)*pos);
    memcpy(childfield,&child,sizeof(child));
    *childptr = child;
    *parentlink = childfield;
    return n;
}

/* Turn the node 'n', that must be a node without any children, into a
 * compressed node representing a set of nodes linked one after the other
 * and having exactly one child each. The node can be a key or not: this
 * property and the associated value if any will be preserved.
 *
 * The function also returns a child node, since the last node of the
 * compressed chain cannot be part of the chain: it has zero children while
 * we can only compress inner nodes with exactly one child each. */
/* 将没有任何子节点的节点 'n' 转换为一个压缩节点。
 * 该节点有且仅有一个子节点，且节点可以不是 key。 */
raxNode *raxCompressNode(raxNode *n, unsigned char *s, size_t len, raxNode **child) {
    assert(n->size == 0 && n->iscompr == 0);
    void *data = NULL; /* Initialized only to avoid warnings. */
    size_t newsize;

    debugf("Compress node: %.*s\n", (int)len,s);

    /* Allocate the child to link to this node. */
    *child = raxNewNode(0,0);
    if (*child == NULL) return NULL;

    /* Make space in the parent node. */
    newsize = sizeof(raxNode)+len+raxPadding(len)+sizeof(raxNode*);
    if (n->iskey) {
        data = raxGetData(n); /* To restore it later. */
        if (!n->isnull) newsize += sizeof(void*);
    }
    raxNode *newn = rax_realloc(n,newsize);
    if (newn == NULL) {
        rax_free(*child);
        return NULL;
    }
    n = newn;

    n->iscompr = 1;
    n->size = len;
    memcpy(n->data,s,len);
    if (n->iskey) raxSetData(n,data);
    raxNode **childfield = raxNodeLastChildPtr(n);
    memcpy(childfield,child,sizeof(*child));
    return n;
}

/* Low level function that walks the tree looking for the string
 * 's' of 'len' bytes. The function returns the number of characters
 * of the key that was possible to process: if the returned integer
 * is the same as 'len', then it means that the node corresponding to the
 * string was found (however it may not be a key in case the node->iskey is
 * zero or if simply we stopped in the middle of a compressed node, so that
 * 'splitpos' is non zero).
 * 遍历基数树来查询指定字符串。该函数返回匹配的字符长度，如果返回值等于查询字符串长度，
 * 则说明找到指定字符串，但此时的字符串不一定是一个 key，因为节点的 iskey 可能为 0，
 * 或者指定字符串匹配到压缩节点中途就停止了（'splitpos' 不为零）。
 *
 * Otherwise if the returned integer is not the same as 'len', there was an
 * early stop during the tree walk because of a character mismatch.
 * 否则，如果返回的整数值小于 'len'，则是由于字符不匹配，在树遍历的过程中提前停止。
 *
 * The node where the search ended (because the full string was processed
 * or because there was an early stop) is returned by reference as
 * '*stopnode' if the passed pointer is not NULL. This node link in the
 * parent's node is returned as '*plink' if not NULL. Finally, if the
 * search stopped in a compressed node, '*splitpos' returns the index
 * inside the compressed node where the search ended. This is useful to
 * know where to split the node for insertion.
 * 如果 '*stopnode' 不为 NULL，则将搜索的结束节点保存其中。如果 '*plink' 不为 NULL，
 * 则将父节点中指向结束节点的地址将保存其中。如果最终搜索的节点为压缩节点，且 '*splitpos'
 * 不为 NULL，则将节点中匹配的最后一个字符的位置保存到 '*splitpos' 中，该值对之后插入
 * 新数据而拆分节点很有用。
 *
 * Note that when we stop in the middle of a compressed node with
 * a perfect match, this function will return a length equal to the
 * 'len' argument (all the key matched), and will return a *splitpos which is
 * always positive (that will represent the index of the character immediately
 * *after* the last match in the current compressed node).
 *
 * When instead we stop at a compressed node and *splitpos is zero, it
 * means that the current node represents the key (that is, none of the
 * compressed node characters are needed to represent the key, just all
 * its parents nodes). */
static inline size_t raxLowWalk(rax *rax, unsigned char *s, size_t len, raxNode **stopnode, raxNode ***plink, int *splitpos, raxStack *ts) {
    raxNode *h = rax->head;
    raxNode **parentlink = &rax->head;

    /* 指定字符串中当前匹配字符的索引位置，函数最后返回该值。 */
    size_t i = 0; /* Position in the string. */
    /* 节点中当前匹配字符的索引位置 */
    size_t j = 0; /* Position in the node children (or bytes if compressed).*/
    while(h->size && i < len) {
        debugnode("Lookup current node",h);
        unsigned char *v = h->data;

        if (h->iscompr) {
            /* 节点压缩时，只有当前节点中全部的字符，都可以匹配到指定字符串中剩余未匹配的字符时，
             * 才可以继续向子节点进行匹配。 */
            for (j = 0; j < h->size && i < len; j++, i++) {
                if (v[j] != s[i]) break;
            }
            /* 如果不能完全匹配，跳出循环。 */
            if (j != h->size) break;
        } else {
            /* Even when h->size is large, linear scan provides good
             * performances compared to other approaches that are in theory
             * more sounding, like performing a binary search. */
            /* 节点未压缩时，在节点中遍历查询指定字符串中索引 i 位置的字符，
             * 只有查询到，才可以继续向子节点进行匹配。 */
            for (j = 0; j < h->size; j++) {
                if (v[j] == s[i]) break;
            }
            /* 如果未能查询到指定字符，跳出循环。 */
            if (j == h->size) break;
            i++;
        }

        if (ts) raxStackPush(ts,h); /* Save stack of parent nodes. */
        /* 获取新节点（继续匹配的子节点）地址 */
        raxNode **children = raxNodeFirstChildPtr(h);
        if (h->iscompr) j = 0; /* Compressed node only child is at index 0. */
        memcpy(&h,children+j,sizeof(h));
        /* 保存父节点中指向新节点的地址 */
        parentlink = children+j;
        /* 到达这里说明父节点已成功匹配，如果之后不再进行循环（i==len），
         * 则新节点不需要进行拆分（新节点压缩时），将拆分位置设置为 0 。 */
        j = 0; /* If the new node is compressed and we do not
                  iterate again (since i == l) set the split
                  position to 0 to signal this node represents
                  the searched key. */
    }
    debugnode("Lookup stop node is",h);
    if (stopnode) *stopnode = h;
    if (plink) *plink = parentlink;
    /* 如果 splitpos 不为 NULL，且新节点为压缩节点，则设置节点的拆分位置。 */
    if (splitpos && h->iscompr) *splitpos = j;
    return i;
}

/* Insert the element 's' of size 'len', setting as auxiliary data
 * the pointer 'data'. If the element is already present, the associated
 * data is updated (only if 'overwrite' is set to 1), and 0 is returned,
 * otherwise the element is inserted and 1 is returned. On out of memory the
 * function returns 0 as well but sets errno to ENOMEM, otherwise errno will
 * be set to 0.
 */
/* 向基数树中插入指定字符串的 key，并关联指定的数据值。
 * 如果指定的 key 值已存在，则更新 key 关联的数据值（当'overwrite'=1）。
 * 当函数成功插入字符串时返回 1，否则返回 0（更新或异常），
 * 内存不足时，设置 errno 值为 ENOMEM，否则设置为 0。 */
int raxGenericInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old, int overwrite) {
    size_t i;
    /* 拆分位置。
     * 在执行 raxLowWalk() 函数进行匹配之后，如果整个字符串均匹配上，且最终匹配位置停在一个压缩节点中，
     * 则该停止的位置索引会保存在 j 中，也是之后拆分压缩节点的位置。*/
    int j = 0; /* Split position. If raxLowWalk() stops in a compressed
                  node, the index 'j' represents the char we stopped within the
                  compressed node, that is, the position where to split the
                  node for insertion. */
    raxNode *h, **parentlink;

    debugf("### Insert %.*s with value %p\n", (int)len, s, data);
    /* 由上而下遍历基数树，对指定字符串进行匹配。 */
    i = raxLowWalk(rax,s,len,&h,&parentlink,&j,NULL);

    /* If i == len we walked following the whole string. If we are not
     * in the middle of a compressed node, the string is either already
     * inserted or this middle node is currently not a key, but can represent
     * our key. We have just to reallocate the node and make space for the
     * data pointer. */
    /* 如果 i == len，则说明整个字符串都已匹配，如果同时匹配的结束位置不在压缩
     * 节点的中间（节点非压缩或者 j == 0），则说明字符串对应的 key 已经存在，
     * 或者不存在对应的 key，但是可以直接将其设置为 key（不需要拆分节点）。
     * 对于这两种情况，都需要保证有足够的空间来保存数据指针。 */
    if (i == len && (!h->iscompr || j == 0 /* not in the middle if j is 0 */)) {
        debugf("### Insert: node representing key exists\n");
        /* Make space for the value pointer if needed. */
        /* 需要添加数据指针时（非 key，或 key 对应的数据值为 NULL），重新分配节点内存。 */
        if (!h->iskey || (h->isnull && overwrite)) {
            h = raxReallocForData(h,data);
            if (h) memcpy(parentlink,&h,sizeof(h));
        }
        if (h == NULL) {
            errno = ENOMEM;
            return 0;
        }

        /* Update the existing key if there is already one. */
        /* 如果 key 已经存在，更新对应的 value 即可。 */
        if (h->iskey) {
            if (old) *old = raxGetData(h);
            if (overwrite) raxSetData(h,data);
            errno = 0;
            return 0; /* Element already exists. */
        }

        /* Otherwise set the node as a key. Note that raxSetData()
         * will set h->iskey. */
        /* 否则设置数据指针的同时将节点 iskey 设置为 1。 */
        raxSetData(h,data);
        rax->numele++;
        return 1; /* Element inserted. */
    }

    /* If the node we stopped at is a compressed node, we need to
     * split it before to continue.
     * 如果匹配完整个字符串后，是在压缩节点的中间结束的话，那么需要先拆分该节点。
     *
     * Splitting a compressed node have a few possible cases.
     * Imagine that the node 'h' we are currently at is a compressed
     * node contaning the string "ANNIBALE" (it means that it represents
     * nodes A -> N -> N -> I -> B -> A -> L -> E with the only child
     * pointer of this node pointing at the 'E' node, because remember that
     * we have characters at the edges of the graph, not inside the nodes
     * themselves.
     * 拆分一个压缩的节点会有几种可能的情况，下面会依次说明。
     * 假设当前变量 'h' 指向了一个包含字符串 'ANNIBALE' 的压缩节点，由于是压缩节点，
     * 字符串之后仅有一个指针指向子节点。
     *
     * In order to show a real case imagine our node to also point to
     * another compressed node, that finally points at the node without
     * children, representing 'O':
     * 为了更加真实，假设当前节点也指向另一个压缩节点，另一个节点中用大写的 'O' 表示
     * 其指向的节点没有子节点。大致结构如下：
     *
     *     "ANNIBALE" -> "SCO" -> []
     *
     * When inserting we may face the following cases. Note that all the cases
     * require the insertion of a non compressed node with exactly two
     * children, except for the last case which just requires splitting a
     * compressed node.
     * 插入时，我们可能会遇到以下几种情况。需要注意是，除了最后一种情况外，其它的都需要插入
     * 一个只有两个子节点的非压缩节点，而最后一种情况仅需要拆分当前的压缩节点即可。
     *
     * 1) Inserting "ANNIENTARE"
     *
     *               |B| -> "ALE" -> "SCO" -> []
     *     "ANNI" -> |-|
     *               |E| -> (... continue algo ...) "NTARE" -> []
     *
     * 2) Inserting "ANNIBALI"
     *
     *                  |E| -> "SCO" -> []
     *     "ANNIBAL" -> |-|
     *                  |I| -> (... continue algo ...) []
     *
     * 3) Inserting "AGO" (Like case 1, but set iscompr = 0 into original node)
     *
     *            |N| -> "NIBALE" -> "SCO" -> []
     *     |A| -> |-|
     *            |G| -> (... continue algo ...) |O| -> []
     *
     * 4) Inserting "CIAO"
     *
     *     |A| -> "NNIBALE" -> "SCO" -> []
     *     |-|
     *     |C| -> (... continue algo ...) "IAO" -> []
     *
     * 5) Inserting "ANNI"
     *
     *     "ANNI" -> "BALE" -> "SCO" -> []
     *
     * The final algorithm for insertion covering all the above cases is as
     * follows.
     * 包含以上所有情况中的最终插入算法如下。
     *
     * ============================= ALGO 1 =============================
     * 算法一
     *
     * For the above cases 1 to 4, that is, all cases where we stopped in
     * the middle of a compressed node for a character mismatch, do:
     * 对于上述情况中 1 至 4，也就是由于字符不匹配而停在压缩节点中间的情况，执行以下操作：
     *
     * Let $SPLITPOS be the zero-based index at which, in the
     * compressed node array of characters, we found the mismatching
     * character. For example if the node contains "ANNIBALE" and we add
     * "ANNIENTARE" the $SPLITPOS is 4, that is, the index at which the
     * mismatching character is found.
     * 假定 $SPLITPOS 为从零开始的索引，在压缩节点字符数组中该位置发现不匹配的字符。
     * 例如，如果节点包含 'ANNIBALE'，而当添加 'ANNIENTARE' 时，则 $SPLITPOS 为 4，
     * 即找到不匹配字符的索引。
     *
     * 1. Save the current compressed node $NEXT pointer (the pointer to the
     *    child element, that is always present in compressed nodes).
     * 1. 保存当前节点的唯一子节点到 $NEXT 中。
     *
     * 2. Create "split node" having as child the non common letter
     *    at the compressed node. The other non common letter (at the key)
     *    will be added later as we continue the normal insertion algorithm
     *    at step "6".
     * 2. 创建一个包含两个子节点中不相同字符的 '拆分节点'。该步骤执行后，只会将原节点中
     *    的不同字符保存其中，步骤 6 时，才会将另一个不同字符添加进来。
     *
     * 3a. IF $SPLITPOS == 0:
     *     Replace the old node with the split node, by copying the auxiliary
     *     data if any. Fix parent's reference. Free old node eventually
     *     (we still need its data for the next steps of the algorithm).
     * 3a. 如果 $SPLITPOS == 0:
     *     使用拆分节点替换当前的节点位置（修改父节点指向拆分节点），如果当前节点中有辅助
     *     数据，也需要一并设置到拆分节点中。
     *
     * 3b. IF $SPLITPOS != 0:
     *     Trim the compressed node (reallocating it as well) in order to
     *     contain $splitpos characters. Change chilid pointer in order to link
     *     to the split node. If new compressed node len is just 1, set
     *     iscompr to 0 (layout is the same). Fix parent's reference.
     * 3b. 如果 $SPLITPOS != 0:
     *     删除压缩节点中 $SPLITPOS 位置及之后的字符，并将其子节点指向拆分节点。
     *     如果最终拆分节点中只有一个字符，则设置其参数 iscompr 值为 0。
     *
     * 4a. IF the postfix len (the length of the remaining string of the
     *     original compressed node after the split character) is non zero,
     *     create a "postfix node". If the postfix node has just one character
     *     set iscompr to 0, otherwise iscompr to 1. Set the postfix node
     *     child pointer to $NEXT.
     * 4a. 如果后缀字符长度不为 0（源压缩拆分后剩余的字符长度)，则需要创建一个 '后缀节点'
     *     来保存剩余的字符，并设置后缀节点的子节点指针值为 $NEXT。 
     *     如果后缀节点中只有一个字符，则设置其中参数 iscompr 值为 0，否则设置为 1。
     *
     * 4b. IF the postfix len is zero, just use $NEXT as postfix pointer.
     * 4b. 如果后缀字符长度为 0，则使用 $NEXT 作为后缀指针。
     *
     * 5. Set child[0] of split node to postfix node.
     * 5. 设置拆分节点的第一个子节点为后缀节点（如果没有新建的话，则为 $NEXT 指向的节点）。
     *
     * 6. Set the split node as the current node, set current index at child[1]
     *    and continue insertion algorithm as usually.
     * 6. 设置拆分节点为当前节点，将当前索引设置为 child[1]，之后继续执行剩余插入算法。
     *
     * ============================= ALGO 2 =============================
     * 算法二
     *
     * For case 5, that is, if we stopped in the middle of a compressed
     * node but no mismatch was found, do:
     * 对于情况 5，匹配停在某个压缩节点中间，且字符串完全都匹配上，做如下处理：
     *
     * Let $SPLITPOS be the zero-based index at which, in the
     * compressed node array of characters, we stopped iterating because
     * there were no more keys character to match. So in the example of
     * the node "ANNIBALE", addig the string "ANNI", the $SPLITPOS is 4.
     * 假定 $SPLITPOS 为节点中从零开始的字符数组的索引，是由于没有更多字符进行匹配时而停止的位置。
     * 例如，如果节点包含 'ANNIBALE'，而当添加 'ANNI' 时，则 $SPLITPOS 为 4。
     *
     * 1. Save the current compressed node $NEXT pointer (the pointer to the
     *    child element, that is always present in compressed nodes).
     * 1. 保存当前节点的唯一子节点到 $NEXT 中。
     *
     * 2. Create a "postfix node" containing all the characters from $SPLITPOS
     *    to the end. Use $NEXT as the postfix node child pointer.
     *    If the postfix node length is 1, set iscompr to 0.
     *    Set the node as a key with the associated value of the new
     *    inserted key.
     * 2. 创建一个 '后缀节点'，其中包含压缩节点中从 $SPLITPOS 到末尾的所有字符。
     *    使用 $NEXT 作为后缀节点的子指针。 如果后缀节点长度为 1，则将其 iscompr 值设置为 0。
     *    并设置节点的数据指针。
     *
     * 3. Trim the current node to contain the first $SPLITPOS characters.
     *    As usually if the new node length is just 1, set iscompr to 0.
     *    Take the iskey / associated value as it was in the orignal node.
     *    Fix the parent's reference.
     * 3. 裁剪当前节点以包含压缩节点中 $SPLITPOS 位置之前的全部字符。
     *    如果新节点的长度仅为 1，则将其 iscompr 值设置为 0。
     *    设置节点的数据指针，并更新父节点的子节点指针。
     *
     * 4. Set the postfix node as the only child pointer of the trimmed
     *    node created at step 1.
     * 4. 将后缀节点设置为在步骤 1 中创建的裁剪节点的唯一子节点。
     */

    /* ------------------------- ALGORITHM 1 --------------------------- */
    /* 最终匹配停在压缩节点中，且字符串未能完全匹配。 */
    if (h->iscompr && i != len) {
        debugf("ALGO 1: Stopped at compressed node %.*s (%p)\n",
            h->size, h->data, (void*)h);
        debugf("Still to insert: %.*s\n", (int)(len-i), s+i);
        debugf("Splitting at %d: '%c'\n", j, ((char*)h->data)[j]);
        debugf("Other (key) letter is '%c'\n", s[i]);

        /* 1: Save next pointer. */
        /* 步骤一：将压缩节点的唯一子节点地址保存到指针变量 $NEXT 中。 */
        raxNode **childfield = raxNodeLastChildPtr(h);
        raxNode *next;
        memcpy(&next,childfield,sizeof(next));
        debugf("Next is %p\n", (void*)next);
        debugf("iskey %d\n", h->iskey);
        if (h->iskey) {
            debugf("key value is %p\n", raxGetData(h));
        }

        /* Set the length of the additional nodes we will need. */
        /* 计算拆分之后的裁剪长度和后缀长度 */
        size_t trimmedlen = j;
        size_t postfixlen = h->size - j - 1;
        int split_node_is_key = !trimmedlen && h->iskey && !h->isnull;
        size_t nodesize;

        /* 2: Create the split node. Also allocate the other nodes we'll need
         *    ASAP, so that it will be simpler to handle OOM. */
        /* 步骤二：创建拆分节点，并为需要的节点分配空间。
         * 这里拆分节点只创建了一个子节点，考虑只有一个子节点的情况。 */
        raxNode *splitnode = raxNewNode(1, split_node_is_key);
        raxNode *trimmed = NULL;
        raxNode *postfix = NULL;

        /* 申请保存原节点拆分后的前半段字符的节点（裁剪节点） */
        if (trimmedlen) {
            nodesize = sizeof(raxNode)+trimmedlen+raxPadding(trimmedlen)+
                       sizeof(raxNode*);
            /* 如果原节点有数据指针 */
            if (h->iskey && !h->isnull) nodesize += sizeof(void*);
            trimmed = rax_malloc(nodesize);
        }

        /* 申请保存原节点拆分后的后半段字符的节点（后缀节点） */
        if (postfixlen) {
            nodesize = sizeof(raxNode)+postfixlen+raxPadding(postfixlen)+
                       sizeof(raxNode*);
            postfix = rax_malloc(nodesize);
        }

        /* OOM? Abort now that the tree is untouched. */
        /* 如果内存不足时，此时终止操作可以避免影响到原本的基数树。 */
        if (splitnode == NULL ||
            (trimmedlen && trimmed == NULL) ||
            (postfixlen && postfix == NULL))
        {
            rax_free(splitnode);
            rax_free(trimmed);
            rax_free(postfix);
            errno = ENOMEM;
            return 0;
        }
        /* 将压缩节点中首个不同的字符保存到拆分节点中 */
        splitnode->data[0] = h->data[j];

        /* 匹配的结束位置在当前节点的首字符，且指定的字符串未能全部匹配。 */
        if (j == 0) {
            /* 3a: Replace the old node with the split node. */
            /* 步骤三（分支一）：使用拆分节点替换原节点（参考情形四）。 */
            if (h->iskey) {
                /* 将原节点的数据指针保存到拆分节点中 */
                void *ndata = raxGetData(h);
                raxSetData(splitnode,ndata);
            }
            /* 将父节点中指向原节点的指针指向拆分节点 */
            memcpy(parentlink,&splitnode,sizeof(splitnode));
        } else {
            /* 3b: Trim the compressed node. */
            /* 步骤三（分支二）：将拆分后的压缩表前半段的字符保存到裁剪节点中，
             * 并使用裁剪节点替换旧节点。 */
            trimmed->size = j;
            /* 将原节点中前半段字符保存到裁剪节点中 */
            memcpy(trimmed->data,h->data,j);
            trimmed->iscompr = j > 1 ? 1 : 0;
            trimmed->iskey = h->iskey;
            trimmed->isnull = h->isnull;
            if (h->iskey && !h->isnull) {
                void *ndata = raxGetData(h);
                raxSetData(trimmed,ndata);
            }
            /* 将裁剪节点的子节点指向拆分节点 */
            raxNode **cp = raxNodeLastChildPtr(trimmed);
            memcpy(cp,&splitnode,sizeof(splitnode));
            /* 将父节点中指向原节点的指针指向裁剪节点 */
            memcpy(parentlink,&trimmed,sizeof(trimmed));
            /* 将 parentlink 指向裁剪节点中指向拆分节点的地址 */
            parentlink = cp; /* Set parentlink to splitnode parent. */
            rax->numnodes++;
        }

        /* 4: Create the postfix node: what remains of the original
         * compressed node after the split. */
        /* 步骤四：设置后缀节点，保存原节点拆分后剩余的字节。 */
        if (postfixlen) {
            /* 4a: create a postfix node. */
            /* 分支一：设置后缀节点。 */
            postfix->iskey = 0;
            postfix->isnull = 0;
            postfix->size = postfixlen;
            postfix->iscompr = postfixlen > 1;
            /* 将原节点中剩余部分的字节保存到后缀节点中 */
            memcpy(postfix->data,h->data+j+1,postfixlen);
            /* 将后缀节点指向原节点的子节点 */
            raxNode **cp = raxNodeLastChildPtr(postfix);
            memcpy(cp,&next,sizeof(next));
            rax->numnodes++;
        } else {
            /* 4b: just use next as postfix node. */
            /* 分支二：直接使用 $NEXT 作为后缀节点。 */
            postfix = next;
        }

        /* 5: Set splitnode first child as the postfix node. */
        /* 步骤五：将拆分节点的首个子节点指向后缀节点（此时拆分节点只有一个子节点）。 */
        raxNode **splitchild = raxNodeLastChildPtr(splitnode);
        memcpy(splitchild,&postfix,sizeof(postfix));

        /* 6. Continue insertion: this will cause the splitnode to
         * get a new child (the non common character at the currently
         * inserted key). */
        /* 步骤六：设置拆分节点为当前节点，并在之后设置拆分节点第二个子节点。 */
        rax_free(h);
        h = splitnode;

    /* 最终匹配停在压缩节点中，且字符串完全匹配。 */
    } else if (h->iscompr && i == len) {
    /* ------------------------- ALGORITHM 2 --------------------------- */
        debugf("ALGO 2: Stopped at compressed node %.*s (%p) j = %d\n",
            h->size, h->data, (void*)h, j);

        /* Allocate postfix & trimmed nodes ASAP to fail for OOM gracefully. */
        /* 为后缀节点和裁剪节点申请内存空间，如果内存不足时尽快退出。 */
        size_t postfixlen = h->size - j;
        size_t nodesize = sizeof(raxNode)+postfixlen+raxPadding(postfixlen)+
                          sizeof(raxNode*);
        if (data != NULL) nodesize += sizeof(void*);
        raxNode *postfix = rax_malloc(nodesize);

        nodesize = sizeof(raxNode)+j+raxPadding(j)+sizeof(raxNode*);
        if (h->iskey && !h->isnull) nodesize += sizeof(void*);
        raxNode *trimmed = rax_malloc(nodesize);

        if (postfix == NULL || trimmed == NULL) {
            rax_free(postfix);
            rax_free(trimmed);
            errno = ENOMEM;
            return 0;
        }

        /* 1: Save next pointer. */
        /* 步骤一：将压缩节点的唯一子节点地址保存到指针变量 $NEXT 中。 */
        raxNode **childfield = raxNodeLastChildPtr(h);
        raxNode *next;
        memcpy(&next,childfield,sizeof(next));

        /* 2: Create the postfix node. */
        /* 步骤二：设置后缀节点。 */
        postfix->size = postfixlen;
        postfix->iscompr = postfixlen > 1;
        postfix->iskey = 1;
        postfix->isnull = 0;
        memcpy(postfix->data,h->data+j,postfixlen);
        raxSetData(postfix,data);
        /* 将后缀节点的子节点指向原节点的子节点 */
        raxNode **cp = raxNodeLastChildPtr(postfix);
        memcpy(cp,&next,sizeof(next));
        rax->numnodes++;

        /* 3: Trim the compressed node. */
        /* 步骤三：裁剪原压缩节点。 */
        trimmed->size = j;
        trimmed->iscompr = j > 1;
        trimmed->iskey = 0;
        trimmed->isnull = 0;
        memcpy(trimmed->data,h->data,j);
        /* 使用新的裁剪节点替换原压缩节点 */
        memcpy(parentlink,&trimmed,sizeof(trimmed));
        if (h->iskey) {
            void *aux = raxGetData(h);
            raxSetData(trimmed,aux);
        }

        /* Fix the trimmed node child pointer to point to
         * the postfix node. */
        /* 将裁剪节点指向后缀节点 */
        cp = raxNodeLastChildPtr(trimmed);
        memcpy(cp,&postfix,sizeof(postfix));

        /* Finish! We don't need to continue with the insertion
         * algorithm for ALGO 2. The key is already inserted. */
        /* 执行结束，返回。 */
        rax->numele++;
        rax_free(h);
        return 1; /* Key inserted. */
    }

    /* We walked the radix tree as far as we could, but still there are left
     * chars in our string. We need to insert the missing nodes. */
    /* 处理字符串中匹配的剩余字符 */
    while(i < len) {
        raxNode *child;

        /* If this node is going to have a single child, and there
         * are other characters, so that that would result in a chain
         * of single-childed nodes, turn it into a compressed node. */
        /* 如果当前节点是空节点，则将该节点转换为压缩节点，并将字符串中剩余的
         * 字符都保存进去。*/
        if (h->size == 0 && len-i > 1) {
            debugf("Inserting compressed node\n");
            size_t comprsize = len-i;
            if (comprsize > RAX_NODE_MAX_SIZE)
                comprsize = RAX_NODE_MAX_SIZE;
            raxNode *newh = raxCompressNode(h,s+i,comprsize,&child);
            if (newh == NULL) goto oom;
            h = newh;
            /* 使用新的压缩节点替代之前的空节点 */
            memcpy(parentlink,&h,sizeof(h));
            /* 将 parentlink 指向新压缩节点中唯一指向子节点的地址 */
            parentlink = raxNodeLastChildPtr(h);
            i += comprsize;
        } else {
            debugf("Inserting normal node\n");
            raxNode **new_parentlink;
            /* 创建只有一个字符的子节点 */
            raxNode *newh = raxAddChild(h,s[i],&child,&new_parentlink);
            if (newh == NULL) goto oom;
            h = newh;
            /* 由于当前节点添加子节点之后，可能发生重新分配内存的情况，
             * 所以这里重新设置父节点的指针。 */
            memcpy(parentlink,&h,sizeof(h));
            parentlink = new_parentlink;
            i++;
        }
        rax->numnodes++;
        h = child;
    }
    /* 为最终位置的节点申请保存数据指针的空间，并将数据指针值设置其中。 */
    raxNode *newh = raxReallocForData(h,data);
    if (newh == NULL) goto oom;
    h = newh;
    if (!h->iskey) rax->numele++;
    raxSetData(h,data);
    memcpy(parentlink,&h,sizeof(h));
    return 1; /* Element inserted. */

oom:
    /* This code path handles out of memory after part of the sub-tree was
     * already modified. Set the node as a key, and then remove it. However we
     * do that only if the node is a terminal node, otherwise if the OOM
     * happened reallocating a node in the middle, we don't need to free
     * anything. */
    if (h->size == 0) {
        h->isnull = 1;
        h->iskey = 1;
        rax->numele++; /* Compensate the next remove. */
        assert(raxRemove(rax,s,i,NULL) != 0);
    }
    errno = ENOMEM;
    return 0;
}

/* Overwriting insert. Just a wrapper for raxGenericInsert() that will
 * update the element if there is already one for the same key. */
/* 封装 raxGenericInsert() 函数，当 key 存在时进行覆盖。 */
int raxInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old) {
    return raxGenericInsert(rax,s,len,data,old,1);
}

/* Non overwriting insert function: this if an element with the same key
 * exists, the value is not updated and the function returns 0.
 * This is a just a wrapper for raxGenericInsert(). */
/* 封装 raxGenericInsert() 函数，当 key 存在时不进行覆盖。 */
int raxTryInsert(rax *rax, unsigned char *s, size_t len, void *data, void **old) {
    return raxGenericInsert(rax,s,len,data,old,0);
}

/* Find a key in the rax, returns raxNotFound special void pointer value
 * if the item was not found, otherwise the value associated with the
 * item is returned. */
/* 查询基数树中指定 key 关联的数据值。
 * 如果未查询指定的数据值，返回指向表示未能查询到关联数据值的字符串。 */
void *raxFind(rax *rax, unsigned char *s, size_t len) {
    raxNode *h;

    debugf("### Lookup: %.*s\n", (int)len, s);
    int splitpos = 0;
    size_t i = raxLowWalk(rax,s,len,&h,NULL,&splitpos,NULL);
    if (i != len || (h->iscompr && splitpos != 0) || !h->iskey)
        return raxNotFound;
    return raxGetData(h);
}

/* Return the memory address where the 'parent' node stores the specified
 * 'child' pointer, so that the caller can update the pointer with another
 * one if needed. The function assumes it will find a match, otherwise the
 * operation is an undefined behavior (it will continue scanning the
 * memory without any bound checking). */
/* 获取父节点中指向指定子节点的地址。
 * 该函数假设是能够在父节点中找到匹配项的，不然函数会没有限制的不断扫描下去。 */
raxNode **raxFindParentLink(raxNode *parent, raxNode *child) {
    raxNode **cp = raxNodeFirstChildPtr(parent);
    raxNode *c;
    while(1) {
        memcpy(&c,cp,sizeof(c));
        if (c == child) break;
        cp++;
    }
    return cp;
}

/* Low level child removal from node. The new node pointer (after the child
 * removal) is returned. Note that this function does not fix the pointer
 * of the parent node in its parent, so this task is up to the caller.
 * The function never fails for out of memory. */
/* 移除指定父节点中指定子节点，并返回新的节点指针。 */
raxNode *raxRemoveChild(raxNode *parent, raxNode *child) {
    debugnode("raxRemoveChild before", parent);
    /* If parent is a compressed node (having a single child, as for definition
     * of the data structure), the removal of the child consists into turning
     * it into a normal node without children. */
    /* 如果父节点是压缩节点（只有一个子节点），则删除子节点后，父节点就会变成没有子节点的普通节点。 */
    if (parent->iscompr) {
        void *data = NULL;
        /* 保存数据值指针 */
        if (parent->iskey) data = raxGetData(parent);
        parent->isnull = 0;
        parent->iscompr = 0;
        /* 设置 size 为 0，之后的设置数据指针时，就会覆盖指向子节点的地址。 */
        parent->size = 0;
        if (parent->iskey) raxSetData(parent,data);
        debugnode("raxRemoveChild after", parent);
        return parent;
    }

    /* Otherwise we need to scan for the child pointer and memmove()
     * accordingly.
     *
     * 1. To start we seek the first element in both the children
     *    pointers and edge bytes in the node. */
    /* 否则，就需要通过扫描找到指向子节点的位置，并将后续的数据向前移动。
     * 步骤一、首先获取父节点中首个指向子节点的指针地址，和首个字符地址。 */
    raxNode **cp = raxNodeFirstChildPtr(parent);
    raxNode **c = cp;
    unsigned char *e = parent->data;

    /* 2. Search the child pointer to remove inside the array of children
     *    pointers. */
    /* 步骤二、搜索到指定子指针位置及对应字符地址。 */
    while(1) {
        raxNode *aux;
        memcpy(&aux,c,sizeof(aux));
        if (aux == child) break;
        c++;
        e++;
    }

    /* 3. Remove the edge and the pointer by memmoving the remaining children
     *    pointer and edge bytes one position before. */
    /* 步骤三、通过移动后续的子指针和字符的位置，来实现移除指定指针和字符。 */

    /* 获取后续字符长度，并将后续字符前移一位。 */
    int taillen = parent->size - (e - parent->data) - 1;
    debugf("raxRemoveChild tail len: %d\n", taillen);
    memmove(e,e+1,taillen);

    /* Compute the shift, that is the amount of bytes we should move our
     * child pointers to the left, since the removal of one edge character
     * and the corresponding padding change, may change the layout.
     * We just check if in the old version of the node there was at the
     * end just a single byte and all padding: in that case removing one char
     * will remove a whole sizeof(void*) word. */
    /* 获取指向子节点的整体指针需要向前移动的距离（考虑空白填充）。 */
    size_t shift = ((parent->size+4) % sizeof(void*)) == 1 ? sizeof(void*) : 0;

    /* Move the children pointers before the deletion point. */
    /* 向前移动整体指向子节点的指针 */
    if (shift)
        memmove(((char*)cp)-shift,cp,(parent->size-taillen-1)*sizeof(raxNode**));

    /* Move the remaining "tail" pointers at the right position as well. */
    /* 向前移动节点末尾可能存在的数据指针 */
    size_t valuelen = (parent->iskey && !parent->isnull) ? sizeof(void*) : 0;
    memmove(((char*)c)-shift,c+1,taillen*sizeof(raxNode**)+valuelen);

    /* 4. Update size. */
    /* 步骤四、更新节点大小。 */
    parent->size--;

    /* realloc the node according to the theoretical memory usage, to free
     * data if we are over-allocating right now. */
    /* 重新分配节点，以释放数据空间。 */
    raxNode *newnode = rax_realloc(parent,raxNodeCurrentLength(parent));
    if (newnode) {
        debugnode("raxRemoveChild after", newnode);
    }
    /* Note: if rax_realloc() fails we just return the old address, which
     * is valid. */
    /* 如果 rax_realloc() 失败，返回有效的原父节点地址。 */
    return newnode ? newnode : parent;
}

/* Remove the specified item. Returns 1 if the item was found and
 * deleted, 0 otherwise. */
/* 删除指定的字符串项目。
 * 查询到并被删除返回 1，否则返回 0。 */
int raxRemove(rax *rax, unsigned char *s, size_t len, void **old) {
    raxNode *h;
    raxStack ts;

    debugf("### Delete: %.*s\n", (int)len, s);
    raxStackInit(&ts);
    int splitpos = 0;
    /* 在基数树中匹配指定字符串 */
    size_t i = raxLowWalk(rax,s,len,&h,NULL,&splitpos,&ts);
    /* 如果未能查询到，直接返回。 */
    if (i != len || (h->iscompr && splitpos != 0) || !h->iskey) {
        raxStackFree(&ts);
        return 0;
    }
    if (old) *old = raxGetData(h);
    h->iskey = 0;
    rax->numele--;

    /* If this node has no children, the deletion needs to reclaim the
     * no longer used nodes. This is an iterative process that needs to
     * walk the three upward, deleting all the nodes with just one child
     * that are not keys, until the head of the rax is reached or the first
     * node with more than one child is found. */

    /* 是否在删除节点之后对相邻节点进行压缩优化 */
    int trycompress = 0; /* Will be set to 1 if we should try to optimize the
                            tree resulting from the deletion. */

    /* 如果节点没有子节点，则首先需要回收不再使用的节点，该过程需要不断向上迭代删除节点，
     * 直到到达基数树的头部，或者找到持有 key 的节点，再或者找到有多个子节点的节点。
     * 其它情况都表示当前节点中的所以字符都是要删除 key 中独占的部分，可以被整个删除。 */
    if (h->size == 0) {
        debugf("Key deleted in node without children. Cleanup needed.\n");
        raxNode *child = NULL;
        while(h != rax->head) {
            child = h;
            debugf("Freeing child %p [%.*s] key:%d\n", (void*)child,
                (int)child->size, (char*)child->data, child->iskey);
            rax_free(child);
            rax->numnodes--;
            /* 获取父节点 */
            h = raxStackPop(&ts);
             /* If this node has more then one child, or actually holds
              * a key, stop here. */
            /* 节点持有 key，或拥有不止一个子节点，则停止迭代。 */
            if (h->iskey || (!h->iscompr && h->size != 1)) break;
        }
        if (child) {
            debugf("Unlinking child %p from parent %p\n",
                (void*)child, (void*)h);
            /* 移除最上层需要删除的子节点 */
            raxNode *new = raxRemoveChild(h,child);
            /* 节点重新分配内存，更新其父节点中指向当前节点的指针。 */
            if (new != h) {
                raxNode *parent = raxStackPeek(&ts);
                raxNode **parentlink;
                if (parent == NULL) {
                    parentlink = &rax->head;
                } else {
                    parentlink = raxFindParentLink(parent,h);
                }
                /* 将新的节点地址写入之前父节点中 */
                memcpy(parentlink,&new,sizeof(new));
            }

            /* If after the removal the node has just a single child
             * and is not a key, we need to try to compress it. */
            /* 如果移除子节点之后，节点只有一个子节点，并且不持有 key，则尝试之后进行压缩。 */
            if (new->size == 1 && new->iskey == 0) {
                trycompress = 1;
                h = new;
            }
        }
    } else if (h->size == 1) {
        /* If the node had just one child, after the removal of the key
         * further compression with adjacent nodes is pontentially possible. */
        /* 如果节点只有一个子节点，当删除 key 后，尝试之后和相邻节点做进一步压缩。 */
        trycompress = 1;
    }

    /* Don't try node compression if our nodes pointers stack is not
     * complete because of OOM while executing raxLowWalk() */
    /* 如果节点指针栈在执行 raxLowWalk() 函数时由于 OOM 而未完成，则尝试压缩节点。 */
    if (trycompress && ts.oom) trycompress = 0;

    /* Recompression: if trycompress is true, 'h' points to a radix tree node
     * that changed in a way that could allow to compress nodes in this
     * sub-branch. Compressed nodes represent chains of nodes that are not
     * keys and have a single child, so there are two deletion events that
     * may alter the tree so that further compression is needed:
     *
     * 1) A node with a single child was a key and now no longer is a key.
     * 2) A node with two children now has just one child.
     *
     * We try to navigate upward till there are other nodes that can be
     * compressed, when we reach the upper node which is not a key and has
     * a single child, we scan the chain of children to collect the
     * compressable part of the tree, and replace the current node with the
     * new one, fixing the child pointer to reference the first non
     * compressable node.
     *
     * Example of case "1". A tree stores the keys "FOO" = 1 and
     * "FOOBAR" = 2:
     * 案例一、基数树包含键 "FOO" 和 "FOOBAR"，如下：
     *
     *
     * "FOO" -> "BAR" -> [] (2)
     *           (1)
     *
     * After the removal of "FOO" the tree can be compressed as:
     * 删除 "FOO" 之后可以被合并为：
     *
     * "FOOBAR" -> [] (2)
     *
     *
     * Example of case "2". A tree stores the keys "FOOBAR" = 1 and
     * "FOOTER" = 2:
     * 案例二、基数树包含键 "FOOBAR" 和 "FOOTER"， 如下：
     *
     *          |B| -> "AR" -> [] (1)
     * "FOO" -> |-|
     *          |T| -> "ER" -> [] (2)
     *
     * After the removal of "FOOTER" the resulting tree is:
     * 删除 "FOOTER" 后：
     *
     * "FOO" -> |B| -> "AR" -> [] (1)
     *
     * That can be compressed into:
     * 可以被合并为：
     *
     * "FOOBAR" -> [] (1)
     */
    if (trycompress) {
        debugf("After removing %.*s:\n", (int)len, s);
        debugnode("Compression may be needed",h);
        debugf("Seek start node\n");

        /* Try to reach the upper node that is compressible.
         * At the end of the loop 'h' will point to the first node we
         * can try to compress and 'parent' to its parent. */
        /* 尝试向上查询可以合并的节点，最终 'h' 将指向第一个可以合并的节点。 */
        raxNode *parent;
        while(1) {
            parent = raxStackPop(&ts);
            if (!parent || parent->iskey ||
                (!parent->iscompr && parent->size != 1)) break;
            h = parent;
            debugnode("Going up to",h);
        }
        raxNode *start = h; /* Compression starting node. */

        /* Scan chain of nodes we can compress. */
        /* 扫描整个可以合并的节点，确认最终需要合并的节点数量。 */
        size_t comprsize = h->size;
        int nodes = 1;
        while(h->size != 0) {
            /* 能够合并的节点都只有一个子节点 */
            raxNode **cp = raxNodeLastChildPtr(h);
            memcpy(&h,cp,sizeof(h));
            if (h->iskey || (!h->iscompr && h->size != 1)) break;
            /* Stop here if going to the next node would result into
             * a compressed node larger than h->size can hold. */
            /* 如果最终的压缩节点长度超过节点最大长度则停止 */
            if (comprsize + h->size > RAX_NODE_MAX_SIZE) break;
            nodes++;
            comprsize += h->size;
        }
        /* 当存在多个可以合并的节点 */
        if (nodes > 1) {
            /* If we can compress, create the new node and populate it. */
            /* 首先创建一个新的压缩节点 */
            size_t nodesize =
                sizeof(raxNode)+comprsize+raxPadding(comprsize)+sizeof(raxNode*);
            raxNode *new = rax_malloc(nodesize);
            /* An out of memory here just means we cannot optimize this
             * node, but the tree is left in a consistent state. */
            /* 如果内存不足，说明此时不能做优化处理，但是基数树本身还是保持一致状态。 */
            if (new == NULL) {
                raxStackFree(&ts);
                return 1;
            }
            new->iskey = 0;
            new->isnull = 0;
            new->iscompr = 1;
            new->size = comprsize;
            rax->numnodes++;

            /* Scan again, this time to populate the new node content and
             * to fix the new node child pointer. At the same time we free
             * all the nodes that we'll no longer use. */
            /* 再次扫描可以合并的节点，这次需要把节点的内容复制到新节点中，
             * 并同时释放当前节点。 */
            comprsize = 0;
            h = start;
            while(h->size != 0) {
                memcpy(new->data+comprsize,h->data,h->size);
                comprsize += h->size;
                raxNode **cp = raxNodeLastChildPtr(h);
                raxNode *tofree = h;
                memcpy(&h,cp,sizeof(h));
                rax_free(tofree); rax->numnodes--;
                if (h->iskey || (!h->iscompr && h->size != 1)) break;
            }
            debugnode("New node",new);

            /* Now 'h' points to the first node that we still need to use,
             * so our new node child pointer will point to it. */
            /* 将新节点指向第一个不需要合并的节点 */
            raxNode **cp = raxNodeLastChildPtr(new);
            memcpy(cp,&h,sizeof(h));

            /* Fix parent link. */
            /* 找到父节点，并指向新节点。 */
            if (parent) {
                /* 如果第一个可以合并节点的父节点存在，找到父节点中指向该节点的位置，
                 * 将新节点的地址写入其中。 */
                raxNode **parentlink = raxFindParentLink(parent,start);
                memcpy(parentlink,&new,sizeof(new));
            } else {
                rax->head = new;
            }

            debugf("Compressed %d nodes, %d total bytes\n",
                nodes, (int)comprsize);
        }
    }
    raxStackFree(&ts);
    return 1;
}

/* This is the core of raxFree(): performs a depth-first scan of the
 * tree and releases all the nodes found. */
/* 这是 raxFree() 函数的核心：对树执行深度优先扫描，并释放所有找到的节点。 */
void raxRecursiveFree(rax *rax, raxNode *n, void (*free_callback)(void*)) {
    debugnode("free traversing",n);
    int numchildren = n->iscompr ? 1 : n->size;
    /* 获取最后一个子节点指针 */
    raxNode **cp = raxNodeLastChildPtr(n);
    while(numchildren--) {
        raxNode *child;
        memcpy(&child,cp,sizeof(child));
        /* 递归释放子节点 */
        raxRecursiveFree(rax,child,free_callback);
        cp--;
    }
    debugnode("free depth-first",n);
    /* 释放辅助数据 */
    if (free_callback && n->iskey && !n->isnull)
        free_callback(raxGetData(n));
    /* 释放节点 */
    rax_free(n);
    rax->numnodes--;
}

/* Free a whole radix tree, calling the specified callback in order to
 * free the auxiliary data. */
/* 释放整个基数树，并调用指定的回调以释放辅助数据。 */
void raxFreeWithCallback(rax *rax, void (*free_callback)(void*)) {
    raxRecursiveFree(rax,rax->head,free_callback);
    assert(rax->numnodes == 0);
    rax_free(rax);
}

/* Free a whole radix tree. */
/* 释放整个基数树 */
void raxFree(rax *rax) {
    raxFreeWithCallback(rax,NULL);
}

/* ------------------------------- Iterator --------------------------------- */
/* ------------------------------- 基数迭代器 -------------------------------- */

/* Initialize a Rax iterator. This call should be performed a single time
 * to initialize the iterator, and must be followed by a raxSeek() call,
 * otherwise the raxPrev()/raxNext() functions will just return EOF. */
/* 初始化一个一次性的基数迭代器。
 * 该迭代器只能被 raxSeek() 函数调用。 */
void raxStart(raxIterator *it, rax *rt) {
    it->flags = RAX_ITER_EOF; /* No crash if the iterator is not seeked. */
    it->rt = rt;
    it->key_len = 0;
    it->key = it->key_static_string;
    it->key_max = RAX_ITER_STATIC_LEN;
    it->data = NULL;
    it->node_cb = NULL;
    raxStackInit(&it->stack);
}

/* Append characters at the current key string of the iterator 'it'. This
 * is a low level function used to implement the iterator, not callable by
 * the user. Returns 0 on out of memory, otherwise 1 is returned. */
/* 在当前迭代器中 key 值后添加字符串。内存不足时返回 0，否则返回 1。 */
int raxIteratorAddChars(raxIterator *it, unsigned char *s, size_t len) {
    if (it->key_max < it->key_len+len) {
        unsigned char *old = (it->key == it->key_static_string) ? NULL :
                                                                  it->key;
        /* 申请两倍的存储空间 */
        size_t new_max = (it->key_len+len)*2;
        it->key = rax_realloc(old,new_max);
        if (it->key == NULL) {
            /* 内存不足，还原之前的 key 值。 */
            it->key = (!old) ? it->key_static_string : old;
            errno = ENOMEM;
            return 0;
        }
        /* 如果之前 key 值是保持在 key_static_string 中，则需要将值复制到新申请的内存空间中。 */
        if (old == NULL) memcpy(it->key,it->key_static_string,it->key_len);
        it->key_max = new_max;
    }
    /* Use memmove since there could be an overlap between 's' and
     * it->key when we use the current key in order to re-seek. */
    /* 将指定字符串追加到 key 值之后 */
    memmove(it->key+it->key_len,s,len);
    it->key_len += len;
    return 1;
}

/* Remove the specified number of chars from the right of the current
 * iterator key. */
/* 从当前迭代器中 key 值的右侧删除指定数量的字符 */
void raxIteratorDelChars(raxIterator *it, size_t count) {
    it->key_len -= count;
}

/* Do an iteration step towards the next element. At the end of the step the
 * iterator key will represent the (new) current key. If it is not possible
 * to step in the specified direction since there are no longer elements, the
 * iterator is flagged with RAX_ITER_EOF.
 *
 * If 'noup' is true the function starts directly scanning for the next
 * lexicographically smaller children, and the current node is already assumed
 * to be the parent of the last key node, so the first operation to go back to
 * the parent will be skipped. This option is used by raxSeek() when
 * implementing seeking a non existing element with the ">" or "<" options:
 * the starting node is not a key in that particular case, so we start the scan
 * from a node that does not represent the key set.
 *
 * The function returns 1 on success or 0 on out of memory. */
/* 获取迭代器当前的下一个元素，并将其设置到迭代器中。如果不存在下一个元素，则将迭代器标记为 RAX_ITER_EOF。
 *
 * 如果 'noup' 为 false，则函数开始要先向上获取父节点，再开始扫描在字典上较小的下一个节点，
 * 否则直接开始扫描较小的下一个节点。 */
int raxIteratorNextStep(raxIterator *it, int noup) {
    /* 如果已迭代到末尾 */
    if (it->flags & RAX_ITER_EOF) {
        return 1;
    /* 或者刚刚执行完 raxSeek() 函数，还未进行迭代，当前节点即为第一个有效节点。 */
    } else if (it->flags & RAX_ITER_JUST_SEEKED) {
        /* 清除标志位 */
        it->flags &= ~RAX_ITER_JUST_SEEKED;
        return 1;
    }

    /* Save key len, stack items and the node where we are currently
     * so that on iterator EOF we can restore the current key and state. */
    /* 保存原有变量值，在查询无果时恢复当前的节点和状态。 */
    size_t orig_key_len = it->key_len;
    size_t orig_stack_items = it->stack.items;
    raxNode *orig_node = it->node;

    while(1) {
        int children = it->node->iscompr ? 1 : it->node->size;
        /* noup == 0 并且节点包含子节点 */
        if (!noup && children) {
            debugf("GO DEEPER\n");
            /* Seek the lexicographically smaller key in this subtree, which
             * is the first one found always going torwards the first child
             * of every successive node. */
            /* 通过不断向下查询每个节点首个子节点，找出按字典顺序最小的 key 值。 */

            /* 入栈当前迭代器中节点，并获取节点的首个节点地址，添加节点字符到迭代器中。 */
            if (!raxStackPush(&it->stack,it->node)) return 0;
            raxNode **cp = raxNodeFirstChildPtr(it->node);
            if (!raxIteratorAddChars(it,it->node->data,
                it->node->iscompr ? it->node->size : 1)) return 0;
            /* 更新迭代器当前节点指向首个子节点 */
            memcpy(&it->node,cp,sizeof(it->node));
            /* Call the node callback if any, and replace the node pointer
             * if the callback returns true. */
            /* 执行可能存在的回调函数 */
            if (it->node_cb && it->node_cb(&it->node))
                memcpy(cp,&it->node,sizeof(it->node));
            /* For "next" step, stop every time we find a key along the
             * way, since the key is lexicograhically smaller compared to
             * what follows in the sub-children. */
            /* 每次找到 key 值时就停止，因为该 key 值肯定比之后再追加新的字符的 key 值都要小。 */
            if (it->node->iskey) {
                it->data = raxGetData(it->node);
                return 1;
            }
        } else {
            /* If we finished exporing the previous sub-tree, switch to the
             * new one: go upper until a node is found where there are
             * children representing keys lexicographically greater than the
             * current key. */
            /* 向上扫描，直到找到一个节点，在该节点上存在按字典顺序大于当前 key 的子节点。 */
            while(1) {
                int old_noup = noup;

                /* Already on head? Can't go up, iteration finished. */
                /* 向上迭代到头节点，则停止迭代。 */
                if (!noup && it->node == it->rt->head) {
                    /* 设置迭代结束符，并还原迭代器变量值。 */
                    it->flags |= RAX_ITER_EOF;
                    it->stack.items = orig_stack_items;
                    it->key_len = orig_key_len;
                    it->node = orig_node;
                    return 1;
                }
                /* If there are no children at the current node, try parent's
                 * next child. */
                /* 获取当前 key 值最后一个字符，当前父节点是非压缩节点时，表示 key 值在父节点中唯一的字符，
                 * 是压缩节点的话，表示 key 值在整个父节点字符串中的最后一个字符。 */
                unsigned char prevchild = it->key[it->key_len-1];
                /* 如果 noup 等于 false，表示首次向上获取父节点。 */
                if (!noup) {
                    it->node = raxStackPop(&it->stack);
                } else {
                    noup = 0;
                }
                /* Adjust the current key to represent the node we are
                 * at. */
                /* 根据当前节点调整当前 key 值，删除 key 值中在当前节点中对应的字符串。 */
                int todel = it->node->iscompr ? it->node->size : 1;
                raxIteratorDelChars(it,todel);

                /* Try visiting the next child if there was at least one
                 * additional child. */
                /* 因为最终是要查询下一个 key 值，处理的方式是，先获取首个右边相邻的节点，
                 * 再获取该节点下最小的 key 值。所以：
                 * 1、首先节点不能为压缩节点。
                 * 2、当 noup 为 false 时，首次需要向上获取父节点，说明当前节点不满足条件，
                 * 那么父节点需拥有除当前节点之外的节点用来扫描；否则当前有子节点即可进行向下扫描。 */
                if (!it->node->iscompr && it->node->size > (old_noup ? 0 : 1)) {
                    /* 查询父节点中首个大于之前子节点最后一个字符的字符位置和子节点指针 */
                    raxNode **cp = raxNodeFirstChildPtr(it->node);
                    int i = 0;
                    while (i < it->node->size) {
                        debugf("SCAN NEXT %c\n", it->node->data[i]);
                        if (it->node->data[i] > prevchild) break;
                        i++;
                        cp++;
                    }
                    /* 找到满足的子节点 */
                    if (i != it->node->size) {
                        debugf("SCAN found a new node\n");
                        /* 设置迭代器变量，用于下一次迭代。 */
                        raxIteratorAddChars(it,it->node->data+i,1);
                        if (!raxStackPush(&it->stack,it->node)) return 0;
                        memcpy(&it->node,cp,sizeof(it->node));
                        /* Call the node callback if any, and replace the node
                         * pointer if the callback returns true. */
                        /* 执行可能存在的回调函数 */
                        if (it->node_cb && it->node_cb(&it->node))
                            memcpy(cp,&it->node,sizeof(it->node));
                        /* 找到 key 值时就停止，因为该 key 值肯定比之后再追加新的字符的 key 值都要小。 */
                        if (it->node->iskey) {
                            it->data = raxGetData(it->node);
                            return 1;
                        }
                        break;
                    }
                }
            }
        }
    }
}

/* Seek the greatest key in the subtree at the current node. Return 0 on
 * out of memory, otherwise 1. This is an helper function for different
 * iteration functions below. */
/* 在当前节点的子树中寻找最大的 key 值。内存不足返回 0，否则返回1。 */
int raxSeekGreatest(raxIterator *it) {
    while(it->node->size) {
        if (it->node->iscompr) {
            /* 添加整个压缩节点字符 */
            if (!raxIteratorAddChars(it,it->node->data,
                it->node->size)) return 0;
        } else {
            /* 添加非压缩节点最后一个字符 */
            if (!raxIteratorAddChars(it,it->node->data+it->node->size-1,1))
                return 0;
        }
        /* 移动到当前节点最后一个子字节点上 */
        raxNode **cp = raxNodeLastChildPtr(it->node);
        if (!raxStackPush(&it->stack,it->node)) return 0;
        memcpy(&it->node,cp,sizeof(it->node));
    }
    return 1;
}

/* Like raxIteratorNextStep() but implements an iteration step moving
 * to the lexicographically previous element. The 'noup' option has a similar
 * effect to the one of raxIteratorNextStep(). */
/* 与 raxIteratorNextStep() 函数类似，但是是获取按字典顺序排列的前一个元素。 */
int raxIteratorPrevStep(raxIterator *it, int noup) {
    /* 如果已迭代到末尾 */
    if (it->flags & RAX_ITER_EOF) {
        return 1;
    /* 或者刚刚执行完 raxSeek() 函数，还未进行迭代，当前节点即为第一个有效节点。 */
    } else if (it->flags & RAX_ITER_JUST_SEEKED) {
        /* 清除标志位 */
        it->flags &= ~RAX_ITER_JUST_SEEKED;
        return 1;
    }

    /* Save key len, stack items and the node where we are currently
     * so that on iterator EOF we can restore the current key and state. */
    /* 保存原有变量值，在查询无果时恢复当前的节点和状态。 */
    size_t orig_key_len = it->key_len;
    size_t orig_stack_items = it->stack.items;
    raxNode *orig_node = it->node;

    while(1) {
        int old_noup = noup;

        /* Already on head? Can't go up, iteration finished. */
        /* 向上迭代到头节点，则停止迭代。 */
        if (!noup && it->node == it->rt->head) {
            it->flags |= RAX_ITER_EOF;
            it->stack.items = orig_stack_items;
            it->key_len = orig_key_len;
            it->node = orig_node;
            return 1;
        }

        /* 获取当前 key 值最后一个字符，当前父节点是非压缩节点时，表示 key 值在父节点中唯一的字符，
         * 是压缩节点的话，表示 key 值在整个父节点字符串中的最后一个字符。 */
        unsigned char prevchild = it->key[it->key_len-1];
        /* 如果 noup 等于 false，表示首次向上获取父节点。 */
        if (!noup) {
            it->node = raxStackPop(&it->stack);
        } else {
            noup = 0;
        }

        /* Adjust the current key to represent the node we are
         * at. */
        /* 根据当前节点调整当前 key 值，删除 key 值中在当前节点中对应的字符串。 */
        int todel = it->node->iscompr ? it->node->size : 1;
        raxIteratorDelChars(it,todel);

        /* Try visiting the prev child if there is at least one
         * child. */
        /* 因为最终是要查询上一个 key 值，处理的方式是，先获取首个左边相邻的节点，
         * 再获取该节点下最大的 key 值。所以：
         * 1、首先节点不能为压缩节点。
         * 2、当 noup 为 false 时，首次需要向上获取父节点，说明当前节点不满足条件，
         * 那么父节点需拥有除当前节点之外的节点用来扫描；否则当前有子节点即可进行向下扫描。 */
        if (!it->node->iscompr && it->node->size > (old_noup ? 0 : 1)) {
            /* 查询节点中首个小于 key 值在当前节点中的字符，及其对应的子节点指针。 */
            raxNode **cp = raxNodeLastChildPtr(it->node);
            int i = it->node->size-1;
            while (i >= 0) {
                debugf("SCAN PREV %c\n", it->node->data[i]);
                if (it->node->data[i] < prevchild) break;
                i--;
                cp--;
            }
            /* If we found a new subtree to explore in this node,
             * go deeper following all the last children in order to
             * find the key lexicographically greater. */
            /* 如果找到满足的子节点，则获取该子节点下最大的 key 值。 */
            if (i != -1) {
                debugf("SCAN found a new node\n");
                /* Enter the node we just found. */
                /* 向下进入查询到的子节点 */
                if (!raxIteratorAddChars(it,it->node->data+i,1)) return 0;
                if (!raxStackPush(&it->stack,it->node)) return 0;
                memcpy(&it->node,cp,sizeof(it->node));
                /* Seek sub-tree max. */
                /* 获取子树中最大 key 值 */
                if (!raxSeekGreatest(it)) return 0;
            }
        }

        /* Return the key: this could be the key we found scanning a new
         * subtree, or if we did not find a new subtree to explore here,
         * before giving up with this node, check if it's a key itself. */
        /* 有两种情况，一是找到左边相邻最大的 key 值；另一种是向上查询过程中，
         * 当前的节点表示 key，则该 key 值一定比左边相邻节点的子节点中最大 key 值都要大，
         * 可以返回该 key 值。 */
        if (it->node->iskey) {
            it->data = raxGetData(it->node);
            return 1;
        }
    }
}

/* Seek an iterator at the specified element.
 * Return 0 if the seek failed for syntax error or out of memory. Otherwise
 * 1 is returned. When 0 is returned for out of memory, errno is set to
 * the ENOMEM value. */
/* 在一个迭代器上查询指定元素。
 * 如果查询过程中发现语法错误和内存不足，则返回 0，否则返回 1。 */
int raxSeek(raxIterator *it, const char *op, unsigned char *ele, size_t len) {
    int eq = 0, lt = 0, gt = 0, first = 0, last = 0;

    /* 重置迭代器，重头开始查询。 */
    it->stack.items = 0; /* Just resetting. Intialized by raxStart(). */
    it->flags |= RAX_ITER_JUST_SEEKED;
    it->flags &= ~RAX_ITER_EOF;
    it->key_len = 0;
    it->node = NULL;

    /* Set flags according to the operator used to perform the seek. */
    /* 根据参数设置标志 */
    if (op[0] == '>') {
        gt = 1;
        if (op[1] == '=') eq = 1;
    } else if (op[0] == '<') {
        lt = 1;
        if (op[1] == '=') eq = 1;
    } else if (op[0] == '=') {
        eq = 1;
    } else if (op[0] == '^') {
        first = 1;
    } else if (op[0] == '$') {
        last = 1;
    } else {
        errno = 0;
        return 0; /* Error. */
    }

    /* If there are no elements, set the EOF condition immediately and
     * return. */
    /* 如果没有元素，则设置 EOF 并返回。 */
    if (it->rt->numele == 0) {
        it->flags |= RAX_ITER_EOF;
        return 1;
    }

    if (first) {
        /* Seeking the first key greater or equal to the empty string
         * is equivalent to seeking the smaller key available. */
        /* 获取第一个 key 值 */
        return raxSeek(it,">=",NULL,0);
    }

    if (last) {
        /* Find the greatest key taking always the last child till a
         * final node is found. */
        /* 获取最后一个 key 值 */
        it->node = it->rt->head;
        if (!raxSeekGreatest(it)) return 0;
        assert(it->node->iskey);
        it->data = raxGetData(it->node);
        return 1;
    }

    /* We need to seek the specified key. What we do here is to actually
     * perform a lookup, and later invoke the prev/next key code that
     * we already use for iteration. */
    /* 在基数树中查询指定字符串 */
    int splitpos = 0;
    size_t i = raxLowWalk(it->rt,ele,len,&it->node,NULL,&splitpos,&it->stack);

    /* Return OOM on incomplete stack info. */
    if (it->stack.oom) return 0;

    if (eq && i == len && (!it->node->iscompr || splitpos == 0) &&
        it->node->iskey)
    {
        /* We found our node, since the key matches and we have an
         * "equal" condition. */
        /* 如果找到了指定的 key 值，并且有"相等"条件，则可以返回。 */
        if (!raxIteratorAddChars(it,ele,len)) return 0; /* OOM. */
        it->data = raxGetData(it->node);
    } else if (lt || gt) {
        /* Exact key not found or eq flag not set. We have to set as current
         * key the one represented by the node we stopped at, and perform
         * a next/prev operation to seek. To reconstruct the key at this node
         * we start from the parent and go to the current node, accumulating
         * the characters found along the way. */
        /* 没查询到指定的 key 值，或没有"相等"条件。
         * 此时将查询停在的节点作为迭代器当前节点，然后执行 next/prev 操作来查找。 */

        /* 将停止节点入栈，以便之后处理，最终扔需出栈。 */
        if (!raxStackPush(&it->stack,it->node)) return 0;
        /* 将匹配的字符都保存到迭代器中 */
        for (size_t j = 1; j < it->stack.items; j++) {
            raxNode *parent = it->stack.stack[j-1];
            raxNode *child = it->stack.stack[j];
            if (parent->iscompr) {
                /* 压缩节点，添加节点中全部字符。 */
                if (!raxIteratorAddChars(it,parent->data,parent->size))
                    return 0;
            } else {
                /* 非压缩节点，添加匹配子节点对应的字符。 */
                raxNode **cp = raxNodeFirstChildPtr(parent);
                unsigned char *p = parent->data;
                while(1) {
                    raxNode *aux;
                    memcpy(&aux,cp,sizeof(aux));
                    if (aux == child) break;
                    cp++;
                    p++;
                }
                if (!raxIteratorAddChars(it,p,1)) return 0;
            }
        }
        raxStackPop(&it->stack);

        /* We need to set the iterator in the correct state to call next/prev
         * step in order to seek the desired element. */
        debugf("After initial seek: i=%d len=%d key=%.*s\n",
            (int)i, (int)len, (int)it->key_len, it->key);
        if (i != len && !it->node->iscompr) {
            /* If we stopped in the middle of a normal node because of a
             * mismatch, add the mismatching character to the current key
             * and call the iterator with the 'noup' flag so that it will try
             * to seek the next/prev child in the current node directly based
             * on the mismatching character. */
            /* 如果由于不匹配而停在非压缩节点的中间时，需要将不匹配的字符也添加到当前
             * 迭代器中，并通过 'noup' 标志通知 next/prev 函数基于不匹配字符进行处理。 */
            if (!raxIteratorAddChars(it,ele+i,1)) return 0;
            debugf("Seek normal node on mismatch: %.*s\n",
                (int)it->key_len, (char*)it->key);

            it->flags &= ~RAX_ITER_JUST_SEEKED;
            if (lt && !raxIteratorPrevStep(it,1)) return 0;
            if (gt && !raxIteratorNextStep(it,1)) return 0;
            it->flags |= RAX_ITER_JUST_SEEKED; /* Ignore next call. */
        } else if (i != len && it->node->iscompr) {
            debugf("Compressed mismatch: %.*s\n",
                (int)it->key_len, (char*)it->key);
            /* In case of a mismatch within a compressed node. */
            /* 如果压缩节点内不匹配 */
            int nodechar = it->node->data[splitpos];
            int keychar = ele[i];
            it->flags &= ~RAX_ITER_JUST_SEEKED;
            if (gt) {
                /* If the key the compressed node represents is greater
                 * than our seek element, continue forward, otherwise set the
                 * state in order to go back to the next sub-tree. */
                /* 如果节点中字符大于指定键中的字符，则说明当前节点是首个大于指定的 key 值的节点，
                 * 可以直接向下继续查询找到首个 key 值即可；
                 * 否则说明当前节点小于指定的 key 值，需要先向上获取父节点，再向后查询首个大于 key 值的节点。 */
                if (nodechar > keychar) {
                    if (!raxIteratorNextStep(it,0)) return 0;
                } else {
                    if (!raxIteratorAddChars(it,it->node->data,it->node->size))
                        return 0;
                    if (!raxIteratorNextStep(it,1)) return 0;
                }
            }
            if (lt) {
                /* If the key the compressed node represents is smaller
                 * than our seek element, seek the greater key in this
                 * subtree, otherwise set the state in order to go back to
                 * the previous sub-tree. */
                /* 如果节点中字符小于指定键中的字符，则说明当前节点是首个小于指定的 key 值的节点，
                 * 可以直接获取当前节点下的最大 key 值；
                 * 否则说明当前节点小于指定的 key 值，需要先向上获取父节点，再向后查询大于 key 值的节点。 */
                if (nodechar < keychar) {
                    if (!raxSeekGreatest(it)) return 0;
                    it->data = raxGetData(it->node);
                } else {
                    if (!raxIteratorAddChars(it,it->node->data,it->node->size))
                        return 0;
                    if (!raxIteratorPrevStep(it,1)) return 0;
                }
            }
            it->flags |= RAX_ITER_JUST_SEEKED; /* Ignore next call. */
        } else {
            debugf("No mismatch: %.*s\n",
                (int)it->key_len, (char*)it->key);
            /* If there was no mismatch we are into a node representing the
             * key, (but which is not a key or the seek operator does not
             * include 'eq'), or we stopped in the middle of a compressed node
             * after processing all the key. Continue iterating as this was
             * a legitimate key we stopped at. */
            /* 如果没有不匹配的字符，说明当前节点不是 key，或者查询不包含"等于"条件，
             * 再或者匹配停止在压缩节点中间，此时需要继续迭代。 */
            it->flags &= ~RAX_ITER_JUST_SEEKED;
            if (it->node->iscompr && it->node->iskey && splitpos && lt) {
                /* If we stopped in the middle of a compressed node with
                 * perfect match, and the condition is to seek a key "<" than
                 * the specified one, then if this node is a key it already
                 * represents our match. For instance we may have nodes:
                 * 如果匹配停在压缩节点的中间，并且查询包含"小于"条件，那么如果该节点还是 key，
                 * 则表示找到匹配的 key 值，例如，我们可能有以下节点：
                 *
                 * "f" -> "oobar" = 1 -> "" = 2
                 *
                 * Representing keys "f" = 1, "foobar" = 2. A seek for
                 * the key < "foo" will stop in the middle of the "oobar"
                 * node, but will be our match, representing the key "f".
                 * "f" 表示 key 值，假如查询小于 "foo" 的 key 值，则查找将停止在 "oobar" 节点的中间，
                 * 则此时的 "f" 就是需要的结果。
                 *
                 * So in that case, we don't seek backward. */
                it->data = raxGetData(it->node);
            } else {
                /* 其它情况继续迭代查询 */
                if (gt && !raxIteratorNextStep(it,0)) return 0;
                if (lt && !raxIteratorPrevStep(it,0)) return 0;
            }
            it->flags |= RAX_ITER_JUST_SEEKED; /* Ignore next call. */
        }
    } else {
        /* If we are here just eq was set but no match was found. */
        /* 到达这里，说明条件只有"等于"，且没有找到匹配的 key 值。 */
        it->flags |= RAX_ITER_EOF;
        return 1;
    }
    return 1;
}

/* Go to the next element in the scope of the iterator 'it'.
 * If EOF (or out of memory) is reached, 0 is returned, otherwise 1 is
 * returned. In case 0 is returned because of OOM, errno is set to ENOMEM. */
/* 获取指定迭代器范围内下一个 key 值，如果不存在或内存不足返回 0，否则返回 1。 */
int raxNext(raxIterator *it) {
    if (!raxIteratorNextStep(it,0)) {
        errno = ENOMEM;
        return 0;
    }
    if (it->flags & RAX_ITER_EOF) {
        errno = 0;
        return 0;
    }
    return 1;
}

/* Go to the previous element in the scope of the iterator 'it'.
 * If EOF (or out of memory) is reached, 0 is returned, otherwise 1 is
 * returned. In case 0 is returned because of OOM, errno is set to ENOMEM. */
/* 获取指定迭代器范围内上一个 key 值，如果不存在或内存不足返回 0，否则返回 1。 */
int raxPrev(raxIterator *it) {
    if (!raxIteratorPrevStep(it,0)) {
        errno = ENOMEM;
        return 0;
    }
    if (it->flags & RAX_ITER_EOF) {
        errno = 0;
        return 0;
    }
    return 1;
}

/* Perform a random walk starting in the current position of the iterator.
 * Return 0 if the tree is empty or on out of memory. Otherwise 1 is returned
 * and the iterator is set to the node reached after doing a random walk
 * of 'steps' steps. If the 'steps' argument is 0, the random walk is performed
 * using a random number of steps between 1 and two times the logarithm of
 * the number of elements.
 *
 * NOTE: if you use this function to generate random elements from the radix
 * tree, expect a disappointing distribution. A random walk produces good
 * random elements if the tree is not sparse, however in the case of a radix
 * tree certain keys will be reported much more often than others. At least
 * this function should be able to expore every possible element eventually. */
/* 从迭代器的当前位置开始执行随机游走。如果基数树为空或内存不足，则返回 0。
 * 否则，返回 1，并将迭代器设置为在 'step' 步骤之后到达的节点。
 * 如果 'steps' 参数为 0，则设置其为 2 倍于元素数量对数内的随机值。 */
int raxRandomWalk(raxIterator *it, size_t steps) {
    /* 基数树为空 */
    if (it->rt->numele == 0) {
        it->flags |= RAX_ITER_EOF;
        return 0;
    }

    /* 如果 steps == 0，则为其赋值随机值。 */
    if (steps == 0) {
        size_t fle = floor(log(it->rt->numele));
        fle *= 2;
        steps = 1 + rand() % fle;
    }

    raxNode *n = it->node;
    while(steps > 0 || !n->iskey) {
        /* 获取随机子节点 */
        int numchildren = n->iscompr ? 1 : n->size;
        int r = rand() % (numchildren+(n != it->rt->head));

        /* 如果随机值等于子节点个数，则返回父节点，重新随机选择子节点。 */
        if (r == numchildren) {
            /* Go up to parent. */
            n = raxStackPop(&it->stack);
            int todel = n->iscompr ? n->size : 1;
            raxIteratorDelChars(it,todel);
        } else {
            /* Select a random child. */
            /* 选择随机的子节点 */
            if (n->iscompr) {
                if (!raxIteratorAddChars(it,n->data,n->size)) return 0;
            } else {
                if (!raxIteratorAddChars(it,n->data+r,1)) return 0;
            }
            /* 将随机的子节点入栈 */
            raxNode **cp = raxNodeFirstChildPtr(n)+r;
            if (!raxStackPush(&it->stack,n)) return 0;
            memcpy(&n,cp,sizeof(n));
        }
        if (n->iskey) steps--;
    }
    it->node = n;
    it->data = raxGetData(it->node);
    return 1;
}

/* Compare the key currently pointed by the iterator to the specified
 * key according to the specified operator. Returns 1 if the comparison is
 * true, otherwise 0 is returned. */
/* 根据指定的运算符，将迭代器当前指向的 key 值与指定的 key 值进行比较。
 * 如果比较为 true，则返回 1，否则返回 0。 */
int raxCompare(raxIterator *iter, const char *op, unsigned char *key, size_t key_len) {
    int eq = 0, lt = 0, gt = 0;

    /* 根据参数设置标志 */
    if (op[0] == '=' || op[1] == '=') eq = 1;
    if (op[0] == '>') gt = 1;
    else if (op[0] == '<') lt = 1;
    else if (op[1] != '=') return 0; /* Syntax error. */

    /* 对比字符串 */
    size_t minlen = key_len < iter->key_len ? key_len : iter->key_len;
    int cmp = memcmp(iter->key,key,minlen);

    /* Handle == */
    if (lt == 0 && gt == 0) return cmp == 0 && key_len == iter->key_len;

    /* Handle >, >=, <, <= */
    if (cmp == 0) {
        /* Same prefix: longer wins. */
        if (eq && key_len == iter->key_len) return 1;
        else if (lt) return iter->key_len < key_len;
        else if (gt) return iter->key_len > key_len;
        else return 0; /* Avoid warning, just 'eq' is handled before. */
    } else if (cmp > 0) {
        return gt ? 1 : 0;
    } else /* (cmp < 0) */ {
        return lt ? 1 : 0;
    }
}

/* Free the iterator. */
/* 释放基数迭代器 */
void raxStop(raxIterator *it) {
    if (it->key != it->key_static_string) rax_free(it->key);
    raxStackFree(&it->stack);
}

/* Return if the iterator is in an EOF state. This happens when raxSeek()
 * failed to seek an appropriate element, so that raxNext() or raxPrev()
 * will return zero, or when an EOF condition was reached while iterating
 * with raxNext() and raxPrev(). */
/* 判断迭代器是否处于 EOF 状态 */
int raxEOF(raxIterator *it) {
    return it->flags & RAX_ITER_EOF;
}

/* Return the number of elements inside the radix tree. */
/* 返回基数树内部元素个数 */
uint64_t raxSize(rax *rax) {
    return rax->numele;
}

/* ----------------------------- Introspection ------------------------------ */

/* This function is mostly used for debugging and learning purposes.
 * It shows an ASCII representation of a tree on standard output, outling
 * all the nodes and the contained keys.
 *
 * The representation is as follow:
 *
 *  "foobar" (compressed node)
 *  [abc] (normal node with three children)
 *  [abc]=0x12345678 (node is a key, pointing to value 0x12345678)
 *  [] (a normal empty node)
 *
 *  Children are represented in new idented lines, each children prefixed by
 *  the "`-(x)" string, where "x" is the edge byte.
 *
 *  [abc]
 *   `-(a) "ladin"
 *   `-(b) [kj]
 *   `-(c) []
 *
 *  However when a node has a single child the following representation
 *  is used instead:
 *
 *  [abc] -> "ladin" -> []
 */

/* The actual implementation of raxShow(). */
/* raxShow() 的实际实现函数 */
void raxRecursiveShow(int level, int lpad, raxNode *n) {
    char s = n->iscompr ? '"' : '[';
    char e = n->iscompr ? '"' : ']';

    int numchars = printf("%c%.*s%c", s, n->size, n->data, e);
    if (n->iskey) {
        numchars += printf("=%p",raxGetData(n));
    }

    int numchildren = n->iscompr ? 1 : n->size;
    /* Note that 7 and 4 magic constants are the string length
     * of " `-(x) " and " -> " respectively. */
    if (level) {
        lpad += (numchildren > 1) ? 7 : 4;
        if (numchildren == 1) lpad += numchars;
    }
    /* 递归打印全部的子节点信息 */
    raxNode **cp = raxNodeFirstChildPtr(n);
    for (int i = 0; i < numchildren; i++) {
        char *branch = " `-(%c) ";
        if (numchildren > 1) {
            printf("\n");
            for (int j = 0; j < lpad; j++) putchar(' ');
            printf(branch,n->data[i]);
        } else {
            printf(" -> ");
        }
        raxNode *child;
        memcpy(&child,cp,sizeof(child));
        raxRecursiveShow(level+1,lpad,child);
        cp++;
    }
}

/* Show a tree, as outlined in the comment above. */
/* 打印整个基数树 */
void raxShow(rax *rax) {
    raxRecursiveShow(0,0,rax->head);
    putchar('\n');
}

/* Used by debugnode() macro to show info about a given node. */
/* 被 debugnode() 宏调用显示指定节点信息 */
void raxDebugShowNode(const char *msg, raxNode *n) {
    if (raxDebugMsg == 0) return;
    printf("%s: %p [%.*s] key:%d size:%d children:",
        msg, (void*)n, (int)n->size, (char*)n->data, n->iskey, n->size);
    int numcld = n->iscompr ? 1 : n->size;
    raxNode **cldptr = raxNodeLastChildPtr(n) - (numcld-1);
    while(numcld--) {
        raxNode *child;
        memcpy(&child,cldptr,sizeof(child));
        cldptr++;
        printf("%p ", (void*)child);
    }
    printf("\n");
    fflush(stdout);
}

/* Touch all the nodes of a tree returning a check sum. This is useful
 * in order to make Valgrind detect if there is something wrong while
 * reading the data structure.
 *
 * This function was used in order to identify Rax bugs after a big refactoring
 * using this technique:
 *
 * 1. The rax-test is executed using Valgrind, adding a printf() so that for
 *    the fuzz tester we see what iteration in the loop we are in.
 * 2. After every modification of the radix tree made by the fuzz tester
 *    in rax-test.c, we add a call to raxTouch().
 * 3. Now as soon as an operation will corrupt the tree, raxTouch() will
 *    detect it (via Valgrind) immediately. We can add more calls to narrow
 *    the state.
 * 4. At this point a good idea is to enable Rax debugging messages immediately
 *    before the moment the tree is corrupted, to see what happens.
 */
/* 遍历树的所有节点并返回校验和 */
unsigned long raxTouch(raxNode *n) {
    debugf("Touching %p\n", (void*)n);
    unsigned long sum = 0;
    if (n->iskey) {
        sum += (unsigned long)raxGetData(n);
    }

    int numchildren = n->iscompr ? 1 : n->size;
    raxNode **cp = raxNodeFirstChildPtr(n);
    int count = 0;
    for (int i = 0; i < numchildren; i++) {
        if (numchildren > 1) {
            sum += (long)n->data[i];
        }
        raxNode *child;
        memcpy(&child,cp,sizeof(child));
        if (child == (void*)0x65d1760) count++;
        if (count > 1) exit(1);
        sum += raxTouch(child);
        cp++;
    }
    return sum;
}
