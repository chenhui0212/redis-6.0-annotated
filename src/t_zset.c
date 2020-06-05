/*
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2009-2012, Pieter Noordhuis <pcnoordhuis at gmail dot com>
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

/*-----------------------------------------------------------------------------
 * Sorted set API
 *----------------------------------------------------------------------------*/

/* ZSETs are ordered sets using two data structures to hold the same elements
 * in order to get O(log(N)) INSERT and REMOVE operations into a sorted
 * data structure.
 * ZSET 表示有序的集合，使用两种不同的数据结构来存储相同的元素，以达到新增和删除操作的时间
 * 复杂度都是 O(log(N))。
 *
 * The elements are added to a hash table mapping Redis objects to scores.
 * At the same time the elements are added to a skip list mapping scores
 * to Redis objects (so objects are sorted by scores in this "view").
 * 元素以成员值-分值对的形式添加到哈希表中，与此同时，元素也会被添加到跳表中。
 *
 * Note that the SDS string representing the element is the same in both
 * the hash table and skiplist in order to save memory. What we do in order
 * to manage the shared SDS string more easily is to free the SDS string
 * only in zslFreeNode(). The dictionary has no value free method set.
 * So we should always remove an element from the dictionary, and later from
 * the skiplist.
 *
 * This skiplist implementation is almost a C translation of the original
 * algorithm described by William Pugh in "Skip Lists: A Probabilistic
 * Alternative to Balanced Trees", modified in three ways:
 * a) this implementation allows for repeated scores.
 * b) the comparison is not just by key (our 'score') but by satellite data.
 * c) there is a back pointer, so it's a doubly linked list with the back
 * pointers being only at "level 1". This allows to traverse the list
 * from tail to head, useful for ZREVRANGE. */

#include "server.h"
#include <math.h>

/*-----------------------------------------------------------------------------
 * Skiplist implementation of the low level API
 *----------------------------------------------------------------------------*/

int zslLexValueGteMin(sds value, zlexrangespec *spec);
int zslLexValueLteMax(sds value, zlexrangespec *spec);

/* Create a skiplist node with the specified number of levels.
 * The SDS string 'ele' is referenced by the node after the call. */
/* 创建一个指定层数的跳表节点，并将指定的成员值和分值赋值其中。 */
zskiplistNode *zslCreateNode(int level, double score, sds ele) {
    zskiplistNode *zn =
        zmalloc(sizeof(*zn)+level*sizeof(struct zskiplistLevel));
    zn->score = score;
    zn->ele = ele;
    return zn;
}

/* Create a new skiplist. */
/* 创建一个新的跳表 */
zskiplist *zslCreate(void) {
    int j;
    zskiplist *zsl;

    zsl = zmalloc(sizeof(*zsl));
    zsl->level = 1;
    zsl->length = 0;
    /* 创建并初始化拥有最大层数的首节点 */
    zsl->header = zslCreateNode(ZSKIPLIST_MAXLEVEL,0,NULL);
    for (j = 0; j < ZSKIPLIST_MAXLEVEL; j++) {
        zsl->header->level[j].forward = NULL;
        zsl->header->level[j].span = 0;
    }
    zsl->header->backward = NULL;
    zsl->tail = NULL;
    return zsl;
}

/* Free the specified skiplist node. The referenced SDS string representation
 * of the element is freed too, unless node->ele is set to NULL before calling
 * this function. */
/* 释放指定跳表的节点 */
void zslFreeNode(zskiplistNode *node) {
    sdsfree(node->ele);
    zfree(node);
}

/* Free a whole skiplist. */
/* 释放整个跳表 */
void zslFree(zskiplist *zsl) {
    zskiplistNode *node = zsl->header->level[0].forward, *next;

    /* 释放头节点 */
    zfree(zsl->header);
    /* 依次释放所有节点 */
    while(node) {
        next = node->level[0].forward;
        zslFreeNode(node);
        node = next;
    }
    zfree(zsl);
}

/* Returns a random level for the new skiplist node we are going to create.
 * The return value of this function is between 1 and ZSKIPLIST_MAXLEVEL
 * (both inclusive), with a powerlaw-alike distribution where higher
 * levels are less likely to be returned. */
/* 返回将要新建的跳表节点的随机层数，
 * 结果在 1 到 ZSKIPLIST_MAXLEVEL 层之间（前后都包含），越大的值出现的可能性越小。 */
int zslRandomLevel(void) {
    int level = 1;
    while ((random()&0xFFFF) < (ZSKIPLIST_P * 0xFFFF))
        level += 1;
    return (level<ZSKIPLIST_MAXLEVEL) ? level : ZSKIPLIST_MAXLEVEL;
}

/* Insert a new node in the skiplist. Assumes the element does not already
 * exist (up to the caller to enforce that). The skiplist takes ownership
 * of the passed SDS string 'ele'. */
/* 插入一个新节点到跳表中。
 * 调用方需确保成员值不存在，函数返回新节点的地址。 */
zskiplistNode *zslInsert(zskiplist *zsl, double score, sds ele) {
    /* 保存每层将要插入新节点的前置节点指针，
     * 前置节点是指，该节点在当前层的下一个节点指向新节点将要插入的位置。 */
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    /* 保存每层将要插入新节点的前置节点排行（也就是在有序集合中的位置） */
    unsigned int rank[ZSKIPLIST_MAXLEVEL];
    int i, level;

    serverAssert(!isnan(score));
    x = zsl->header;
    /* 根据新节点的分值，在跳表最大层数范围内，依次查找每层将要插入新节点的前置节点。
     * 将该节点排行保存到 rank 数组中，而将该节点指针保存在 update 指针数组中。 */
    for (i = zsl->level-1; i >= 0; i--) {
        /* store rank that is crossed to reach the insert position */
        /* 初始设置当前层的前置节点的排行 */
        rank[i] = i == (zsl->level-1) ? 0 : rank[i+1];
        /* 递进查找当前层的前置节点 */
        while (x->level[i].forward &&
                (x->level[i].forward->score < score ||
                    (x->level[i].forward->score == score &&
                    sdscmp(x->level[i].forward->ele,ele) < 0)))
        {
            /* 增加排行值，并移下一个节点。 */
            rank[i] += x->level[i].span;
            x = x->level[i].forward;
        }
        /* 保存当前层前置节点指针 */
        update[i] = x;
    }

    /* we assume the element is not already inside, since we allow duplicated
     * scores, reinserting the same element should never happen since the
     * caller of zslInsert() should test in the hash table if the element is
     * already inside or not. */
    /* 由于跳表支持相等分值的节点存在，调用方需要通过检查成员值是否存在于字典中，
     * 来保证调用该函数时成员值不重复。 */

    /* 为新节点获取随机层数 */
    level = zslRandomLevel();
    /* 如果层数超过跳表中已存在的节点最大层数 */
    if (level > zsl->level) {
        /* 初始化超过部分的层 */
        for (i = zsl->level; i < level; i++) {
            rank[i] = 0;
            update[i] = zsl->header;
            /* 设置头节点中当前层的跨度 */
            update[i]->level[i].span = zsl->length;
        }
        /* 更新跳表中节点的最大层数 */
        zsl->level = level;
    }

    /* 创建新节点 */
    x = zslCreateNode(level,score,ele);
    for (i = 0; i < level; i++) {
        /* 设置新节点和其前置节点在当前层的前进指针 */
        x->level[i].forward = update[i]->level[i].forward;
        update[i]->level[i].forward = x;

        /* update span covered by update[i] as x is inserted here */
        /* 更新新节点和其前置节点在当前层的跨度值，
         * rank[0] 为新节点左边第一个节点的排行；
         * rank[i] 为在第 i 层，新节点的前置节点的排行；
         * (rank[0] - rank[i]) + 1，也就表示第 i 层的前置节点离新节点的距离，
         * 同理可以计算出新节点在 i 层到下一个节点的距离（也就是前置节点的下一个节点）。 */
        x->level[i].span = update[i]->level[i].span - (rank[0] - rank[i]);
        update[i]->level[i].span = (rank[0] - rank[i]) + 1;
    }

    /* increment span for untouched levels */
    /* 增加未接触层的跨度值，
     * 未接触层是指，超过新节点层数并小于等于最大层数的那些层。
     * 由于新节点的添加，导致在未接触层上，新节点的前置节点的跨度增加。 */
    for (i = level; i < zsl->level; i++) {
        update[i]->level[i].span++;
    }

    /* 设置新节点的后退指针 */
    x->backward = (update[0] == zsl->header) ? NULL : update[0];
    /* 新节点是否插入到末尾 */
    if (x->level[0].forward)
        /* 不是，则将新节点之后节点的后退指针指向新节点 */
        x->level[0].forward->backward = x;
    else
        /* 否则，将尾指针指向新节点 */
        zsl->tail = x;
    /* 跳表长度加一 */
    zsl->length++;
    return x;
}

/* Internal function used by zslDelete, zslDeleteRangeByScore and
 * zslDeleteRangeByRank. */
/* 删除跳表中指定节点，
 * 内部函数，被 zslDelete()、zslDeleteRangeByScore() 和 zslDeleteByRank() 等函数调用。 */
void zslDeleteNode(zskiplist *zsl, zskiplistNode *x, zskiplistNode **update) {
    int i;
    /* 更新指定 x 节点在每层中的前置节点信息 */
    for (i = 0; i < zsl->level; i++) {
        if (update[i]->level[i].forward == x) {
            /* 在接触层上，更新前置节点跨度值，
             * 并将其前进指针指向 x 节点的下一个节点。 */
            update[i]->level[i].span += x->level[i].span - 1;
            update[i]->level[i].forward = x->level[i].forward;
        } else {
            /* 在未接触层上，仅将跨度值减一。 */
            update[i]->level[i].span -= 1;
        }
    }
    /* 指定 x 节点是否是尾节点 */
    if (x->level[0].forward) {
        /* 不是，则更新下一个节点的后退指针为 x 节点的后退指针。 */
        x->level[0].forward->backward = x->backward;
    } else {
        /* 否则，更新尾节点指向 x 节点的前一个节点。 */
        zsl->tail = x->backward;
    }
    /* 更新跳表中节点最大层数，
     * 根据头节点中前进指针来判断，如果删除的 x 节点是最拥有最大层数的节点，
     * 那么必定会导致头节点最大层的前进指针为空，并通过不断向头节点下层判断，
     * 从而计算出减少的层数。 */
    while(zsl->level > 1 && zsl->header->level[zsl->level-1].forward == NULL)
        zsl->level--;
    /* 跳表长度减一 */
    zsl->length--;
}

/* Delete an element with matching score/element from the skiplist.
 * The function returns 1 if the node was found and deleted, otherwise
 * 0 is returned.
 *
 * If 'node' is NULL the deleted node is freed by zslFreeNode(), otherwise
 * it is not freed (but just unlinked) and *node is set to the node pointer,
 * so that it is possible for the caller to reuse the node (including the
 * referenced SDS string at node->ele). */
/* 删除跳表中匹配指定分值和成员值的节点，如果成功删除返回 1，否则返回 0。
 * 
 * 如果参数 node 为空，则删除的节点会被 zslFreeNode() 函数释放，
 * 否则不释放删除节点，并将 node 指向被删除节点。 */
int zslDelete(zskiplist *zsl, double score, sds ele, zskiplistNode **node) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    x = zsl->header;
    /* 获取可能的删除节点在每一层的前置节点的地址 */
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
                (x->level[i].forward->score < score ||
                    (x->level[i].forward->score == score &&
                     sdscmp(x->level[i].forward->ele,ele) < 0)))
        {
            x = x->level[i].forward;
        }
        update[i] = x;
    }
    /* We may have multiple elements with the same score, what we need
     * is to find the element with both the right score and object. */
    /* 获取可能的删除节点 */
    x = x->level[0].forward;
    /* 因为跳表支持同分值的节点，所以需要判断分值和成员值都相等时，才将能删除节点。 */
    if (x && score == x->score && sdscmp(x->ele,ele) == 0) {
        zslDeleteNode(zsl, x, update);
        if (!node)
            zslFreeNode(x);
        else
            *node = x;
        return 1;
    }
    return 0; /* not found */
}

/* Update the score of an elmenent inside the sorted set skiplist.
 * Note that the element must exist and must match 'score'.
 * This function does not update the score in the hash table side, the
 * caller should take care of it.
 *
 * Note that this function attempts to just update the node, in case after
 * the score update, the node would be exactly at the same position.
 * Otherwise the skiplist is modified by removing and re-adding a new
 * element, which is more costly.
 *
 * The function returns the updated element skiplist node pointer. */
/* 更新跳表中指定节点的分值。
 * 节点必须存在，且分值必须匹配才行。且该函数不会更新字典中的分值，调用方需自行处理。
 * 
 * 函数返回指向更新节点的地址。 */
zskiplistNode *zslUpdateScore(zskiplist *zsl, double curscore, sds ele, double newscore) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    int i;

    /* We need to seek to element to update to start: this is useful anyway,
     * we'll have to update or remove it. */
    /* 获取更新节点在每一层的前置节点的地址 */
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
                (x->level[i].forward->score < curscore ||
                    (x->level[i].forward->score == curscore &&
                     sdscmp(x->level[i].forward->ele,ele) < 0)))
        {
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    /* Jump to our element: note that this function assumes that the
     * element with the matching score exists. */
    /* 调整到要被更新的节点，并验证分值和成员值。 */
    x = x->level[0].forward;
    serverAssert(x && curscore == x->score && sdscmp(x->ele,ele) == 0);

    /* If the node, after the score update, would be still exactly
     * at the same position, we can just update the score without
     * actually removing and re-inserting the element in the skiplist. */
    /* 如果更新分值后的节点可以保持原有的位置不动，则仅更新节点分值即可。 */
    if ((x->backward == NULL || x->backward->score < newscore) &&
        (x->level[0].forward == NULL || x->level[0].forward->score > newscore))
    {
        x->score = newscore;
        return x;
    }

    /* No way to reuse the old node: we need to remove and insert a new
     * one at a different place. */
    /* 删除原有节点，并插入新节点。 */
    zslDeleteNode(zsl, x, update);
    zskiplistNode *newnode = zslInsert(zsl,newscore,x->ele);
    /* We reused the old node x->ele SDS string, free the node now
     * since zslInsert created a new one. */
    x->ele = NULL;
    zslFreeNode(x);
    return newnode;
}

/* 判断指定值是否大于（或大于等于）指定范围的最小值 */
int zslValueGteMin(double value, zrangespec *spec) {
    return spec->minex ? (value > spec->min) : (value >= spec->min);
}

/* 判断指定值是否小于（或小于等于）指定范围的最大值 */
int zslValueLteMax(double value, zrangespec *spec) {
    return spec->maxex ? (value < spec->max) : (value <= spec->max);
}

/* Returns if there is a part of the zset is in range. */
/* 判断跳表中在指定范围内是否可能存在节点（大致的范围判断），
 * 可能存在返回 1，否则返回 0。 */
int zslIsInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    /* 排除总为空的范围值 */
    if (range->min > range->max ||
            (range->min == range->max && (range->minex || range->maxex)))
        return 0;
    /* 判断尾节点分值是否小于范围中的最小值 */
    x = zsl->tail;
    if (x == NULL || !zslValueGteMin(x->score,range))
        return 0;
    /* 判断头节点分值是否大于范围中的最大值 */
    x = zsl->header->level[0].forward;
    if (x == NULL || !zslValueLteMax(x->score,range))
        return 0;
    return 1;
}

/* Find the first node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
/* 从跳表中获取指定分值范围的第一个节点，
 * 如果指定范围内不存在任何节点，则返回空。 */
zskiplistNode *zslFirstInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    /* 如果指定范围内不可能存在任何节点，返回空。 */
    if (!zslIsInRange(zsl,range)) return NULL;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        /* 查找首个下个节点的分值符合指定范围最小值的节点 */
        while (x->level[i].forward &&
            !zslValueGteMin(x->level[i].forward->score,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    /* 获取范围内的第一个节点 */
    x = x->level[0].forward;
    serverAssert(x != NULL);

    /* Check if score <= max. */
    /* 检查节点的分值是否符合指定范围最大值 */
    if (!zslValueLteMax(x->score,range)) return NULL;
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
/* 从跳表中获取指定分值范围的最后一个节点，
 * 如果指定范围内不存在任何节点，则返回空。 */
zskiplistNode *zslLastInRange(zskiplist *zsl, zrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    /* 如果指定范围内不可能存在任何节点，返回空。 */
    if (!zslIsInRange(zsl,range)) return NULL;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        /* 查找最后一个节点的分值符合范围最大值的节点 */
        while (x->level[i].forward &&
            zslValueLteMax(x->level[i].forward->score,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    serverAssert(x != NULL);

    /* Check if score >= min. */
    /* 检查节点的分值是否符合指定范围最小值 */
    if (!zslValueGteMin(x->score,range)) return NULL;
    return x;
}

/* Delete all the elements with score between min and max from the skiplist.
 * Min and max are inclusive, so a score >= min || score <= max is deleted.
 * Note that this function takes the reference to the hash table view of the
 * sorted set, in order to remove the elements from the hash table too. */
/* 从跳表中删除分值在指定范围内的节点（前后都是闭区间），
 * 同时也会将节点从字典中删除，函数返回被删除的节点数量。 */
unsigned long zslDeleteRangeByScore(zskiplist *zsl, zrangespec *range, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;

    /* 获取范围最小值在每一层的前置节点的地址 */
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (range->minex ?
            x->level[i].forward->score <= range->min :
            x->level[i].forward->score < range->min))
                x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    /* 获取第一个满足条件的节点 */
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    /* 删除范围内的全部节点 */
    while (x &&
           (range->maxex ? x->score < range->max : x->score <= range->max))
    {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl,x,update);
        dictDelete(dict,x->ele);
        zslFreeNode(x); /* Here is where x->ele is actually released. */
        removed++;
        x = next;
    }
    return removed;
}

/* 从跳表中删除成员值在指定范围内的节点（前后都是闭区间），
 * 同时也会将节点从字典中删除，函数返回被删除的节点数量。 */
unsigned long zslDeleteRangeByLex(zskiplist *zsl, zlexrangespec *range, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long removed = 0;
    int i;


    /* 获取范围最小值在每一层的前置节点的地址 */
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward &&
            !zslLexValueGteMin(x->level[i].forward->ele,range))
                x = x->level[i].forward;
        update[i] = x;
    }

    /* Current node is the last with score < or <= min. */
    /* 获取第一个满足条件的节点 */
    x = x->level[0].forward;

    /* Delete nodes while in range. */
    /* 删除范围内的全部节点 */
    while (x && zslLexValueLteMax(x->ele,range)) {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl,x,update);
        dictDelete(dict,x->ele);
        zslFreeNode(x); /* Here is where x->ele is actually released. */
        removed++;
        x = next;
    }
    return removed;
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based */
/* 从跳表中删除在指定排行范围内的节点（前后都是闭区间），开始和结尾都是基于 1 开始的，
 * 函数同时也会将节点从字典中删除，并最终返回被删除的节点数量。 */
unsigned long zslDeleteRangeByRank(zskiplist *zsl, unsigned int start, unsigned int end, dict *dict) {
    zskiplistNode *update[ZSKIPLIST_MAXLEVEL], *x;
    unsigned long traversed = 0, removed = 0;
    int i;

    /* 获取排行范围开始位置在每一层的前置节点的地址 */
    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        while (x->level[i].forward && (traversed + x->level[i].span) < start) {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        update[i] = x;
    }

    traversed++;
    /* 获取排行起始位置的节点 */
    x = x->level[0].forward;
    /* 删除排行范围内的全部节点 */
    while (x && traversed <= end) {
        zskiplistNode *next = x->level[0].forward;
        zslDeleteNode(zsl,x,update);
        dictDelete(dict,x->ele);
        zslFreeNode(x);
        removed++;
        traversed++;
        x = next;
    }
    return removed;
}

/* Find the rank for an element by both score and key.
 * Returns 0 when the element cannot be found, rank otherwise.
 * Note that the rank is 1-based due to the span of zsl->header to the
 * first element. */
/* 查询指定分值和成员值在跳表中的排行。
 * 如果跳表中不存在指定条件的节点，返回 0，否则返回排行值。 */
unsigned long zslGetRank(zskiplist *zsl, double score, sds ele) {
    zskiplistNode *x;
    unsigned long rank = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* 获取满足分值和成员值在每一层的节点或者前置节点 */
        while (x->level[i].forward &&
            (x->level[i].forward->score < score ||
                (x->level[i].forward->score == score &&
                sdscmp(x->level[i].forward->ele,ele) <= 0))) {
            rank += x->level[i].span;
            x = x->level[i].forward;
        }

        /* x might be equal to zsl->header, so test if obj is non-NULL */
        /* 如果该节点的成员值等于指定的值，说明找到指定节点，返回排行值。
         * 由于开始时，节点 x 可能是头节点，需要判断成员值是否为空。 */
        if (x->ele && sdscmp(x->ele,ele) == 0) {
            return rank;
        }
    }
    return 0;
}

/* Finds an element by its rank. The rank argument needs to be 1-based. */
/* 返回跳表中指定排行的节点（排行基于 1 开始）。 */
zskiplistNode* zslGetElementByRank(zskiplist *zsl, unsigned long rank) {
    zskiplistNode *x;
    unsigned long traversed = 0;
    int i;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* 获取每层最后一个不超过排行的节点 */
        while (x->level[i].forward && (traversed + x->level[i].span) <= rank)
        {
            traversed += x->level[i].span;
            x = x->level[i].forward;
        }
        /* 如果等于排行值，则返回节点。 */
        if (traversed == rank) {
            return x;
        }
    }
    return NULL;
}

/* Populate the rangespec according to the objects min and max. */
/* 对指定分值的 min 和 max 对象进行解析，并将解析值保存到指定范围中。 */
static int zslParseRange(robj *min, robj *max, zrangespec *spec) {
    char *eptr;
    spec->minex = spec->maxex = 0;

    /* Parse the min-max interval. If one of the values is prefixed
     * by the "(" character, it's considered "open". For instance
     * ZRANGEBYSCORE zset (1.5 (2.5 will match min < x < max
     * ZRANGEBYSCORE zset 1.5 2.5 will instead match min <= x <= max */
    /* 解析范围的区间。如果值以 '(' 开头，则表示是开区间，没有则表示是闭区间。
     * 比如：
     * ZRANGEBYSCORE zset (1.5 (2.5 ---> 匹配 min < x < max
     * ZRANGEBYSCORE zset 1.5 2.5   ---> 匹配 min <= x <= max */
    if (min->encoding == OBJ_ENCODING_INT) {
        spec->min = (long)min->ptr;
    } else {
        if (((char*)min->ptr)[0] == '(') {
            spec->min = strtod((char*)min->ptr+1,&eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return C_ERR;
            spec->minex = 1;
        } else {
            spec->min = strtod((char*)min->ptr,&eptr);
            if (eptr[0] != '\0' || isnan(spec->min)) return C_ERR;
        }
    }
    if (max->encoding == OBJ_ENCODING_INT) {
        spec->max = (long)max->ptr;
    } else {
        if (((char*)max->ptr)[0] == '(') {
            spec->max = strtod((char*)max->ptr+1,&eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return C_ERR;
            spec->maxex = 1;
        } else {
            spec->max = strtod((char*)max->ptr,&eptr);
            if (eptr[0] != '\0' || isnan(spec->max)) return C_ERR;
        }
    }

    return C_OK;
}

/* ------------------------ Lexicographic ranges ---------------------------- */

/* Parse max or min argument of ZRANGEBYLEX.
  * (foo means foo (open interval)
  * [foo means foo (closed interval)
  * - means the min string possible
  * + means the max string possible
  *
  * If the string is valid the *dest pointer is set to the redis object
  * that will be used for the comparison, and ex will be set to 0 or 1
  * respectively if the item is exclusive or inclusive. C_OK will be
  * returned.
  *
  * If the string is not a valid range C_ERR is returned, and the value
  * of *dest and *ex is undefined. */
/* 解析 ZRANGEBYLEX 命令的 min 和 max 参数。 
 * '(' 开头表示开区间
 * '[' 开头表示闭区间
 * '-' 表示可能的最小值
 * '+' 表中可能的最大值 */
int zslParseLexRangeItem(robj *item, sds *dest, int *ex) {
    char *c = item->ptr;

    switch(c[0]) {
    case '+':
        if (c[1] != '\0') return C_ERR;
        *ex = 1;
        *dest = shared.maxstring;
        return C_OK;
    case '-':
        if (c[1] != '\0') return C_ERR;
        *ex = 1;
        *dest = shared.minstring;
        return C_OK;
    case '(':
        *ex = 1;
        *dest = sdsnewlen(c+1,sdslen(c)-1);
        return C_OK;
    case '[':
        *ex = 0;
        *dest = sdsnewlen(c+1,sdslen(c)-1);
        return C_OK;
    default:
        return C_ERR;
    }
}

/* Free a lex range structure, must be called only after zelParseLexRange()
 * populated the structure with success (C_OK returned). */
/* 释放一个范围的结构体 */
void zslFreeLexRange(zlexrangespec *spec) {
    if (spec->min != shared.minstring &&
        spec->min != shared.maxstring) sdsfree(spec->min);
    if (spec->max != shared.minstring &&
        spec->max != shared.maxstring) sdsfree(spec->max);
}

/* Populate the lex rangespec according to the objects min and max.
 *
 * Return C_OK on success. On error C_ERR is returned.
 * When OK is returned the structure must be freed with zslFreeLexRange(),
 * otherwise no release is needed. */
/* 对指定成员值的 min 和 max 对象进行解析，并将解析值保存到指定范围中。 */
int zslParseLexRange(robj *min, robj *max, zlexrangespec *spec) {
    /* The range can't be valid if objects are integer encoded.
     * Every item must start with ( or [. */
    /* 前后范围值都不能为整型，必须是以 '(' 或 '[' 开头的字符串。 */
    if (min->encoding == OBJ_ENCODING_INT ||
        max->encoding == OBJ_ENCODING_INT) return C_ERR;

    spec->min = spec->max = NULL;
    if (zslParseLexRangeItem(min, &spec->min, &spec->minex) == C_ERR ||
        zslParseLexRangeItem(max, &spec->max, &spec->maxex) == C_ERR) {
        zslFreeLexRange(spec);
        return C_ERR;
    } else {
        return C_OK;
    }
}

/* This is just a wrapper to sdscmp() that is able to
 * handle shared.minstring and shared.maxstring as the equivalent of
 * -inf and +inf for strings */
/* 支持处理 shared.minstring 和 shared.maxstring 的字符串比较函数 */
int sdscmplex(sds a, sds b) {
    if (a == b) return 0;
    if (a == shared.minstring || b == shared.maxstring) return -1;
    if (a == shared.maxstring || b == shared.minstring) return 1;
    return sdscmp(a,b);
}

/* 判断指定字符串值是否大于（或大于等于）指定范围的最小字符串 */
int zslLexValueGteMin(sds value, zlexrangespec *spec) {
    return spec->minex ?
        (sdscmplex(value,spec->min) > 0) :
        (sdscmplex(value,spec->min) >= 0);
}

/* 判断指定字符串值是否小于（或小于等于）指定范围的最大字符串 */
int zslLexValueLteMax(sds value, zlexrangespec *spec) {
    return spec->maxex ?
        (sdscmplex(value,spec->max) < 0) :
        (sdscmplex(value,spec->max) <= 0);
}

/* Returns if there is a part of the zset is in the lex range. */
/* 判断跳表中在指定成员值范围内是否可能存在节点（大致的范围判断），
 * 可能存在返回 1，否则返回 0。 */
int zslIsInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;

    /* Test for ranges that will always be empty. */
    /* 排除总为空的范围值 */
    int cmp = sdscmplex(range->min,range->max);
    if (cmp > 0 || (cmp == 0 && (range->minex || range->maxex)))
        return 0;
    /* 判断尾节点的成员值是否小于范围中的最小字符串 */
    x = zsl->tail;
    if (x == NULL || !zslLexValueGteMin(x->ele,range))
        return 0;
    /* 判断头节点的成员值是否大于范围中的最大字符串 */
    x = zsl->header->level[0].forward;
    if (x == NULL || !zslLexValueLteMax(x->ele,range))
        return 0;
    return 1;
}

/* Find the first node that is contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */
/* 从跳表中获取指定成员值范围的第一个节点，
 * 如果指定范围内不存在任何节点，则返回空。 */
zskiplistNode *zslFirstInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    /* 如果指定范围内不可能存在任何节点，返回空。 */
    if (!zslIsInLexRange(zsl,range)) return NULL;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *OUT* of range. */
        /* 查找首个下个节点的成员值符合指定范围最小值的节点 */
        while (x->level[i].forward &&
            !zslLexValueGteMin(x->level[i].forward->ele,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so the next node cannot be NULL. */
    /* 获取范围内的第一个节点 */
    x = x->level[0].forward;
    serverAssert(x != NULL);

    /* Check if score <= max. */
    /* 检查节点的成员值是否符合指定范围最大值 */
    if (!zslLexValueLteMax(x->ele,range)) return NULL;
    return x;
}

/* Find the last node that is contained in the specified range.
 * Returns NULL when no element is contained in the range. */
/* 从跳表中获取指定成员值范围的最后一个节点，
 * 如果指定范围内不存在任何节点，则返回空。 */
zskiplistNode *zslLastInLexRange(zskiplist *zsl, zlexrangespec *range) {
    zskiplistNode *x;
    int i;

    /* If everything is out of range, return early. */
    /* 如果指定范围内不可能存在任何节点，返回空。 */
    if (!zslIsInLexRange(zsl,range)) return NULL;

    x = zsl->header;
    for (i = zsl->level-1; i >= 0; i--) {
        /* Go forward while *IN* range. */
        /* 查找最后一个节点的成员值符合指定范围最大值的节点 */
        while (x->level[i].forward &&
            zslLexValueLteMax(x->level[i].forward->ele,range))
                x = x->level[i].forward;
    }

    /* This is an inner range, so this node cannot be NULL. */
    serverAssert(x != NULL);

    /* Check if score >= min. */
    /* 检查节点的成员值是否符合指定范围最小值 */
    if (!zslLexValueGteMin(x->ele,range)) return NULL;
    return x;
}

/*-----------------------------------------------------------------------------
 * Ziplist-backed sorted set API
 *----------------------------------------------------------------------------*/

/* 从压缩表指定位置获取分值 */
double zzlGetScore(unsigned char *sptr) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    char buf[128];
    double score;

    serverAssert(sptr != NULL);
    serverAssert(ziplistGet(sptr,&vstr,&vlen,&vlong));

    if (vstr) {
        /* 如果是字符串，需要转换为整型。 */
        memcpy(buf,vstr,vlen);
        buf[vlen] = '\0';
        score = strtod(buf,NULL);
    } else {
        score = vlong;
    }

    return score;
}

/* Return a ziplist element as an SDS string. */
/* 从压缩表指定位置获取成员值 */
sds ziplistGetObject(unsigned char *sptr) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;

    serverAssert(sptr != NULL);
    serverAssert(ziplistGet(sptr,&vstr,&vlen,&vlong));

    if (vstr) {
        return sdsnewlen((char*)vstr,vlen);
    } else {
        return sdsfromlonglong(vlong);
    }
}

/* Compare element in sorted set with given element. */
/* 对比 eptr 处的成员值和给定的 cstr 处值 */
int zzlCompareElements(unsigned char *eptr, unsigned char *cstr, unsigned int clen) {
    unsigned char *vstr;
    unsigned int vlen;
    long long vlong;
    unsigned char vbuf[32];
    int minlen, cmp;

    /* 获取成员值 */
    serverAssert(ziplistGet(eptr,&vstr,&vlen,&vlong));
    /* 如果是以整型保存的，需要转换为字符串。 */
    if (vstr == NULL) {
        /* Store string representation of long long in buf. */
        vlen = ll2string((char*)vbuf,sizeof(vbuf),vlong);
        vstr = vbuf;
    }

    /* 进行对比 */
    minlen = (vlen < clen) ? vlen : clen;
    cmp = memcmp(vstr,cstr,minlen);
    if (cmp == 0) return vlen-clen;
    return cmp;
}

/* 返回有序集合的节点数量 */
unsigned int zzlLength(unsigned char *zl) {
    return ziplistLen(zl)/2;
}

/* Move to next entry based on the values in eptr and sptr. Both are set to
 * NULL when there is no next entry. */
/* 根据 eptr 和 sptr 当前指向位置，移动它们分别指向下一个成员值和下一个分值。
 * 如果下一个节点不存在，这两个指针都会被设置为空。 */
void zzlNext(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;
    serverAssert(*eptr != NULL && *sptr != NULL);

    /* 获取下一个成员值 */
    _eptr = ziplistNext(zl,*sptr);
    if (_eptr != NULL) {
        /* 获取下一个分值 */
        _sptr = ziplistNext(zl,_eptr);
        serverAssert(_sptr != NULL);
    } else {
        /* No next entry. */
        _sptr = NULL;
    }

    *eptr = _eptr;
    *sptr = _sptr;
}

/* Move to the previous entry based on the values in eptr and sptr. Both are
 * set to NULL when there is no next entry. */
/* 根据 eptr 和 sptr 当前指向位置，移动它们分别指向上一个成员值和上一个分值。
 * 如果上一个节点不存在，这两个指针都会被设置为空。 */
void zzlPrev(unsigned char *zl, unsigned char **eptr, unsigned char **sptr) {
    unsigned char *_eptr, *_sptr;
    serverAssert(*eptr != NULL && *sptr != NULL);

    /* 获取上一个分值 */
    _sptr = ziplistPrev(zl,*eptr);
    if (_sptr != NULL) {
        /* 获取上一个成员值 */
        _eptr = ziplistPrev(zl,_sptr);
        serverAssert(_eptr != NULL);
    } else {
        /* No previous entry. */
        _eptr = NULL;
    }

    *eptr = _eptr;
    *sptr = _sptr;
}

/* Returns if there is a part of the zset is in range. Should only be used
 * internally by zzlFirstInRange and zzlLastInRange. */
/* 判断压缩表中在指定分值范围内是否可能存在节点（大致的范围判断），
 * 可能存在返回 1，否则返回 0。 */
int zzlIsInRange(unsigned char *zl, zrangespec *range) {
    unsigned char *p;
    double score;

    /* Test for ranges that will always be empty. */
    /* 排除总为空的范围值 */
    if (range->min > range->max ||
            (range->min == range->max && (range->minex || range->maxex)))
        return 0;

    /* 获取最后一个元素的节点地址 */
    p = ziplistIndex(zl,-1); /* Last score. */
    if (p == NULL) return 0; /* Empty sorted set */
    /* 获取最后一个节点的分值（最大值），判断其是否小于范围中的最小值。 */
    score = zzlGetScore(p);
    if (!zslValueGteMin(score,range))
        return 0;

    /* 获取第二个元素的节点地址 */
    p = ziplistIndex(zl,1); /* First score. */
    serverAssert(p != NULL);
    /* 获取第一个节点的分值（最小值），判断其是否大于范围中的最大值。 */
    score = zzlGetScore(p);
    if (!zslValueLteMax(score,range))
        return 0;

    return 1;
}

/* Find pointer to the first element contained in the specified range.
 * Returns NULL when no element is contained in the range. */
/* 从压缩表中获取指定范围的第一个节点，如果指定范围内不存在任何节点，则返回空。 */
unsigned char *zzlFirstInRange(unsigned char *zl, zrangespec *range) {
    /* 获取压缩表第一个元素地址（第一个成员值地址） */
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;
    double score;

    /* If everything is out of range, return early. */
    /* 如果指定范围内不可能存在任何节点，返回空。 */
    if (!zzlIsInRange(zl,range)) return NULL;

    /* 正向遍历压缩表中元素 */
    while (eptr != NULL) {
        /* 获取分值所在地址 */
        sptr = ziplistNext(zl,eptr);
        serverAssert(sptr != NULL);

        /* 获取分值，判断其是否大于等于范围的最小值。 */
        score = zzlGetScore(sptr);
        if (zslValueGteMin(score,range)) {
            /* Check if score <= max. */
            /* 如果同时满足小于等于范围的最大值，则返回成员值地址。 */
            if (zslValueLteMax(score,range))
                return eptr;
            return NULL;
        }

        /* Move to next element. */
        eptr = ziplistNext(zl,sptr);
    }

    return NULL;
}

/* Find pointer to the last element contained in the specified range.
 * Returns NULL when no element is contained in the range. */
/* 从压缩表中获取指定分值范围的最后一个节点，
 * 如果指定范围内不存在任何节点，则返回空。 */
unsigned char *zzlLastInRange(unsigned char *zl, zrangespec *range) {
    /* 获取压缩表倒数第二个元素地址（倒数第一个成员值地址） */
    unsigned char *eptr = ziplistIndex(zl,-2), *sptr;
    double score;

    /* If everything is out of range, return early. */
    /* 如果指定范围内不可能存在任何节点，返回空。 */
    if (!zzlIsInRange(zl,range)) return NULL;

    /* 逆向遍历压缩表中元素 */
    while (eptr != NULL) {
        /* 获取分值所在地址 */
        sptr = ziplistNext(zl,eptr);
        serverAssert(sptr != NULL);

        /* 获取分值，判断其是否小于等于范围的最大值。 */
        score = zzlGetScore(sptr);
        if (zslValueLteMax(score,range)) {
            /* Check if score >= min. */
            /* 如果同时满足大于等于范围的最小值，则返回成员值地址。 */
            if (zslValueGteMin(score,range))
                return eptr;
            return NULL;
        }

        /* Move to previous element by moving to the score of previous element.
         * When this returns NULL, we know there also is no element. */
        /* 获取前一个节点的分值地址和成员值地址 */
        sptr = ziplistPrev(zl,eptr);
        if (sptr != NULL)
            serverAssert((eptr = ziplistPrev(zl,sptr)) != NULL);
        else
            eptr = NULL;
    }

    return NULL;
}

/* 判断压缩表中指定地址对应的成员值是否大于（或大于等于）指定范围的最小字符串 */
int zzlLexValueGteMin(unsigned char *p, zlexrangespec *spec) {
    sds value = ziplistGetObject(p);
    int res = zslLexValueGteMin(value,spec);
    sdsfree(value);
    return res;
}

/* 判断压缩表中指定地址对应的成员值是否小于（或小于等于）指定范围的最大字符串 */
int zzlLexValueLteMax(unsigned char *p, zlexrangespec *spec) {
    sds value = ziplistGetObject(p);
    int res = zslLexValueLteMax(value,spec);
    sdsfree(value);
    return res;
}

/* Returns if there is a part of the zset is in range. Should only be used
 * internally by zzlFirstInRange and zzlLastInRange. */
/* 判断压缩表中在指定范围内是否可能存在节点（大致的范围判断），
 * 可能存在返回 1，否则返回 0。 */
int zzlIsInLexRange(unsigned char *zl, zlexrangespec *range) {
    unsigned char *p;

    /* Test for ranges that will always be empty. */
    /* 排除总为空的范围值 */
    int cmp = sdscmplex(range->min,range->max);
    if (cmp > 0 || (cmp == 0 && (range->minex || range->maxex)))
        return 0;

    /* 判断尾节点分值是否小于范围中的最小值 */
    p = ziplistIndex(zl,-2); /* Last element. */
    if (p == NULL) return 0;
    if (!zzlLexValueGteMin(p,range))
        return 0;

    /* 判断头节点分值是否大于范围中的最大值 */
    p = ziplistIndex(zl,0); /* First element. */
    serverAssert(p != NULL);
    if (!zzlLexValueLteMax(p,range))
        return 0;

    return 1;
}

/* Find pointer to the first element contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */
/* 从压缩表中获取指定成员值范围的第一个节点，
 * 如果指定范围内不存在任何节点，则返回空。 */
unsigned char *zzlFirstInLexRange(unsigned char *zl, zlexrangespec *range) {
    /* 获取压缩表第一个元素地址（第一个成员值地址） */
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;

    /* If everything is out of range, return early. */
    /* 如果指定范围内不可能存在任何节点，返回空。 */
    if (!zzlIsInLexRange(zl,range)) return NULL;

    /* 正向遍历压缩表中元素 */
    while (eptr != NULL) {
        /* 判断成员值是否大于等于指定范围的最小值 */
        if (zzlLexValueGteMin(eptr,range)) {
            /* Check if score <= max. */
            /* 如果同时满足小于等于指定范围的最大值，则返回该成员值地址。 */
            if (zzlLexValueLteMax(eptr,range))
                return eptr;
            return NULL;
        }

        /* Move to next element. */
        /* 获取前一个节点的分值地址和成员值地址 */
        sptr = ziplistNext(zl,eptr); /* This element score. Skip it. */
        serverAssert(sptr != NULL);
        eptr = ziplistNext(zl,sptr); /* Next element. */
    }

    return NULL;
}

/* Find pointer to the last element contained in the specified lex range.
 * Returns NULL when no element is contained in the range. */
/* 从压缩表中获取指定成员值范围的最后一个节点，
 * 如果指定范围内不存在任何节点，则返回空。 */
unsigned char *zzlLastInLexRange(unsigned char *zl, zlexrangespec *range) {
    /* 获取压缩表倒数第二个元素地址（倒数第一个成员值地址） */
    unsigned char *eptr = ziplistIndex(zl,-2), *sptr;

    /* If everything is out of range, return early. */
    /* 如果指定范围内不可能存在任何节点，返回空。 */
    if (!zzlIsInLexRange(zl,range)) return NULL;

    /* 逆向遍历压缩表中元素 */
    while (eptr != NULL) {
        /* 判断成员值是否小于等于指定范围的最大值 */
        if (zzlLexValueLteMax(eptr,range)) {
            /* Check if score >= min. */
            /* 如果同时满足大于等于指定范围的最小值，则返回该成员值地址。 */
            if (zzlLexValueGteMin(eptr,range))
                return eptr;
            return NULL;
        }

        /* Move to previous element by moving to the score of previous element.
         * When this returns NULL, we know there also is no element. */
        /* 获取前一个节点的分值地址和成员值地址 */
        sptr = ziplistPrev(zl,eptr);
        if (sptr != NULL)
            serverAssert((eptr = ziplistPrev(zl,sptr)) != NULL);
        else
            eptr = NULL;
    }

    return NULL;
}

/* 从压缩表中查询指定成员值。
 * 如果存在，返回成员值地址，否则返回空。
 * 如果参数 '*score' 不为空，则将成员值对应的分值取出，存放到该参数中。 */
unsigned char *zzlFind(unsigned char *zl, sds ele, double *score) {
    /* 获取压缩表第一个元素地址（第一个成员值地址） */
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;

    /* 正向遍历压缩表中元素 */
    while (eptr != NULL) {
        /* 获取分值所在地址 */
        sptr = ziplistNext(zl,eptr);
        serverAssert(sptr != NULL);

        /* 对比成员值是否相等 */
        if (ziplistCompare(eptr,(unsigned char*)ele,sdslen(ele))) {
            /* Matching element, pull out score. */
            /* 相等则取出分值 */
            if (score != NULL) *score = zzlGetScore(sptr);
            return eptr;
        }

        /* Move to next element. */
        /* 获取下一个成员值地址 */
        eptr = ziplistNext(zl,sptr);
    }
    return NULL;
}

/* Delete (element,score) pair from ziplist. Use local copy of eptr because we
 * don't want to modify the one given as argument. */
/* 从压缩表中删除指定位置的成员值元素和分值元素 */
unsigned char *zzlDelete(unsigned char *zl, unsigned char *eptr) {
    unsigned char *p = eptr;

    /* TODO: add function to ziplist API to delete N elements from offset. */
    zl = ziplistDelete(zl,&p);
    zl = ziplistDelete(zl,&p);
    return zl;
}

/* 向压缩表中指定位置添加指定的成员值和分值。
 * 如果 eptr 不为空，则将它们插入到压缩表中 eptr 指向的位置，否则追加到压缩表结尾。 */
unsigned char *zzlInsertAt(unsigned char *zl, unsigned char *eptr, sds ele, double score) {
    unsigned char *sptr;
    char scorebuf[128];
    int scorelen;
    size_t offset;

    /* 将分值转换为字符串 */
    scorelen = d2string(scorebuf,sizeof(scorebuf),score);
    if (eptr == NULL) {
        /* 依次将成员值和分值追加到压缩表尾部 */
        zl = ziplistPush(zl,(unsigned char*)ele,sdslen(ele),ZIPLIST_TAIL);
        zl = ziplistPush(zl,(unsigned char*)scorebuf,scorelen,ZIPLIST_TAIL);
    } else {
        /* Keep offset relative to zl, as it might be re-allocated. */
        /* 保存 eptr 指针的偏移位置，考虑到压缩表可能发生重新分配。 */
        offset = eptr-zl;
        /* 将成员值插入到压缩表中 */
        zl = ziplistInsert(zl,eptr,(unsigned char*)ele,sdslen(ele));
        /* 重新获取 eptr 地址 */
        eptr = zl+offset;

        /* Insert score after the element. */
        /* 将分值插入到刚刚添加的成员值之后 */
        serverAssert((sptr = ziplistNext(zl,eptr)) != NULL);
        zl = ziplistInsert(zl,sptr,(unsigned char*)scorebuf,scorelen);
    }
    return zl;
}

/* Insert (element,score) pair in ziplist. This function assumes the element is
 * not yet present in the list. */
/* 向压缩表中添加指定的成员值和分值 */
unsigned char *zzlInsert(unsigned char *zl, sds ele, double score) {
    /* 获取压缩表第一个元素地址（第一个成员值地址） */
    unsigned char *eptr = ziplistIndex(zl,0), *sptr;
    double s;

    /* 正向遍历压缩表中元素 */
    while (eptr != NULL) {
        /* 获取分值所在地址，并取出分值。 */
        sptr = ziplistNext(zl,eptr);
        serverAssert(sptr != NULL);
        s = zzlGetScore(sptr);

        if (s > score) {
            /* First element with score larger than score for element to be
             * inserted. This means we should take its spot in the list to
             * maintain ordering. */
            /* 在第一个分值大于参数指定分值的元素之前插入 */
            zl = zzlInsertAt(zl,eptr,ele,score);
            break;
        } else if (s == score) {
            /* Ensure lexicographical ordering for elements. */
            /* 如果分值相等，需要确保按照成员值进行排序。 */
            if (zzlCompareElements(eptr,(unsigned char*)ele,sdslen(ele)) > 0) {
                zl = zzlInsertAt(zl,eptr,ele,score);
                break;
            }
        }

        /* Move to next element. */
        /* 获取下一个成员值地址 */
        eptr = ziplistNext(zl,sptr);
    }

    /* Push on tail of list when it was not yet inserted. */
    /* 如果到这里仍没有插入，则将其追加到压缩表结尾。 */
    if (eptr == NULL)
        zl = zzlInsertAt(zl,NULL,ele,score);
    return zl;
}

/* 从压缩表表中删除分值在指定范围内的节点（前后都是闭区间）。
 * 如果参数 '*deleted' 不为空，则将删除的节点数量存储其中。 */
unsigned char *zzlDeleteRangeByScore(unsigned char *zl, zrangespec *range, unsigned long *deleted) {
    unsigned char *eptr, *sptr;
    double score;
    unsigned long num = 0;

    if (deleted != NULL) *deleted = 0;

    /* 查询指定范围内第一个满足的成员值地址 */
    eptr = zzlFirstInRange(zl,range);
    if (eptr == NULL) return zl;

    /* When the tail of the ziplist is deleted, eptr will point to the sentinel
     * byte and ziplistNext will return NULL. */
    /* 当 eptr 指向压缩表结尾时，ziplistNext() 函数返回 NULL。 */
    while ((sptr = ziplistNext(zl,eptr)) != NULL) {
        score = zzlGetScore(sptr);
        if (zslValueLteMax(score,range)) {
            /* Delete both the element and the score. */
            /* 删除压缩表中保存成员值和分值的元素 */
            zl = ziplistDelete(zl,&eptr);
            zl = ziplistDelete(zl,&eptr);
            num++;
        } else {
            /* No longer in range. */
            break;
        }
    }

    if (deleted != NULL) *deleted = num;
    return zl;
}

/* 从压缩表表中删除成员值在指定范围内的节点（前后都是闭区间）。
 * 如果参数 '*deleted' 不为空，则将删除的节点数量存储其中。 */
unsigned char *zzlDeleteRangeByLex(unsigned char *zl, zlexrangespec *range, unsigned long *deleted) {
    unsigned char *eptr, *sptr;
    unsigned long num = 0;

    if (deleted != NULL) *deleted = 0;

    /* 查询指定范围内第一个满足的成员值地址 */
    eptr = zzlFirstInLexRange(zl,range);
    if (eptr == NULL) return zl;

    /* When the tail of the ziplist is deleted, eptr will point to the sentinel
     * byte and ziplistNext will return NULL. */
    /* 当 eptr 指向压缩表结尾时，ziplistNext() 函数返回 NULL。 */
    while ((sptr = ziplistNext(zl,eptr)) != NULL) {
        if (zzlLexValueLteMax(eptr,range)) {
            /* Delete both the element and the score. */
            /* 删除压缩表中保存成员值和分值的元素 */
            zl = ziplistDelete(zl,&eptr);
            zl = ziplistDelete(zl,&eptr);
            num++;
        } else {
            /* No longer in range. */
            break;
        }
    }

    if (deleted != NULL) *deleted = num;
    return zl;
}

/* Delete all the elements with rank between start and end from the skiplist.
 * Start and end are inclusive. Note that start and end need to be 1-based */
/* 删除压缩表中所有指定排行范围内的节点（前后闭区间）。
 * 开始和结束位置索引都是基于 1 开始。 */
unsigned char *zzlDeleteRangeByRank(unsigned char *zl, unsigned int start, unsigned int end, unsigned long *deleted) {
    unsigned int num = (end-start)+1;
    if (deleted) *deleted = num;
    /* 每个节点由成员值和分值两个元素组成 */
    zl = ziplistDeleteRange(zl,2*(start-1),2*num);
    return zl;
}

/*-----------------------------------------------------------------------------
 * Common sorted set API
 *----------------------------------------------------------------------------*/

/* 返回有序集合长度（节点数量） */
unsigned long zsetLength(const robj *zobj) {
    unsigned long length = 0;
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        /* 压缩表 */
        length = zzlLength(zobj->ptr);
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        /* 跳表 */
        length = ((const zset*)zobj->ptr)->zsl->length;
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return length;
}

/* 转换有序集合为指定的编码方式 */
void zsetConvert(robj *zobj, int encoding) {
    zset *zs;
    zskiplistNode *node, *next;
    sds ele;
    double score;

    if (zobj->encoding == encoding) return;
    /* 编码方式由压缩表转换为跳表 */
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        if (encoding != OBJ_ENCODING_SKIPLIST)
            serverPanic("Unknown target encoding");

        /* 创建有序集合结构 */
        zs = zmalloc(sizeof(*zs));
        /* 字典 */
        zs->dict = dictCreate(&zsetDictType,NULL);
        /* 跳表 */
        zs->zsl = zslCreate();

        /* 获取压缩表首个成员值和分值地址 */
        eptr = ziplistIndex(zl,0);
        serverAssertWithInfo(NULL,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);
        serverAssertWithInfo(NULL,zobj,sptr != NULL);

        /* 遍历压缩表中全部的节点，并依次添加到新的有序集合中。 */
        while (eptr != NULL) {
            /* 获取分值和成员值 */
            score = zzlGetScore(sptr);
            serverAssertWithInfo(NULL,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));
            if (vstr == NULL)
                ele = sdsfromlonglong(vlong);
            else
                ele = sdsnewlen((char*)vstr,vlen);

            /* 将成员值和分值分别保存到跳表和字典中 */
            node = zslInsert(zs->zsl,score,ele);
            serverAssert(dictAdd(zs->dict,ele,&node->score) == DICT_OK);
            /* 移动指针，指向压缩表中下一个节点。 */
            zzlNext(zl,&eptr,&sptr);
        }

        zfree(zobj->ptr);
        zobj->ptr = zs;
        zobj->encoding = OBJ_ENCODING_SKIPLIST;

    /* 编码方式由跳表转换为压缩表 */
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        /* 创建一个压缩表 */
        unsigned char *zl = ziplistNew();

        if (encoding != OBJ_ENCODING_ZIPLIST)
            serverPanic("Unknown target encoding");

        /* Approach similar to zslFree(), since we want to free the skiplist at
         * the same time as creating the ziplist. */
        zs = zobj->ptr;
        /* 提前释放字典，因为可以通过遍历跳表来完成数据转移。 */
        dictRelease(zs->dict);
        /* 获取跳表中首个节点地址 */
        node = zs->zsl->header->level[0].forward;
        zfree(zs->zsl->header);
        zfree(zs->zsl);

        /* 遍历跳表中全部节点，并将其添加到压缩表中。 */
        while (node) {
            /* 将成员值和分值一次追加到压缩表结尾 */
            zl = zzlInsertAt(zl,NULL,node->ele,node->score);
            /* 获取下一个节点地址 */
            next = node->level[0].forward;
            zslFreeNode(node);
            node = next;
        }

        zfree(zs);
        zobj->ptr = zl;
        zobj->encoding = OBJ_ENCODING_ZIPLIST;
    } else {
        serverPanic("Unknown sorted set encoding");
    }
}

/* Convert the sorted set object into a ziplist if it is not already a ziplist
 * and if the number of elements and the maximum element size is within the
 * expected ranges. */
/* 当有序集合的编码方式是跳表，且保存的节点数和最大的元素大小在指定范围内时，
 * 将其编码方式转换为压缩表。 */
void zsetConvertToZiplistIfNeeded(robj *zobj, size_t maxelelen) {
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) return;
    zset *zset = zobj->ptr;

    if (zset->zsl->length <= server.zset_max_ziplist_entries &&
        maxelelen <= server.zset_max_ziplist_value)
            zsetConvert(zobj,OBJ_ENCODING_ZIPLIST);
}

/* Return (by reference) the score of the specified member of the sorted set
 * storing it into *score. If the element does not exist C_ERR is returned
 * otherwise C_OK is returned and *score is correctly populated.
 * If 'zobj' or 'member' is NULL, C_ERR is returned. */
/* 从有序集合中查询指定成员值对应的分值，并保存到 score 中。
 * 如果成员值存在，则返回 OK，否则返回 ERR。 */
int zsetScore(robj *zobj, sds member, double *score) {
    if (!zobj || !member) return C_ERR;

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        /* 从压缩表中查询指定成员值对应的分值 */
        if (zzlFind(zobj->ptr, member, score) == NULL) return C_ERR;
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        /* 从字典中查询指定成员值对应的节点 */
        dictEntry *de = dictFind(zs->dict, member);
        if (de == NULL) return C_ERR;
        /* 获取并保存分值 */
        *score = *(double*)dictGetVal(de);
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return C_OK;
}

/* Add a new element or update the score of an existing element in a sorted
 * set, regardless of its encoding.
 * 添加或更新有序集合中指定成员值的分值。
 *
 * The set of flags change the command behavior. They are passed with an integer
 * pointer since the function will clear the flags and populate them with
 * other flags to indicate different conditions.
 *
 * The input flags are the following:
 * 参数 flags 有如下含义：
 *
 * ZADD_INCR: Increment the current element score by 'score' instead of updating
 *            the current element score. If the element does not exist, we
 *            assume 0 as previous score.
 *            如果成员值存在，则将对应的分值加一，
 *            如果不存在，则假定之前的分值为 0。
 * ZADD_NX:   Perform the operation only if the element does not exist.
 *            成员值不存在时执行。
 * ZADD_XX:   Perform the operation only if the element already exist.
 *            成员存在时执行。
 *
 * When ZADD_INCR is used, the new score of the element is stored in
 * '*newscore' if 'newscore' is not NULL.
 * 当命令是 ZADD_INCR 时，且 'newscore' 不为空时，将会把新的分值保存其中。
 *
 * The returned flags are the following:
 *
 * ZADD_NAN:     The resulting score is not a number.
 * ZADD_ADDED:   The element was added (not present before the call).
 * ZADD_UPDATED: The element score was updated.
 * ZADD_NOP:     No operation was performed because of NX or XX.
 *
 * Return value:
 * 返回值：
 *
 * 成功返回 1，失败返回 0。
 *
 * The function returns 1 on success, and sets the appropriate flags
 * ADDED or UPDATED to signal what happened during the operation (note that
 * none could be set if we re-added an element using the same score it used
 * to have, or in the case a zero increment is used).
 *
 * The function returns 0 on error, currently only when the increment
 * produces a NAN condition, or when the 'score' value is NAN since the
 * start.
 *
 * The command as a side effect of adding a new element may convert the sorted
 * set internal encoding from ziplist to hashtable+skiplist.
 *
 * Memory management of 'ele':
 *
 * The function does not take ownership of the 'ele' SDS string, but copies
 * it if needed. */
int zsetAdd(robj *zobj, double score, sds ele, int *flags, double *newscore) {
    /* Turn options into simple to check vars. */
    /* 将参数 flags 转换为单个检查变量 */
    int incr = (*flags & ZADD_INCR) != 0;
    int nx = (*flags & ZADD_NX) != 0;
    int xx = (*flags & ZADD_XX) != 0;
    *flags = 0; /* We'll return our response flags. */
    double curscore;

    /* NaN as input is an error regardless of all the other parameters. */
    /* 检查分值是否是数字 */
    if (isnan(score)) {
        *flags = ZADD_NAN;
        return 0;
    }

    /* Update the sorted set according to its encoding. */
    /* 根据不同编码方式更新有序集合中存储的值 */
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *eptr;

        /* 指定成员值存在 */
        if ((eptr = zzlFind(zobj->ptr,ele,&curscore)) != NULL) {
            /* NX? Return, same element already exists. */
            /* 如果 NX 为 true，直接返回。 */
            if (nx) {
                *flags |= ZADD_NOP;
                return 1;
            }

            /* Prepare the score for the increment if needed. */
            /* 将分值加一 */
            if (incr) {
                score += curscore;
                if (isnan(score)) {
                    *flags |= ZADD_NAN;
                    return 0;
                }
                if (newscore) *newscore = score;
            }

            /* Remove and re-insert when score changed. */
            /* 更新节点（删除原节点，再新增节点） */
            if (score != curscore) {
                zobj->ptr = zzlDelete(zobj->ptr,eptr);
                zobj->ptr = zzlInsert(zobj->ptr,ele,score);
                *flags |= ZADD_UPDATED;
            }
            return 1;
        } else if (!xx) {
            /* Optimize: check if the element is too large or the list
             * becomes too long *before* executing zzlInsert. */
            /* 如果指定节点不存在，且 XX 为 false，则新增节点。 */
            zobj->ptr = zzlInsert(zobj->ptr,ele,score);
            if (zzlLength(zobj->ptr) > server.zset_max_ziplist_entries ||
                sdslen(ele) > server.zset_max_ziplist_value)
                zsetConvert(zobj,OBJ_ENCODING_SKIPLIST);
            if (newscore) *newscore = score;
            *flags |= ZADD_ADDED;
            return 1;
        } else {
            *flags |= ZADD_NOP;
            return 1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplistNode *znode;
        dictEntry *de;

        /* 从字典中查询指定成员值对应的节点 */
        de = dictFind(zs->dict,ele);
        /* 节点存在 */
        if (de != NULL) {
            /* NX? Return, same element already exists. */
            /* 如果 NX 为 true，直接返回。 */
            if (nx) {
                *flags |= ZADD_NOP;
                return 1;
            }
            /* 获取分值 */
            curscore = *(double*)dictGetVal(de);

            /* Prepare the score for the increment if needed. */
            /* 将分值加一 */
            if (incr) {
                score += curscore;
                if (isnan(score)) {
                    *flags |= ZADD_NAN;
                    return 0;
                }
                if (newscore) *newscore = score;
            }

            /* Remove and re-insert when score changes. */
            if (score != curscore) {
                /* 更新跳表中指定成员值的分值 */
                znode = zslUpdateScore(zs->zsl,curscore,ele,score);
                /* Note that we did not removed the original element from
                 * the hash table representing the sorted set, so we just
                 * update the score. */
                /* 更新字典中的分值 */
                dictGetVal(de) = &znode->score; /* Update score ptr. */
                *flags |= ZADD_UPDATED;
            }
            return 1;
        } else if (!xx) {
            /* 如果指定节点不存在，且 XX 为 false，则新增节点。 */
            ele = sdsdup(ele);
            znode = zslInsert(zs->zsl,score,ele);
            serverAssert(dictAdd(zs->dict,ele,&znode->score) == DICT_OK);
            *flags |= ZADD_ADDED;
            if (newscore) *newscore = score;
            return 1;
        } else {
            *flags |= ZADD_NOP;
            return 1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return 0; /* Never reached. */
}

/* Delete the element 'ele' from the sorted set, returning 1 if the element
 * existed and was deleted, 0 otherwise (the element was not there). */
/* 删除有序集合中指定成员值对应的节点。
 * 如果节点存在，并被删除，返回 1，否则返回 0。 */
int zsetDel(robj *zobj, sds ele) {
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *eptr;

        /* 获取压缩表中指定成员值所在的地址 */
        if ((eptr = zzlFind(zobj->ptr,ele,NULL)) != NULL) {
            /* 删除压缩表指定位置的节点（成员值和分值） */
            zobj->ptr = zzlDelete(zobj->ptr,eptr);
            return 1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        dictEntry *de;
        double score;

        /* 删除字典中指定成员值的节点 */
        de = dictUnlink(zs->dict,ele);
        if (de != NULL) {
            /* Get the score in order to delete from the skiplist later. */
            /* 获取分值，为了之后从跳表中删除节点。 */
            score = *(double*)dictGetVal(de);

            /* Delete from the hash table and later from the skiplist.
             * Note that the order is important: deleting from the skiplist
             * actually releases the SDS string representing the element,
             * which is shared between the skiplist and the hash table, so
             * we need to delete from the skiplist as the final step. */
            /* 删除的顺序必须是先从字典中删除，然后才能从跳表中删除。
             * 因为从跳表中删除时，同时也会释放保存成员值的 sds 字符串，
             * 该对象是跳表和字典共同使用的，所以如果提前释放，之后将无法从
             * 字典中将对应节点删除。 */

            /* 释放字典的节点 */
            dictFreeUnlinkedEntry(zs->dict,de);

            /* Delete from skiplist. */
            /* 从跳表中删除 */
            int retval = zslDelete(zs->zsl,score,ele,NULL);
            serverAssert(retval);

            if (htNeedsResize(zs->dict)) dictResize(zs->dict);
            return 1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
    return 0; /* No such element found. */
}

/* Given a sorted set object returns the 0-based rank of the object or
 * -1 if the object does not exist.
 *
 * For rank we mean the position of the element in the sorted collection
 * of elements. So the first element has rank 0, the second rank 1, and so
 * forth up to length-1 elements.
 *
 * If 'reverse' is false, the rank is returned considering as first element
 * the one with the lowest score. Otherwise if 'reverse' is non-zero
 * the rank is computed considering as element with rank 0 the one with
 * the highest score. */
/* 返回指定成员值在有序集合的排行（基于 0 开始），如果不存在，返回 -1。
 *
 * reverse 表示获取排行的方向，等于 false 时，表示返回正向的排行，
 * 否则，表示返回逆向的排行。 */
long zsetRank(robj *zobj, sds ele, int reverse) {
    unsigned long llen;
    unsigned long rank;

    /* 集合长度 */
    llen = zsetLength(zobj);

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        /* 获取压缩表首个成员值和分值的地址 */
        eptr = ziplistIndex(zl,0);
        serverAssert(eptr != NULL);
        sptr = ziplistNext(zl,eptr);
        serverAssert(sptr != NULL);

        /* 遍历整个压缩表节点，直到匹配到等于指定成员值的元素。 */
        rank = 1;
        while(eptr != NULL) {
            if (ziplistCompare(eptr,(unsigned char*)ele,sdslen(ele)))
                break;
            rank++;
            zzlNext(zl,&eptr,&sptr);
        }

        /* 返回排行 */
        if (eptr != NULL) {
            if (reverse)
                return llen-rank;
            else
                return rank-1;
        } else {
            return -1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        dictEntry *de;
        double score;

        /* 查询字典中指定成员值对应的节点 */
        de = dictFind(zs->dict,ele);
        if (de != NULL) {
            /* 获取成员值对应的分值 */
            score = *(double*)dictGetVal(de);
            /* 获取指定成员值和分值在跳表中的排行 */
            rank = zslGetRank(zsl,score,ele);
            /* Existing elements always have a rank. */
            serverAssert(rank != 0);
            /* 返回排行 */
            if (reverse)
                return llen-rank;
            else
                return rank-1;
        } else {
            return -1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
}

/*-----------------------------------------------------------------------------
 * Sorted set commands
 *----------------------------------------------------------------------------*/

/* This generic command implements both ZADD and ZINCRBY. */
/* ZADD 和 ZINCRBY 命令通用执行函数 */
void zaddGenericCommand(client *c, int flags) {
    static char *nanerr = "resulting score is not a number (NaN)";
    robj *key = c->argv[1];
    robj *zobj;
    sds ele;
    double score = 0, *scores = NULL;
    int j, elements;
    int scoreidx = 0;
    /* The following vars are used in order to track what the command actually
     * did during the execution, to reply to the client and to trigger the
     * notification of keyspace change. */
    /* 以下的变量用于表示具体要执行的命令 */
    int added = 0;      /* Number of new elements added. */
    int updated = 0;    /* Number of elements with updated score. */
    int processed = 0;  /* Number of elements processed, may remain zero with
                           options like XX. */

    /* Parse options. At the end 'scoreidx' is set to the argument position
     * of the score of the first score-element pair. */
    /* 解析选项参数，保存到 flags 变量中。 */
    scoreidx = 2;
    while(scoreidx < c->argc) {
        char *opt = c->argv[scoreidx]->ptr;
        if (!strcasecmp(opt,"nx")) flags |= ZADD_NX;
        else if (!strcasecmp(opt,"xx")) flags |= ZADD_XX;
        else if (!strcasecmp(opt,"ch")) flags |= ZADD_CH;
        else if (!strcasecmp(opt,"incr")) flags |= ZADD_INCR;
        else break;
        scoreidx++;
    }

    /* Turn options into simple to check vars. */
    /* 将选项值转换为用于检测的简单变量 */
    int incr = (flags & ZADD_INCR) != 0;
    int nx = (flags & ZADD_NX) != 0;
    int xx = (flags & ZADD_XX) != 0;
    int ch = (flags & ZADD_CH) != 0;

    /* After the options, we expect to have an even number of args, since
     * we expect any number of score-element pairs. */
    /* 确认选项之后的参数（score-element），是否成对出现。 */
    elements = c->argc-scoreidx;
    if (elements % 2 || !elements) {
        addReply(c,shared.syntaxerr);
        return;
    }
    elements /= 2; /* Now this holds the number of score-element pairs. */

    /* Check for incompatible options. */
    /* 检查是否存在冲突选项 */
    if (nx && xx) {
        addReplyError(c,
            "XX and NX options at the same time are not compatible");
        return;
    }

    /* INCR 命令只支持单个节点中分值的增加 */
    if (incr && elements > 1) {
        addReplyError(c,
            "INCR option supports a single increment-element pair");
        return;
    }

    /* Start parsing all the scores, we need to emit any syntax error
     * before executing additions to the sorted set, as the command should
     * either execute fully or nothing at all. */
    /* 解析全部的分值，确保没有异常的参数值。 */
    scores = zmalloc(sizeof(double)*elements);
    for (j = 0; j < elements; j++) {
        if (getDoubleFromObjectOrReply(c,c->argv[scoreidx+j*2],&scores[j],NULL)
            != C_OK) goto cleanup;
    }

    /* Lookup the key and create the sorted set if does not exist. */
    /* 获取有序集合对象 */
    zobj = lookupKeyWrite(c->db,key);
    /* 有序集合不存在 */
    if (zobj == NULL) {
        /* 如果 XX 为 true，直接返回。 */
        if (xx) goto reply_to_client; /* No key + XX option: nothing to do. */
        /* 如果有序集合中最大压缩表长度设置为 0，或第一个成员值长度大于有序集合压缩表允许最大值，
         * 则创建编码方式为跳表的有序集合，否则创建压缩表形式的有序集合。 */
        if (server.zset_max_ziplist_entries == 0 ||
            server.zset_max_ziplist_value < sdslen(c->argv[scoreidx+1]->ptr))
        {
            zobj = createZsetObject();
        } else {
            zobj = createZsetZiplistObject();
        }
        /* 保存入库 */
        dbAdd(c->db,key,zobj);
    } else {
        /* 类型检查 */
        if (zobj->type != OBJ_ZSET) {
            addReply(c,shared.wrongtypeerr);
            goto cleanup;
        }
    }

    /* 依次将参数中的 score-element 保存到有序集合中 */
    for (j = 0; j < elements; j++) {
        double newscore;
        score = scores[j];
        int retflags = flags;

        ele = c->argv[scoreidx+1+j*2]->ptr;
        int retval = zsetAdd(zobj, score, ele, &retflags, &newscore);
        if (retval == 0) {
            addReplyError(c,nanerr);
            goto cleanup;
        }
        if (retflags & ZADD_ADDED) added++;
        if (retflags & ZADD_UPDATED) updated++;
        if (!(retflags & ZADD_NOP)) processed++;
        score = newscore;
    }
    server.dirty += (added+updated);

/* 返回给客户端 */
reply_to_client:
    if (incr) { /* ZINCRBY or INCR option. */
        if (processed)
            addReplyDouble(c,score);
        else
            addReplyNull(c);
    } else { /* ZADD. */
        addReplyLongLong(c,ch ? added+updated : added);
    }

/* 清理 */
cleanup:
    zfree(scores);
    if (added || updated) {
        signalModifiedKey(c,c->db,key);
        notifyKeyspaceEvent(NOTIFY_ZSET,
            incr ? "zincr" : "zadd", key, c->db->id);
    }
}

/* ZADD 命令执行函数 */
void zaddCommand(client *c) {
    zaddGenericCommand(c,ZADD_NONE);
}

/* ZINCRBY 命令执行函数 */
void zincrbyCommand(client *c) {
    zaddGenericCommand(c,ZADD_INCR);
}

/* ZREM 命令执行函数 */
void zremCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    int deleted = 0, keyremoved = 0, j;

    /* 获取出有序集合对象，并进行类型检查。 */
    if ((zobj = lookupKeyWriteOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) return;

    /* 遍历输入的成员值参数 */
    for (j = 2; j < c->argc; j++) {
        /* 删除指定成员值对应的节点 */
        if (zsetDel(zobj,c->argv[j]->ptr)) deleted++;
        /* 如果有序集合为空，将其从库中移除。 */
        if (zsetLength(zobj) == 0) {
            dbDelete(c->db,key);
            keyremoved = 1;
            break;
        }
    }

    if (deleted) {
        notifyKeyspaceEvent(NOTIFY_ZSET,"zrem",key,c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key,c->db->id);
        signalModifiedKey(c,c->db,key);
        server.dirty += deleted;
    }
    /* 返回成功删除的节点数量给客户端 */
    addReplyLongLong(c,deleted);
}

/* Implements ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX commands. */
/* 实现 ZREMRANGEBYRANK, ZREMRANGEBYSCORE, ZREMRANGEBYLEX 命令 */
#define ZRANGE_RANK 0
#define ZRANGE_SCORE 1
#define ZRANGE_LEX 2
void zremrangeGenericCommand(client *c, int rangetype) {
    robj *key = c->argv[1];
    robj *zobj;
    int keyremoved = 0;
    unsigned long deleted = 0;
    zrangespec range;
    zlexrangespec lexrange;
    long start, end, llen;

    /* Step 1: Parse the range. */
    /* 第一步：解析范围。 */
    if (rangetype == ZRANGE_RANK) {
        /* 解析开始和结束排行 */
        if ((getLongFromObjectOrReply(c,c->argv[2],&start,NULL) != C_OK) ||
            (getLongFromObjectOrReply(c,c->argv[3],&end,NULL) != C_OK))
            return;
    } else if (rangetype == ZRANGE_SCORE) {
        /* 解析分值范围 */
        if (zslParseRange(c->argv[2],c->argv[3],&range) != C_OK) {
            addReplyError(c,"min or max is not a float");
            return;
        }
    } else if (rangetype == ZRANGE_LEX) {
        /* 解析成员值范围 */
        if (zslParseLexRange(c->argv[2],c->argv[3],&lexrange) != C_OK) {
            addReplyError(c,"min or max not valid string range item");
            return;
        }
    }

    /* Step 2: Lookup & range sanity checks if needed. */
    /* 第二步：获取出有序集合对象，有需要的话，对排行范围进行检查。 */
    if ((zobj = lookupKeyWriteOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) goto cleanup;

    if (rangetype == ZRANGE_RANK) {
        /* Sanitize indexes. */
        /* 处理开始和结束索引值 */
        llen = zsetLength(zobj);
        if (start < 0) start = llen+start;
        if (end < 0) end = llen+end;
        if (start < 0) start = 0;

        /* Invariant: start >= 0, so this test will be true when end < 0.
         * The range is empty when start > end or start >= length. */
        /* 如果排行范围异常，返回空集合给客户端。 */
        if (start > end || start >= llen) {
            addReply(c,shared.czero);
            goto cleanup;
        }
        if (end >= llen) end = llen-1;
    }

    /* Step 3: Perform the range deletion operation. */
    /* 第三步：执行范围删除操作。 */
    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        switch(rangetype) {
        case ZRANGE_RANK:
            zobj->ptr = zzlDeleteRangeByRank(zobj->ptr,start+1,end+1,&deleted);
            break;
        case ZRANGE_SCORE:
            zobj->ptr = zzlDeleteRangeByScore(zobj->ptr,&range,&deleted);
            break;
        case ZRANGE_LEX:
            zobj->ptr = zzlDeleteRangeByLex(zobj->ptr,&lexrange,&deleted);
            break;
        }
        if (zzlLength(zobj->ptr) == 0) {
            dbDelete(c->db,key);
            keyremoved = 1;
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        switch(rangetype) {
        case ZRANGE_RANK:
            deleted = zslDeleteRangeByRank(zs->zsl,start+1,end+1,zs->dict);
            break;
        case ZRANGE_SCORE:
            deleted = zslDeleteRangeByScore(zs->zsl,&range,zs->dict);
            break;
        case ZRANGE_LEX:
            deleted = zslDeleteRangeByLex(zs->zsl,&lexrange,zs->dict);
            break;
        }
        if (htNeedsResize(zs->dict)) dictResize(zs->dict);
        if (dictSize(zs->dict) == 0) {
            dbDelete(c->db,key);
            keyremoved = 1;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    /* Step 4: Notifications and reply. */
    /* 第四步：通知并回复客户端。 */
    if (deleted) {
        char *event[3] = {"zremrangebyrank","zremrangebyscore","zremrangebylex"};
        signalModifiedKey(c,c->db,key);
        notifyKeyspaceEvent(NOTIFY_ZSET,event[rangetype],key,c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key,c->db->id);
    }
    server.dirty += deleted;
    addReplyLongLong(c,deleted);

cleanup:
    if (rangetype == ZRANGE_LEX) zslFreeLexRange(&lexrange);
}

/* ZREMRANGEBYRANK 命令执行函数 */
void zremrangebyrankCommand(client *c) {
    zremrangeGenericCommand(c,ZRANGE_RANK);
}

/* ZREMRANGEBYSCORE 命令执行函数 */
void zremrangebyscoreCommand(client *c) {
    zremrangeGenericCommand(c,ZRANGE_SCORE);
}

/* ZREMRANGEBYLEX 命令执行函数 */
void zremrangebylexCommand(client *c) {
    zremrangeGenericCommand(c,ZRANGE_LEX);
}

/* 支持集合和有序集合的迭代器 */
typedef struct {
    robj *subject;
    /* 集合类型（集合或有序集合） */
    int type; /* Set, sorted set */
    int encoding;
    double weight;

    union {
        /* Set iterators. */
        /* 集合迭代器 */
        union _iterset {
            /* 整型集合 */
            struct {
                intset *is;
                int ii;
            } is;
            /* 字典 */
            struct {
                dict *dict;
                dictIterator *di;
                dictEntry *de;
            } ht;
        } set;

        /* Sorted set iterators. */
        /* 有序集合迭代器 */
        union _iterzset {
            /* 压缩表 */
            struct {
                unsigned char *zl;
                unsigned char *eptr, *sptr;
            } zl;
            /* 字典+跳表 */
            struct {
                zset *zs;
                zskiplistNode *node;
            } sl;
        } zset;
    } iter;
} zsetopsrc;


/* Use dirty flags for pointers that need to be cleaned up in the next
 * iteration over the zsetopval. The dirty flag for the long long value is
 * special, since long long values don't need cleanup. Instead, it means that
 * we already checked that "ell" holds a long long, or tried to convert another
 * representation into a long long value. When this was successful,
 * OPVAL_VALID_LL is set as well. */
/* DIRTY 标识迭代器在下次迭代前要进行清理，当 DIRTY 常量作用于 long long 值时，
 * 该值不需要被清理。因为它表示 'ell' 已经持有一个 long long 值，或者已经将一个对象
 * 转换为 long long 值了，当这些被处理好之后，就会被标识为 OPVAL_VALID_LL。 */
#define OPVAL_DIRTY_SDS 1
#define OPVAL_DIRTY_LL 2
#define OPVAL_VALID_LL 4

/* Store value retrieved from the iterator. */
/* 保存从迭代器中获取值的结构体 */
typedef struct {
    int flags;
    unsigned char _buf[32]; /* Private buffer. */
    sds ele;
    unsigned char *estr;
    unsigned int elen;
    long long ell;
    double score;
} zsetopval;

typedef union _iterset iterset;
typedef union _iterzset iterzset;

/* 初始化迭代器 */
void zuiInitIterator(zsetopsrc *op) {
    if (op->subject == NULL)
        return;

    if (op->type == OBJ_SET) {
        /* 初始化集合迭代器 */
        iterset *it = &op->iter.set;
        if (op->encoding == OBJ_ENCODING_INTSET) {
            it->is.is = op->subject->ptr;
            it->is.ii = 0;
        } else if (op->encoding == OBJ_ENCODING_HT) {
            it->ht.dict = op->subject->ptr;
            it->ht.di = dictGetIterator(op->subject->ptr);
            it->ht.de = dictNext(it->ht.di);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        /* 初始化有序集合迭代器 */
        iterzset *it = &op->iter.zset;
        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            it->zl.zl = op->subject->ptr;
            it->zl.eptr = ziplistIndex(it->zl.zl,0);
            if (it->zl.eptr != NULL) {
                it->zl.sptr = ziplistNext(it->zl.zl,it->zl.eptr);
                serverAssert(it->zl.sptr != NULL);
            }
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            it->sl.zs = op->subject->ptr;
            it->sl.node = it->sl.zs->zsl->header->level[0].forward;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

/* 清空迭代器 */
void zuiClearIterator(zsetopsrc *op) {
    if (op->subject == NULL)
        return;

    if (op->type == OBJ_SET) {
        iterset *it = &op->iter.set;
        if (op->encoding == OBJ_ENCODING_INTSET) {
            UNUSED(it); /* skip */
        } else if (op->encoding == OBJ_ENCODING_HT) {
            dictReleaseIterator(it->ht.di);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        iterzset *it = &op->iter.zset;
        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            UNUSED(it); /* skip */
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            UNUSED(it); /* skip */
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

/* 获取迭代集合的长度 */
unsigned long zuiLength(zsetopsrc *op) {
    if (op->subject == NULL)
        return 0;

    if (op->type == OBJ_SET) {
        if (op->encoding == OBJ_ENCODING_INTSET) {
            return intsetLen(op->subject->ptr);
        } else if (op->encoding == OBJ_ENCODING_HT) {
            dict *ht = op->subject->ptr;
            return dictSize(ht);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            return zzlLength(op->subject->ptr);
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = op->subject->ptr;
            return zs->zsl->length;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

/* Check if the current value is valid. If so, store it in the passed structure
 * and move to the next element. If not valid, this means we have reached the
 * end of the structure and can abort. */
/* 检查迭代器当前指向的元素是否可用，如果是的话，将它保存到参数 val 中，
 * 然后将迭代器的当前指针指向下一节点，如果不可用的话，说明已经到达集合的结尾，可用停止了。 */
int zuiNext(zsetopsrc *op, zsetopval *val) {
    if (op->subject == NULL)
        return 0;

    /* 对上次的对象进行清理 */
    if (val->flags & OPVAL_DIRTY_SDS)
        sdsfree(val->ele);

    memset(val,0,sizeof(zsetopval));

    if (op->type == OBJ_SET) {
        iterset *it = &op->iter.set;
        /* 整型集合 */
        if (op->encoding == OBJ_ENCODING_INTSET) {
            int64_t ell;

            /* 获取成员值 */
            if (!intsetGet(it->is.is,it->is.ii,&ell))
                return 0;
            val->ell = ell;
            /* 分值默认为 1.0 */
            val->score = 1.0;

            /* Move to next element. */
            it->is.ii++;
        
        /* 字典 */
        } else if (op->encoding == OBJ_ENCODING_HT) {
            if (it->ht.de == NULL)
                return 0;
            /* 获取成员值 */
            val->ele = dictGetKey(it->ht.de);
            /* 分值默认为 1.0 */
            val->score = 1.0;

            /* Move to next element. */
            it->ht.de = dictNext(it->ht.di);
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        iterzset *it = &op->iter.zset;
        /* 压缩表 */
        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            /* No need to check both, but better be explicit. */
            if (it->zl.eptr == NULL || it->zl.sptr == NULL)
                return 0;
            /* 获取成员值和分值 */
            serverAssert(ziplistGet(it->zl.eptr,&val->estr,&val->elen,&val->ell));
            val->score = zzlGetScore(it->zl.sptr);

            /* Move to next element. */
            zzlNext(it->zl.zl,&it->zl.eptr,&it->zl.sptr);

        /* 跳表 */
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            if (it->sl.node == NULL)
                return 0;
            /* 获取成员值和分值 */
            val->ele = it->sl.node->ele;
            val->score = it->sl.node->score;

            /* Move to next element. */
            it->sl.node = it->sl.node->level[0].forward;
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
    return 1;
}

/* 将 val 中元素转换为 long long 值 */
int zuiLongLongFromValue(zsetopval *val) {
    if (!(val->flags & OPVAL_DIRTY_LL)) {
        val->flags |= OPVAL_DIRTY_LL;

        if (val->ele != NULL) {
            if (string2ll(val->ele,sdslen(val->ele),&val->ell))
                val->flags |= OPVAL_VALID_LL;
        } else if (val->estr != NULL) {
            if (string2ll((char*)val->estr,val->elen,&val->ell))
                val->flags |= OPVAL_VALID_LL;
        } else {
            /* The long long was already set, flag as valid. */
            val->flags |= OPVAL_VALID_LL;
        }
    }
    return val->flags & OPVAL_VALID_LL;
}

sds zuiSdsFromValue(zsetopval *val) {
    if (val->ele == NULL) {
        if (val->estr != NULL) {
            val->ele = sdsnewlen((char*)val->estr,val->elen);
        } else {
            val->ele = sdsfromlonglong(val->ell);
        }
        val->flags |= OPVAL_DIRTY_SDS;
    }
    return val->ele;
}

/* This is different from zuiSdsFromValue since returns a new SDS string
 * which is up to the caller to free. */
/* 根据 val 内变量值，返回一个新的 SDS 字符串。 */
sds zuiNewSdsFromValue(zsetopval *val) {
    if (val->flags & OPVAL_DIRTY_SDS) {
        /* We have already one to return! */
        /* 成员值刚好要被清理，可以直接返回。 */
        sds ele = val->ele;
        val->flags &= ~OPVAL_DIRTY_SDS;
        val->ele = NULL;
        return ele;
    } else if (val->ele) {
        return sdsdup(val->ele);
    } else if (val->estr) {
        return sdsnewlen((char*)val->estr,val->elen);
    } else {
        return sdsfromlonglong(val->ell);
    }
}

/* 设置 val 中 elen 和 estr 值 */
int zuiBufferFromValue(zsetopval *val) {
    if (val->estr == NULL) {
        if (val->ele != NULL) {
            val->elen = sdslen(val->ele);
            val->estr = (unsigned char*)val->ele;
        } else {
            val->elen = ll2string((char*)val->_buf,sizeof(val->_buf),val->ell);
            val->estr = val->_buf;
        }
    }
    return 1;
}

/* Find value pointed to by val in the source pointer to by op. When found,
 * return 1 and store its score in target. Return 0 otherwise. */
/* 在迭代器指定的集合对象中查找指定成员值，如果能够找到，将对应的分值保存在 'score' 中。
 * 找到返回 1，否则返回 0。 */
int zuiFind(zsetopsrc *op, zsetopval *val, double *score) {
    if (op->subject == NULL)
        return 0;

    if (op->type == OBJ_SET) {
        /* 整型集合 */
        if (op->encoding == OBJ_ENCODING_INTSET) {
            if (zuiLongLongFromValue(val) &&
                intsetFind(op->subject->ptr,val->ell))
            {
                *score = 1.0;
                return 1;
            } else {
                return 0;
            }

        /* 字典 */
        } else if (op->encoding == OBJ_ENCODING_HT) {
            dict *ht = op->subject->ptr;
            zuiSdsFromValue(val);
            if (dictFind(ht,val->ele) != NULL) {
                *score = 1.0;
                return 1;
            } else {
                return 0;
            }
        } else {
            serverPanic("Unknown set encoding");
        }
    } else if (op->type == OBJ_ZSET) {
        zuiSdsFromValue(val);

        /* 压缩表 */
        if (op->encoding == OBJ_ENCODING_ZIPLIST) {
            if (zzlFind(op->subject->ptr,val->ele,score) != NULL) {
                /* Score is already set by zzlFind. */
                return 1;
            } else {
                return 0;
            }

        /* 跳表 */
        } else if (op->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = op->subject->ptr;
            dictEntry *de;
            if ((de = dictFind(zs->dict,val->ele)) != NULL) {
                *score = *(double*)dictGetVal(de);
                return 1;
            } else {
                return 0;
            }
        } else {
            serverPanic("Unknown sorted set encoding");
        }
    } else {
        serverPanic("Unsupported type");
    }
}

/* 对比两个集合的长度 */
int zuiCompareByCardinality(const void *s1, const void *s2) {
    unsigned long first = zuiLength((zsetopsrc*)s1);
    unsigned long second = zuiLength((zsetopsrc*)s2);
    if (first > second) return 1;
    if (first < second) return -1;
    return 0;
}

#define REDIS_AGGR_SUM 1
#define REDIS_AGGR_MIN 2
#define REDIS_AGGR_MAX 3
#define zunionInterDictValue(_e) (dictGetVal(_e) == NULL ? 1.0 : *(double*)dictGetVal(_e))

/* 根据聚合类型，设置新的分值的值。 */
inline static void zunionInterAggregate(double *target, double val, int aggregate) {
    if (aggregate == REDIS_AGGR_SUM) {
        *target = *target + val;
        /* The result of adding two doubles is NaN when one variable
         * is +inf and the other is -inf. When these numbers are added,
         * we maintain the convention of the result being 0.0. */
        if (isnan(*target)) *target = 0.0;
    } else if (aggregate == REDIS_AGGR_MIN) {
        *target = val < *target ? val : *target;
    } else if (aggregate == REDIS_AGGR_MAX) {
        *target = val > *target ? val : *target;
    } else {
        /* safety net */
        serverPanic("Unknown ZUNION/INTER aggregate type");
    }
}

uint64_t dictSdsHash(const void *key);
int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2);

dictType setAccumulatorDictType = {
    dictSdsHash,               /* hash function */
    NULL,                      /* key dup */
    NULL,                      /* val dup */
    dictSdsKeyCompare,         /* key compare */
    NULL,                      /* key destructor */
    NULL                       /* val destructor */
};

/* ZUNIONSTORE 和 ZINTERSTORE 命令通用执行函数 */
void zunionInterGenericCommand(client *c, robj *dstkey, int op) {
    int i, j;
    long setnum;
    int aggregate = REDIS_AGGR_SUM;
    zsetopsrc *src;
    zsetopval zval;
    sds tmp;
    size_t maxelelen = 0;
    robj *dstobj;
    zset *dstzset;
    zskiplistNode *znode;
    int touched = 0;

    /* expect setnum input keys to be given */
    /* 获取参数中有序集合数量 */
    if ((getLongFromObjectOrReply(c, c->argv[2], &setnum, NULL) != C_OK))
        return;

    if (setnum < 1) {
        addReplyError(c,
            "at least 1 input key is needed for ZUNIONSTORE/ZINTERSTORE");
        return;
    }

    /* test if the expected number of keys would overflow */
    /* 判断参数中是否存在足够数量的参数 */
    if (setnum > c->argc-3) {
        addReply(c,shared.syntaxerr);
        return;
    }

    /* read keys to be used for input */
    /* 获取全部的集合对象，并封装到集合迭代器中。 */
    src = zcalloc(sizeof(zsetopsrc) * setnum);
    for (i = 0, j = 3; i < setnum; i++, j++) {
        /* 查询指定 key 对应的集合对象 */
        robj *obj = lookupKeyWrite(c->db,c->argv[j]);
        if (obj != NULL) {
            /* 类型检查 */
            if (obj->type != OBJ_ZSET && obj->type != OBJ_SET) {
                zfree(src);
                addReply(c,shared.wrongtypeerr);
                return;
            }

            src[i].subject = obj;
            src[i].type = obj->type;
            src[i].encoding = obj->encoding;
        } else {
            src[i].subject = NULL;
        }

        /* Default all weights to 1. */
        /* 默认权重为 1。 */
        src[i].weight = 1.0;
    }

    /* parse optional extra arguments */
    /* 解析额外的选项参数 */
    if (j < c->argc) {
        int remaining = c->argc - j;

        while (remaining) {
            /* 权重配置 */
            if (remaining >= (setnum + 1) &&
                !strcasecmp(c->argv[j]->ptr,"weights"))
            {
                j++; remaining--;
                /* 依次将参数中的权重值设置到对应集合迭代器中 */
                for (i = 0; i < setnum; i++, j++, remaining--) {
                    if (getDoubleFromObjectOrReply(c,c->argv[j],&src[i].weight,
                            "weight value is not a float") != C_OK)
                    {
                        zfree(src);
                        return;
                    }
                }

            /* 聚合配置 */
            } else if (remaining >= 2 &&
                       !strcasecmp(c->argv[j]->ptr,"aggregate"))
            {
                j++; remaining--;
                /* 获取聚合类型 */
                if (!strcasecmp(c->argv[j]->ptr,"sum")) {
                    aggregate = REDIS_AGGR_SUM;
                } else if (!strcasecmp(c->argv[j]->ptr,"min")) {
                    aggregate = REDIS_AGGR_MIN;
                } else if (!strcasecmp(c->argv[j]->ptr,"max")) {
                    aggregate = REDIS_AGGR_MAX;
                } else {
                    zfree(src);
                    addReply(c,shared.syntaxerr);
                    return;
                }
                j++; remaining--;

            /* 语法异常 */
            } else {
                zfree(src);
                addReply(c,shared.syntaxerr);
                return;
            }
        }
    }

    /* sort sets from the smallest to largest, this will improve our
     * algorithm's performance */
    /* 按集合长度从小到大排序集合迭代器数组，有助于提高之后算法的效率。 */
    qsort(src,setnum,sizeof(zsetopsrc),zuiCompareByCardinality);

    /* 创建目标有序集合 */
    dstobj = createZsetObject();
    dstzset = dstobj->ptr;
    memset(&zval, 0, sizeof(zval));

    /* 如果是 ZINTERSTORE 命令 */
    if (op == SET_OP_INTER) {
        /* Skip everything if the smallest input is empty. */
        /* 如果最小的集合为空，则不做任何处理。 */
        if (zuiLength(&src[0]) > 0) {
            /* Precondition: as src[0] is non-empty and the inputs are ordered
             * by size, all src[i > 0] are non-empty too. */
            /* 初始化最小集合的迭代器 */
            zuiInitIterator(&src[0]);
            while (zuiNext(&src[0],&zval)) {
                double score, value;

                /* 新的分值 */
                score = src[0].weight * zval.score;
                if (isnan(score)) score = 0;

                /* 遍历之后的全部集合，检查是否包含最小集合中的指定成员值，
                 * 如果不包含，则跳出循环。 */
                for (j = 1; j < setnum; j++) {
                    /* It is not safe to access the zset we are
                     * iterating, so explicitly check for equal object. */
                    if (src[j].subject == src[0].subject) {
                        value = zval.score*src[j].weight;
                        zunionInterAggregate(&score,value,aggregate);
                    } else if (zuiFind(&src[j],&zval,&value)) {
                        value *= src[j].weight;
                        zunionInterAggregate(&score,value,aggregate);
                    } else {
                        break;
                    }
                }

                /* Only continue when present in every input. */
                /* 如果成员值在每个集合中都存在，则将其保存目标集合中。 */
                if (j == setnum) {
                    tmp = zuiNewSdsFromValue(&zval);
                    znode = zslInsert(dstzset->zsl,score,tmp);
                    dictAdd(dstzset->dict,tmp,&znode->score);
                    if (sdslen(tmp) > maxelelen) maxelelen = sdslen(tmp);
                }
            }
            zuiClearIterator(&src[0]);
        }

    /* 如果是 ZUNIONSTORE 命令 */
    } else if (op == SET_OP_UNION) {
        /* 用于计算分值（成员值 -> 聚合分值） */
        dict *accumulator = dictCreate(&setAccumulatorDictType,NULL);
        dictIterator *di;
        dictEntry *de, *existing;
        double score;

        if (setnum) {
            /* Our union is at least as large as the largest set.
             * Resize the dictionary ASAP to avoid useless rehashing. */
            /* 扩展字典到指定大小 */
            dictExpand(accumulator,zuiLength(&src[setnum-1]));
        }

        /* Step 1: Create a dictionary of elements -> aggregated-scores
         * by iterating one sorted set after the other. */
        /* 遍历全部的集合 */
        for (i = 0; i < setnum; i++) {
            /* 跳过空集合 */
            if (zuiLength(&src[i]) == 0) continue;

            /* 初始化集合迭代器 */
            zuiInitIterator(&src[i]);
            while (zuiNext(&src[i],&zval)) {
                /* Initialize value */
                /* 获取新的分值 */
                score = src[i].weight * zval.score;
                if (isnan(score)) score = 0;

                /* Search for this element in the accumulating dictionary. */
                /* 将当前成员值保存到临时分值字典中，如果不存在，函数会返回新建的节点，
                 * 否则，会将 'existing' 指向已存在的节点。 */
                de = dictAddRaw(accumulator,zuiSdsFromValue(&zval),&existing);
                /* If we don't have it, we need to create a new entry. */
                /* 如果不存在，需要把分值保存 */
                if (!existing) {
                    tmp = zuiNewSdsFromValue(&zval);
                    /* Remember the longest single element encountered,
                     * to understand if it's possible to convert to ziplist
                     * at the end. */
                    /* 记录最长的成员值长度，以便最后确认是否需要将编码方式转换为压缩表。 */
                     if (sdslen(tmp) > maxelelen) maxelelen = sdslen(tmp);
                    /* Update the element with its initial score. */
                    /* 设置分值（感觉 dictSetKey() 不需要再设置一次） */
                    dictSetKey(accumulator, de, tmp);
                    dictSetDoubleVal(de,score);
                } else {
                    /* Update the score with the score of the new instance
                     * of the element found in the current sorted set.
                     *
                     * Here we access directly the dictEntry double
                     * value inside the union as it is a big speedup
                     * compared to using the getDouble/setDouble API. */
                    /* 根据聚合类型，更新已存在的成员值对应的分值。 */
                    zunionInterAggregate(&existing->v.d,score,aggregate);
                }
            }
            zuiClearIterator(&src[i]);
        }

        /* Step 2: convert the dictionary into the final sorted set. */
        /* 创建临时字典的迭代器 */
        di = dictGetIterator(accumulator);

        /* We now are aware of the final size of the resulting sorted set,
         * let's resize the dictionary embedded inside the sorted set to the
         * right size, in order to save rehashing time. */
        /* 现在已经能够知道有序集合的最终大小，可以提前将其中的字典调整到该大小，
         * 以节省之后操作导致的 rehash 时间。 */
        dictExpand(dstzset->dict,dictSize(accumulator));

        /* 将临时字典中的节点全部保存到结果有序集合中 */
        while((de = dictNext(di)) != NULL) {
            sds ele = dictGetKey(de);
            score = dictGetDoubleVal(de);
            znode = zslInsert(dstzset->zsl,score,ele);
            dictAdd(dstzset->dict,ele,&znode->score);
        }
        dictReleaseIterator(di);
        dictRelease(accumulator);
    } else {
        serverPanic("Unknown operator");
    }

    /* 删除可能存在的目标 key */
    if (dbDelete(c->db,dstkey))
        touched = 1;
    /* 如果结果有序集合不为空 */
    if (dstzset->zsl->length) {
        /* 根据情况决定是否将结果有序集合的编码方式转为压缩表 */
        zsetConvertToZiplistIfNeeded(dstobj,maxelelen);
        /* 保存入库 */
        dbAdd(c->db,dstkey,dstobj);
        /* 返回结果有序集合的长度给客户端 */
        addReplyLongLong(c,zsetLength(dstobj));
        signalModifiedKey(c,c->db,dstkey);
        notifyKeyspaceEvent(NOTIFY_ZSET,
            (op == SET_OP_UNION) ? "zunionstore" : "zinterstore",
            dstkey,c->db->id);
        server.dirty++;

    /* 如果结果有序集合为空 */
    } else {
        decrRefCount(dstobj);
        /* 返回 0 给客户端 */
        addReply(c,shared.czero);
        if (touched) {
            signalModifiedKey(c,c->db,dstkey);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",dstkey,c->db->id);
            server.dirty++;
        }
    }
    zfree(src);
}

/* ZUNIONSTORE 命令执行函数 */
void zunionstoreCommand(client *c) {
    zunionInterGenericCommand(c,c->argv[1], SET_OP_UNION);
}

/* ZINTERSTORE 命令执行函数 */
void zinterstoreCommand(client *c) {
    zunionInterGenericCommand(c,c->argv[1], SET_OP_INTER);
}

/* ZRANGE 命令通用执行函数 */
void zrangeGenericCommand(client *c, int reverse) {
    robj *key = c->argv[1];
    robj *zobj;
    int withscores = 0;
    long start;
    long end;
    long llen;
    long rangelen;

    /* 获取参数中范围的开始和结束位置值 */
    if ((getLongFromObjectOrReply(c, c->argv[2], &start, NULL) != C_OK) ||
        (getLongFromObjectOrReply(c, c->argv[3], &end, NULL) != C_OK)) return;

    /* 设置是否需要一并返回分值的标识 */
    if (c->argc == 5 && !strcasecmp(c->argv[4]->ptr,"withscores")) {
        withscores = 1;
    } else if (c->argc >= 5) {
        addReply(c,shared.syntaxerr);
        return;
    }

    /* 获取出有序集合对象，并进行类型检查。 */
    if ((zobj = lookupKeyReadOrReply(c,key,shared.emptyarray)) == NULL
         || checkType(c,zobj,OBJ_ZSET)) return;

    /* Sanitize indexes. */
    /* 处理开始和结束索引值 */
    llen = zsetLength(zobj);
    if (start < 0) start = llen+start;
    if (end < 0) end = llen+end;
    if (start < 0) start = 0;

    /* Invariant: start >= 0, so this test will be true when end < 0.
     * The range is empty when start > end or start >= length. */
    /* 如果排行范围异常，返回空集合给客户端。 */
    if (start > end || start >= llen) {
        addReply(c,shared.emptyarray);
        return;
    }
    if (end >= llen) end = llen-1;
    rangelen = (end-start)+1;

    /* Return the result in form of a multi-bulk reply. RESP3 clients
     * will receive sub arrays with score->element, while RESP2 returned
     * a flat array. */
    /* 返回将要返回的数组大小，如果要一并返回分值，
     * 则返回数组大小要翻倍。 */
    if (withscores && c->resp == 2)
        addReplyArrayLen(c, rangelen*2);
    else
        addReplyArrayLen(c, rangelen);

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        /* 根据逆序标识，获取第一个或最后一个成员值地址。 */
        if (reverse)
            eptr = ziplistIndex(zl,-2-(2*start));
        else
            eptr = ziplistIndex(zl,2*start);

        /* 获取成员值对应分值的地址 */
        serverAssertWithInfo(c,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);

        while (rangelen--) {
            serverAssertWithInfo(c,zobj,eptr != NULL && sptr != NULL);
            /* 获取成员值 */
            serverAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));

            /* 如果请求要求一并返回分值，且 RESP 协议版本大于 2 的话，
             * 返回长度 2 给客户端（说明之后有成员值和分值两个数据）。 */
            if (withscores && c->resp > 2) addReplyArrayLen(c,2);
            /* 返回成员值给客户端 */
            if (vstr == NULL)
                addReplyBulkLongLong(c,vlong);
            else
                addReplyBulkCBuffer(c,vstr,vlen);
            /* 根据请求决定是否返回分值给客户端 */
            if (withscores) addReplyDouble(c,zzlGetScore(sptr));

            /* 根据逆序标识，获取下一个或上一个成员值和分值的地址。 */
            if (reverse)
                zzlPrev(zl,&eptr,&sptr);
            else
                zzlNext(zl,&eptr,&sptr);
        }

    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;
        sds ele;

        /* Check if starting point is trivial, before doing log(N) lookup. */
        /* 根据逆序标识，获取第一个或最后一个跳表节点的地址。 */
        if (reverse) {
            ln = zsl->tail;
            if (start > 0)
                ln = zslGetElementByRank(zsl,llen-start);
        } else {
            ln = zsl->header->level[0].forward;
            if (start > 0)
                ln = zslGetElementByRank(zsl,start+1);
        }

        while(rangelen--) {
            serverAssertWithInfo(c,zobj,ln != NULL);
            ele = ln->ele;
            /* 返回成员值及必要的分值给客户端 */
            if (withscores && c->resp > 2) addReplyArrayLen(c,2);
            addReplyBulkCBuffer(c,ele,sdslen(ele));
            if (withscores) addReplyDouble(c,ln->score);
            /* 获取上一个或下一个跳表节点 */
            ln = reverse ? ln->backward : ln->level[0].forward;
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }
}

/* ZRANGE 命令执行函数 */
void zrangeCommand(client *c) {
    zrangeGenericCommand(c,0);
}

/* ZREVRANGE 命令执行函数 */
void zrevrangeCommand(client *c) {
    zrangeGenericCommand(c,1);
}

/* This command implements ZRANGEBYSCORE, ZREVRANGEBYSCORE. */
/* ZRANGEBYSCORE 和 ZREVRANGEBYSCORE 命令通用执行函数 */
void genericZrangebyscoreCommand(client *c, int reverse) {
    zrangespec range;
    robj *key = c->argv[1];
    robj *zobj;
    long offset = 0, limit = -1;
    int withscores = 0;
    unsigned long rangelen = 0;
    void *replylen = NULL;
    int minidx, maxidx;

    /* Parse the range arguments. */
    /* 获取设定范围开始和结束的参数在全部参数集中的位置 */
    if (reverse) {
        /* Range is given as [max,min] */
        maxidx = 2; minidx = 3;
    } else {
        /* Range is given as [min,max] */
        minidx = 2; maxidx = 3;
    }

    /* 解析参数值，生成对应的分值范围。 */
    if (zslParseRange(c->argv[minidx],c->argv[maxidx],&range) != C_OK) {
        addReplyError(c,"min or max is not a float");
        return;
    }

    /* Parse optional extra arguments. Note that ZCOUNT will exactly have
     * 4 arguments, so we'll never enter the following code path. */
    /* 分析读入可选的参数 */
    if (c->argc > 4) {
        int remaining = c->argc - 4;
        int pos = 4;

        while (remaining) {
            /* 解析 withscores 参数 */
            if (remaining >= 1 && !strcasecmp(c->argv[pos]->ptr,"withscores")) {
                pos++; remaining--;
                withscores = 1;

            /* 解析 limit 参数 */
            } else if (remaining >= 3 && !strcasecmp(c->argv[pos]->ptr,"limit")) {
                if ((getLongFromObjectOrReply(c, c->argv[pos+1], &offset, NULL)
                        != C_OK) ||
                    (getLongFromObjectOrReply(c, c->argv[pos+2], &limit, NULL)
                        != C_OK))
                {
                    return;
                }
                pos += 3; remaining -= 3;
            } else {
                addReply(c,shared.syntaxerr);
                return;
            }
        }
    }

    /* Ok, lookup the key and get the range */
    /* 获取出有序集合对象，并进行类型检查。 */
    if ((zobj = lookupKeyReadOrReply(c,key,shared.emptyarray)) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) return;

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;
        double score;

        /* If reversed, get the last node in range as starting point. */
        /* 根据逆序标识，获取指定范围内第一个或最后一个成员值地址。 */
        if (reverse) {
            eptr = zzlLastInRange(zl,&range);
        } else {
            eptr = zzlFirstInRange(zl,&range);
        }

        /* No "first" element in the specified interval. */
        /* 如果范围内没有任何节点，返回空数组给客户端。 */
        if (eptr == NULL) {
            addReply(c,shared.emptyarray);
            return;
        }

        /* Get score pointer for the first element. */
        /* 获取第一个分值 */
        serverAssertWithInfo(c,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        /* 添加一个空对象到输出 BUF 中，并持有该对象，以便之后更新其值。 */
        replylen = addReplyDeferredLen(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        /* 跳过指定偏移量的节点 */
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }

        while (eptr && limit--) {
            /* 获取分值 */
            score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            /* 如果分值不在范围内，跳出循环。 */
            if (reverse) {
                if (!zslValueGteMin(score,&range)) break;
            } else {
                if (!zslValueLteMax(score,&range)) break;
            }

            /* We know the element exists, so ziplistGet should always
             * succeed */
            /* 获取成员值 */
            serverAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));

            rangelen++;
            if (withscores && c->resp > 2) addReplyArrayLen(c,2);
            /* 返回成员值及必要的分值给客户端 */
            if (vstr == NULL) {
                addReplyBulkLongLong(c,vlong);
            } else {
                addReplyBulkCBuffer(c,vstr,vlen);
            }
            if (withscores) addReplyDouble(c,score);

            /* Move to next node */
            /* 移动到下一个节点 */
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        /* 根据逆序标识，获取指定范围内第一个或最后一个成员值地址。 */
        if (reverse) {
            ln = zslLastInRange(zsl,&range);
        } else {
            ln = zslFirstInRange(zsl,&range);
        }

        /* No "first" element in the specified interval. */
        /* 如果范围内没有任何节点，返回空数组给客户端。 */
        if (ln == NULL) {
            addReply(c,shared.emptyarray);
            return;
        }

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        /* 添加一个空对象到输出 BUF 中，并持有该对象，以便之后更新其值。 */
        replylen = addReplyDeferredLen(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        /* 跳过指定偏移量的节点 */
        while (ln && offset--) {
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            /* 如果分值不在范围内，跳出循环。 */
            if (reverse) {
                if (!zslValueGteMin(ln->score,&range)) break;
            } else {
                if (!zslValueLteMax(ln->score,&range)) break;
            }

            rangelen++;
            if (withscores && c->resp > 2) addReplyArrayLen(c,2);
            /* 返回成员值及必要的分值给客户端 */
            addReplyBulkCBuffer(c,ln->ele,sdslen(ln->ele));
            if (withscores) addReplyDouble(c,ln->score);

            /* Move to next node */
            /* 移动到下一个节点 */
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    /* 更新返回给客户端的最终数组长度 */
    if (withscores && c->resp == 2) rangelen *= 2;
    setDeferredArrayLen(c, replylen, rangelen);
}

/* ZRANGEBYSCORE 命令执行函数 */
void zrangebyscoreCommand(client *c) {
    genericZrangebyscoreCommand(c,0);
}

/* ZREVRANGEBYSCORE 命令执行函数 */
void zrevrangebyscoreCommand(client *c) {
    genericZrangebyscoreCommand(c,1);
}

/* ZCOUNT 命令执行函数 */
void zcountCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zrangespec range;
    unsigned long count = 0;

    /* Parse the range arguments */
    /* 解析参数值，生成对应的分值范围。 */
    if (zslParseRange(c->argv[2],c->argv[3],&range) != C_OK) {
        addReplyError(c,"min or max is not a float");
        return;
    }

    /* Lookup the sorted set */
    /* 获取出有序集合对象，并进行类型检查。 */
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, OBJ_ZSET)) return;

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        double score;

        /* Use the first element in range as the starting point */
        /* 获取指定范围内第一个成员值地址 */
        eptr = zzlFirstInRange(zl,&range);

        /* No "first" element */
        /* 如果范围内没有任何节点，返回 0 给客户端。 */
        if (eptr == NULL) {
            addReply(c, shared.czero);
            return;
        }

        /* First element is in range */
        /* 获取第一个分值 */
        sptr = ziplistNext(zl,eptr);
        score = zzlGetScore(sptr);
        serverAssertWithInfo(c,zobj,zslValueLteMax(score,&range));

        /* Iterate over elements in range */
        /* 遍历范围内的节点 */
        while (eptr) {
            /* 获取分值 */
            score = zzlGetScore(sptr);

            /* Abort when the node is no longer in range. */
            /* 如果分值不在范围内，跳出循环。 */
            if (!zslValueLteMax(score,&range)) {
                break;
            } else {
                /* 增加计数器值，然后指向下一个节点。 */
                count++;
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        /* 获取指定范围内第一个跳表节点 */
        zn = zslFirstInRange(zsl, &range);

        /* Use rank of first element, if any, to determine preliminary count */
        /* 如果范围内存在节点 */
        if (zn != NULL) {
            /* 获取第一个节点的排行 */
            rank = zslGetRank(zsl, zn->score, zn->ele);
            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            /* 获取指定范围内最后一个跳表节点 */
            zn = zslLastInRange(zsl, &range);

            /* Use rank of last element, if any, to determine the actual count */
            /* 这里感觉不需要判断，依旧表示范围内存在节点。 */
            if (zn != NULL) {
                /* 获取最后一个节点的排行 */
                rank = zslGetRank(zsl, zn->score, zn->ele);
                /* 计算出最终范围内节点数量 */
                count -= (zsl->length - rank);
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    /* 返回范围内节点数量给客户端 */
    addReplyLongLong(c, count);
}

/* ZLEXCOUNT 命令执行函数 */
void zlexcountCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    zlexrangespec range;
    unsigned long count = 0;

    /* Parse the range arguments */
    /* 解析参数值，生成对应的成员值范围。 */
    if (zslParseLexRange(c->argv[2],c->argv[3],&range) != C_OK) {
        addReplyError(c,"min or max not valid string range item");
        return;
    }

    /* Lookup the sorted set */
    /* 获取出有序集合对象，并进行类型检查。 */
    if ((zobj = lookupKeyReadOrReply(c, key, shared.czero)) == NULL ||
        checkType(c, zobj, OBJ_ZSET))
    {
        zslFreeLexRange(&range);
        return;
    }

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;

        /* Use the first element in range as the starting point */
        /* 获取指定范围内第一个成员值地址 */
        eptr = zzlFirstInLexRange(zl,&range);

        /* No "first" element */
        /* 如果范围内没有任何节点，返回 0 给客户端。 */
        if (eptr == NULL) {
            zslFreeLexRange(&range);
            addReply(c, shared.czero);
            return;
        }

        /* First element is in range */
        /* 获取第一个分值 */
        sptr = ziplistNext(zl,eptr);
        serverAssertWithInfo(c,zobj,zzlLexValueLteMax(eptr,&range));

        /* Iterate over elements in range */
        /* 遍历范围内的节点 */
        while (eptr) {
            /* Abort when the node is no longer in range. */
            /* 如果分值不在范围内，跳出循环。 */
            if (!zzlLexValueLteMax(eptr,&range)) {
                break;
            } else {
                /* 增加计数器值，然后指向下一个节点。 */
                count++;
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *zn;
        unsigned long rank;

        /* Find first element in range */
        /* 获取指定范围内第一个跳表节点 */
        zn = zslFirstInLexRange(zsl, &range);

        /* Use rank of first element, if any, to determine preliminary count */
        /* 如果范围内存在节点 */
        if (zn != NULL) {
            /* 获取第一个节点的排行 */
            rank = zslGetRank(zsl, zn->score, zn->ele);
            count = (zsl->length - (rank - 1));

            /* Find last element in range */
            /* 获取指定范围内最后一个跳表节点 */
            zn = zslLastInLexRange(zsl, &range);

            /* Use rank of last element, if any, to determine the actual count */
            if (zn != NULL) {
                /* 获取最后一个节点的排行 */
                rank = zslGetRank(zsl, zn->score, zn->ele);
                /* 计算出最终范围内节点数量 */
                count -= (zsl->length - rank);
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    /* 释放迭代器，并返回范围内节点数量给客户端。 */
    zslFreeLexRange(&range);
    addReplyLongLong(c, count);
}

/* This command implements ZRANGEBYLEX, ZREVRANGEBYLEX. */
/* ZRANGEBYLEX 和 ZREVRANGEBYLEX 命令通用执行函数 */
void genericZrangebylexCommand(client *c, int reverse) {
    zlexrangespec range;
    robj *key = c->argv[1];
    robj *zobj;
    long offset = 0, limit = -1;
    unsigned long rangelen = 0;
    void *replylen = NULL;
    int minidx, maxidx;

    /* Parse the range arguments. */
    /* 获取设定范围开始和结束的参数在全部参数集中的位置 */
    if (reverse) {
        /* Range is given as [max,min] */
        maxidx = 2; minidx = 3;
    } else {
        /* Range is given as [min,max] */
        minidx = 2; maxidx = 3;
    }

    /* 解析参数值，生成对应的分值范围。 */
    if (zslParseLexRange(c->argv[minidx],c->argv[maxidx],&range) != C_OK) {
        addReplyError(c,"min or max not valid string range item");
        return;
    }

    /* Parse optional extra arguments. Note that ZCOUNT will exactly have
     * 4 arguments, so we'll never enter the following code path. */
    /* 分析读入可选的参数 */
    if (c->argc > 4) {
        int remaining = c->argc - 4;
        int pos = 4;

        while (remaining) {
            /* 解析 limit 参数 */
            if (remaining >= 3 && !strcasecmp(c->argv[pos]->ptr,"limit")) {
                if ((getLongFromObjectOrReply(c, c->argv[pos+1], &offset, NULL) != C_OK) ||
                    (getLongFromObjectOrReply(c, c->argv[pos+2], &limit, NULL) != C_OK)) {
                    zslFreeLexRange(&range);
                    return;
                }
                pos += 3; remaining -= 3;
            } else {
                zslFreeLexRange(&range);
                addReply(c,shared.syntaxerr);
                return;
            }
        }
    }

    /* Ok, lookup the key and get the range */
    /* 获取出有序集合对象，并进行类型检查。 */
    if ((zobj = lookupKeyReadOrReply(c,key,shared.emptyarray)) == NULL ||
        checkType(c,zobj,OBJ_ZSET))
    {
        zslFreeLexRange(&range);
        return;
    }

    if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = zobj->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vlong;

        /* If reversed, get the last node in range as starting point. */
        /* 根据逆序标识，获取指定范围内第一个或最后一个成员值地址。 */
        if (reverse) {
            eptr = zzlLastInLexRange(zl,&range);
        } else {
            eptr = zzlFirstInLexRange(zl,&range);
        }

        /* No "first" element in the specified interval. */
        /* 如果范围内没有任何节点，返回空数组给客户端。 */
        if (eptr == NULL) {
            addReply(c,shared.emptyarray);
            zslFreeLexRange(&range);
            return;
        }

        /* Get score pointer for the first element. */
        /* 获取第一个分值 */
        serverAssertWithInfo(c,zobj,eptr != NULL);
        sptr = ziplistNext(zl,eptr);

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        /* 添加一个空对象到输出 BUF 中，并持有该对象，以便之后更新其值。 */
        replylen = addReplyDeferredLen(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        /* 跳过指定偏移量的节点 */
        while (eptr && offset--) {
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }

        while (eptr && limit--) {
            /* Abort when the node is no longer in range. */
            /* 如果分值不在范围内，跳出循环。 */
            if (reverse) {
                if (!zzlLexValueGteMin(eptr,&range)) break;
            } else {
                if (!zzlLexValueLteMax(eptr,&range)) break;
            }

            /* We know the element exists, so ziplistGet should always
             * succeed. */
            /* 获取成员值 */
            serverAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));

            rangelen++;
            /* 返回成员值给客户端 */
            if (vstr == NULL) {
                addReplyBulkLongLong(c,vlong);
            } else {
                addReplyBulkCBuffer(c,vstr,vlen);
            }

            /* Move to next node */
            /* 移动到下一个节点 */
            if (reverse) {
                zzlPrev(zl,&eptr,&sptr);
            } else {
                zzlNext(zl,&eptr,&sptr);
            }
        }
    } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = zobj->ptr;
        zskiplist *zsl = zs->zsl;
        zskiplistNode *ln;

        /* If reversed, get the last node in range as starting point. */
        /* 根据逆序标识，获取指定范围内第一个或最后一个成员值地址。 */
        if (reverse) {
            ln = zslLastInLexRange(zsl,&range);
        } else {
            ln = zslFirstInLexRange(zsl,&range);
        }

        /* No "first" element in the specified interval. */
        /* 如果范围内没有任何节点，返回空数组给客户端。 */
        if (ln == NULL) {
            addReply(c,shared.emptyarray);
            zslFreeLexRange(&range);
            return;
        }

        /* We don't know in advance how many matching elements there are in the
         * list, so we push this object that will represent the multi-bulk
         * length in the output buffer, and will "fix" it later */
        /* 添加一个空对象到输出 BUF 中，并持有该对象，以便之后更新其值。 */
        replylen = addReplyDeferredLen(c);

        /* If there is an offset, just traverse the number of elements without
         * checking the score because that is done in the next loop. */
        /* 跳过指定偏移量的节点 */
        while (ln && offset--) {
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }

        while (ln && limit--) {
            /* Abort when the node is no longer in range. */
            /* 如果成员值不在范围内，跳出循环。 */
            if (reverse) {
                if (!zslLexValueGteMin(ln->ele,&range)) break;
            } else {
                if (!zslLexValueLteMax(ln->ele,&range)) break;
            }

            rangelen++;
            /* 返回成员值给客户端 */
            addReplyBulkCBuffer(c,ln->ele,sdslen(ln->ele));

            /* Move to next node */
            /* 移动到下一个节点 */
            if (reverse) {
                ln = ln->backward;
            } else {
                ln = ln->level[0].forward;
            }
        }
    } else {
        serverPanic("Unknown sorted set encoding");
    }

    /* 释放迭代器，并更新返回给客户端的最终数组长度 */
    zslFreeLexRange(&range);
    setDeferredArrayLen(c, replylen, rangelen);
}

/* ZRANGEBYLEX 命令执行函数 */
void zrangebylexCommand(client *c) {
    genericZrangebylexCommand(c,0);
}

/* ZREVRANGEBYLEX 命令执行函数 */
void zrevrangebylexCommand(client *c) {
    genericZrangebylexCommand(c,1);
}

/* ZCARD 命令执行函数 */
void zcardCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;

    if ((zobj = lookupKeyReadOrReply(c,key,shared.czero)) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) return;

    /* 返回有序集合长度给客户端 */
    addReplyLongLong(c,zsetLength(zobj));
}

/* ZSCORE 命令执行函数 */
void zscoreCommand(client *c) {
    robj *key = c->argv[1];
    robj *zobj;
    double score;

    if ((zobj = lookupKeyReadOrReply(c,key,shared.null[c->resp])) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) return;

    /* 返回指定成员值对应的分值给客户端 */
    if (zsetScore(zobj,c->argv[2]->ptr,&score) == C_ERR) {
        addReplyNull(c);
    } else {
        addReplyDouble(c,score);
    }
}

/* ZRANK 命令通用执行函数 */
void zrankGenericCommand(client *c, int reverse) {
    robj *key = c->argv[1];
    robj *ele = c->argv[2];
    robj *zobj;
    long rank;

    if ((zobj = lookupKeyReadOrReply(c,key,shared.null[c->resp])) == NULL ||
        checkType(c,zobj,OBJ_ZSET)) return;

    serverAssertWithInfo(c,ele,sdsEncodedObject(ele));
    /* 获取指定成员值和分值的节点的排行，并将其返回给客户端。 */
    rank = zsetRank(zobj,ele->ptr,reverse);
    if (rank >= 0) {
        addReplyLongLong(c,rank);
    } else {
        addReplyNull(c);
    }
}

/* ZRANK 命令执行函数 */
void zrankCommand(client *c) {
    zrankGenericCommand(c, 0);
}

/* ZREVRANK 命令执行函数 */
void zrevrankCommand(client *c) {
    zrankGenericCommand(c, 1);
}

/* ZSCAN 命令执行函数 */
void zscanCommand(client *c) {
    robj *o;
    unsigned long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == C_ERR) return;
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
        checkType(c,o,OBJ_ZSET)) return;
    scanGenericCommand(c,o,cursor);
}

/* This command implements the generic zpop operation, used by:
 * ZPOPMIN, ZPOPMAX, BZPOPMIN and BZPOPMAX. This function is also used
 * inside blocked.c in the unblocking stage of BZPOPMIN and BZPOPMAX.
 *
 * If 'emitkey' is true also the key name is emitted, useful for the blocking
 * behavior of BZPOP[MIN|MAX], since we can block into multiple keys.
 *
 * The synchronous version instead does not need to emit the key, but may
 * use the 'count' argument to return multiple items if available. */
/* 该函数实现了一般的集合弹出操作，
 * 被 ZPOPMIN, ZPOPMAX, BZPOPMIN 和 BZPOPMAX 命令函数调用。 */
void genericZpopCommand(client *c, robj **keyv, int keyc, int where, int emitkey, robj *countarg) {
    int idx;
    robj *key = NULL;
    robj *zobj = NULL;
    sds ele;
    double score;
    long count = 1;

    /* If a count argument as passed, parse it or return an error. */
    /* 获取 count 参数值 */
    if (countarg) {
        if (getLongFromObjectOrReply(c,countarg,&count,NULL) != C_OK)
            return;
        if (count <= 0) {
            addReply(c,shared.emptyarray);
            return;
        }
    }

    /* Check type and break on the first error, otherwise identify candidate. */
    /* 检查全部参数中 key 对应的集合 */
    idx = 0;
    while (idx < keyc) {
        key = keyv[idx++];
        zobj = lookupKeyWrite(c->db,key);
        if (!zobj) continue;
        if (checkType(c,zobj,OBJ_ZSET)) return;
        break;
    }

    /* No candidate for zpopping, return empty. */
    /* 如果不存在候选者，返回空数组给客户端。 */
    if (!zobj) {
        addReply(c,shared.emptyarray);
        return;
    }

    void *arraylen_ptr = addReplyDeferredLen(c);
    long arraylen = 0;

    /* We emit the key only for the blocking variant. */
    /* 在支持阻塞的情况下，返回 key 值给客户端。 */
    if (emitkey) addReplyBulk(c,key);

    /* Remove the element. */
    /* 删除节点 */
    do {
        if (zobj->encoding == OBJ_ENCODING_ZIPLIST) {
            unsigned char *zl = zobj->ptr;
            unsigned char *eptr, *sptr;
            unsigned char *vstr;
            unsigned int vlen;
            long long vlong;

            /* Get the first or last element in the sorted set. */
            /* 获取第一个或最后一个成员值地址 */
            eptr = ziplistIndex(zl,where == ZSET_MAX ? -2 : 0);
            serverAssertWithInfo(c,zobj,eptr != NULL);
            /* 获取成员值 */
            serverAssertWithInfo(c,zobj,ziplistGet(eptr,&vstr,&vlen,&vlong));
            if (vstr == NULL)
                ele = sdsfromlonglong(vlong);
            else
                ele = sdsnewlen(vstr,vlen);

            /* Get the score. */
            /* 获取对应的分值 */
            sptr = ziplistNext(zl,eptr);
            serverAssertWithInfo(c,zobj,sptr != NULL);
            score = zzlGetScore(sptr);
        } else if (zobj->encoding == OBJ_ENCODING_SKIPLIST) {
            zset *zs = zobj->ptr;
            zskiplist *zsl = zs->zsl;
            zskiplistNode *zln;

            /* Get the first or last element in the sorted set. */
            /* 获取第一个或最后一个跳表节点 */
            zln = (where == ZSET_MAX ? zsl->tail :
                                       zsl->header->level[0].forward);

            /* There must be an element in the sorted set. */
            serverAssertWithInfo(c,zobj,zln != NULL);
            /* 获取成员值和分值 */
            ele = sdsdup(zln->ele);
            score = zln->score;
        } else {
            serverPanic("Unknown sorted set encoding");
        }

        /* 删除指定成员值的节点 */
        serverAssertWithInfo(c,zobj,zsetDel(zobj,ele));
        server.dirty++;

        /* 第一次迭代时执行 */
        if (arraylen == 0) { /* Do this only for the first iteration. */
            char *events[2] = {"zpopmin","zpopmax"};
            notifyKeyspaceEvent(NOTIFY_ZSET,events[where],key,c->db->id);
            signalModifiedKey(c,c->db,key);
        }

        /* 返回成员值和分值给客户端 */
        addReplyBulkCBuffer(c,ele,sdslen(ele));
        addReplyDouble(c,score);
        sdsfree(ele);
        arraylen += 2;

        /* Remove the key, if indeed needed. */
        /* 如果集合为空，则将其从库中移除。 */
        if (zsetLength(zobj) == 0) {
            dbDelete(c->db,key);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",key,c->db->id);
            break;
        }
    } while(--count);

    setDeferredArrayLen(c,arraylen_ptr,arraylen + (emitkey != 0));
}

/* ZPOPMIN key [<count>] */
/* ZPOPMIN 命令执行函数 */
void zpopminCommand(client *c) {
    if (c->argc > 3) {
        addReply(c,shared.syntaxerr);
        return;
    }
    genericZpopCommand(c,&c->argv[1],1,ZSET_MIN,0,
        c->argc == 3 ? c->argv[2] : NULL);
}

/* ZMAXPOP key [<count>] */
/* ZPOPMAX 命令执行函数 */
void zpopmaxCommand(client *c) {
    if (c->argc > 3) {
        addReply(c,shared.syntaxerr);
        return;
    }
    genericZpopCommand(c,&c->argv[1],1,ZSET_MAX,0,
        c->argc == 3 ? c->argv[2] : NULL);
}

/* BZPOPMIN / BZPOPMAX actual implementation. */
/* BZPOPMIN 和 BZPOPMAX 命令通用执行函数 */
void blockingGenericZpopCommand(client *c, int where) {
    robj *o;
    mstime_t timeout;
    int j;

    /* 获取 timeout 参数值 */
    if (getTimeoutFromObjectOrReply(c,c->argv[c->argc-1],&timeout,UNIT_SECONDS)
        != C_OK) return;

    /* 遍历参数中多个集合的 key 值 */
    for (j = 1; j < c->argc-1; j++) {
        /* 获取有序集合对象 */
        o = lookupKeyWrite(c->db,c->argv[j]);
        if (o != NULL) {
            /* 类型检查 */
            if (o->type != OBJ_ZSET) {
                addReply(c,shared.wrongtypeerr);
                return;
            } else {
                if (zsetLength(o) != 0) {
                    /* Non empty zset, this is like a normal ZPOP[MIN|MAX]. */
                    /* 集合不为空，等同于 ZPOPMIN 或 ZPOPMAX 命令。 */
                    genericZpopCommand(c,&c->argv[j],1,where,1,NULL);
                    /* Replicate it as an ZPOP[MIN|MAX] instead of BZPOP[MIN|MAX]. */
                    /* 使用 ZPOP[MIN|MAX] 命令代替 BZPOP[MIN|MAX] 命令进行传播 */
                    rewriteClientCommandVector(c,2,
                        where == ZSET_MAX ? shared.zpopmax : shared.zpopmin,
                        c->argv[j]);
                    return;
                }
            }
        }
    }

    /* If we are inside a MULTI/EXEC and the zset is empty the only thing
     * we can do is treating it as a timeout (even with timeout 0). */
    /* 如果命令在一个事务中执行，且集合为空，只能将其当超时处理。 */
    if (c->flags & CLIENT_MULTI) {
        /* 返回一个空回复给客户端 */
        addReplyNullArray(c);
        return;
    }

    /* If the keys do not exist we must block */
    /* 阻塞 */
    blockForKeys(c,BLOCKED_ZSET,c->argv + 1,c->argc - 2,timeout,NULL,NULL);
}

// BZPOPMIN key [key ...] timeout
/* BZPOPMIN 命令执行函数 */
void bzpopminCommand(client *c) {
    blockingGenericZpopCommand(c,ZSET_MIN);
}

// BZPOPMAX key [key ...] timeout
/* BZPOPMAX 命令执行函数 */
void bzpopmaxCommand(client *c) {
    blockingGenericZpopCommand(c,ZSET_MAX);
}
