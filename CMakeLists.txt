# 第一次启动时，需要手动指定 redis-server 位置
cmake_minimum_required(VERSION 3.4.0)
project(redis VERSION 6.0)

set(SRC_SERVER
        src/server.c
        src/acl.c
        src/adlist.c
        src/ae.c
        src/ae_kqueue.c
        src/anet.c
        src/aof.c
        src/bio.c
        src/bitops.c
        src/blocked.c
        src/childinfo.c
        src/cluster.c
        src/config.c
        src/connection.c
        src/crc16.c
        src/crc64.c
        src/db.c
        src/debug.c
        src/defrag.c
        src/dict.c
        src/endianconv.c
        src/evict.c
        src/expire.c
        src/geo.c
        src/geohash.c
        src/geohash_helper.c
        src/gopher.c
        src/hyperloglog.c
        src/intset.c
        src/latency.c
        src/lazyfree.c
        src/listpack.c
        src/localtime.c
        src/lolwut.c
        src/lolwut5.c
        src/lolwut6.c
        src/lzf_c.c
        src/lzf_d.c
        src/memtest.c
        src/module.c
        src/multi.c
        src/networking.c
        src/notify.c
        src/object.c
        src/pqsort.c
        src/pubsub.c
        src/quicklist.c
        src/rand.c
        src/rax.c
        src/rdb.c
        src/redis-check-aof.c
        src/redis-check-rdb.c
        src/release.c
        src/replication.c
        src/rio.c
        src/scripting.c
        src/sds.c
        src/sentinel.c
        src/setproctitle.c
        src/sha1.c
        src/sha256.c
        src/siphash.c
        src/slowlog.c
        src/sort.c
        src/sparkline.c
        src/syncio.c
        src/t_hash.c
        src/t_list.c
        src/t_set.c
        src/t_stream.c
        src/t_string.c
        src/t_zset.c
        src/tls.c
        src/tracking.c
        src/util.c
        src/ziplist.c
        src/zipmap.c
        src/zmalloc.c
)

add_executable(redis-server ${SRC_SERVER})
set(CMAKE_BUILD_TYPE "Debug")
set(CMAKE_CXX_STANDARD 14)
set(CMAKE_VERBOSE_MAKEFILE 1)
add_custom_target(makeRedisDebugCmd
        COMMAND make -j8
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})