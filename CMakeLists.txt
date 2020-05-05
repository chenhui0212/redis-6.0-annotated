# 第一次启动时，需要手动指定 redis-server 位置
cmake_minimum_required(VERSION 3.4.0)
project(redis VERSION "6.0")

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_VERBOSE_MAKEFILE 1)
add_custom_target(makeRedisDebugCmd
        COMMAND make -j8
        WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR})