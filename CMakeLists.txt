cmake_minimum_required(VERSION 3.5)

project(miscutil)

find_package(PkgConfig REQUIRED)
pkg_search_module(GLIB REQUIRED glib-2.0 gobject-2.0 gio-2.0)

add_library(
    miscutil
    SHARED
    avllq.c
    avllq.h
    fdzcq.c
    fdzcq.h
)

target_link_libraries(
    miscutil
    PUBLIC
    pthread
    rt
)

###################
# test
###################
add_executable(test_avllq test_avllq.c)
target_include_directories(test_avllq PRIVATE ${GLIB_INCLUDE_DIRS})
target_link_libraries(test_avllq miscutil ${GLIB_LDFLAGS})

add_executable(test_fdzcq test_fdzcq.c)
target_include_directories(test_fdzcq PRIVATE ${GLIB_INCLUDE_DIRS})
target_link_libraries(test_fdzcq miscutil ${GLIB_LDFLAGS})