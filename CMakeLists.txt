cmake_minimum_required(VERSION 3.5)

project(miscutil)

find_package(PkgConfig REQUIRED)
pkg_search_module(GLIB REQUIRED glib-2.0 gobject-2.0 gio-2.0)

add_library(avllq SHARED avllq.c avllq.h)
target_link_libraries(avllq pthread)


###################
# test
###################
add_executable(test_avllq test_avllq.c)

target_include_directories(test_avllq PRIVATE ${GLIB_INCLUDE_DIRS})
target_link_libraries(test_avllq avllq ${GLIB_LDFLAGS} )
