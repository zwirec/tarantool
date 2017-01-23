find_program(TCLSH tclsh)
if(NOT TCLSH)
    message(FATAL_ERROR "Failed to find tclsh")
endif()

find_package(TCL)
if(NOT TCL_FOUND)
    message(FATAL_ERROR "Failed to find tcl development files")
endif()

if(${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    set(SQLITE_DEBUG 1)
endif()

check_include_file(dlfcn.h HAVE_DLFCN_H)
check_include_file(malloc.h MALLOC_H)
check_include_file(memory.h MEMORY_H)
check_include_file(strings.h STRINGS_H)
check_function_exists(malloc_usable_size HAVE_MALLOC_USABLE_SIZE)
check_function_exists(strchrnul HAVE_STRCHRNUL)
check_function_exists(usleep HAVE_USLEEP)
check_function_exists(utime HAVE_UTIME)
check_function_exists(gmtime_r HAVE_GMTIME_R)
check_function_exists(localtime_r HAVE_LOCALTIME_R)
check_function_exists(localtime_s HAVE_LOCALTIME_S)

configure_file(config.h.cmake src/sqliteConfig.h)

add_definitions(-Wno-pointer-sign -Wno-error)

add_subdirectory(tool)
add_subdirectory(src)
