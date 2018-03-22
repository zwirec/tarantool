##
## Manage LTO (Link-Time-Optimization) and IPO (Inter-Procedural-Optimization)
##

# Tarantool uses both dynamic-list and lto link options, which works only
# since binutils:
#  - 2.30 for linking with gold
#  - last 2.30 or 2.31 in case of ld (dbf)
#    (ld was fixed expecially for Tarantool,
#    https://gcc.gnu.org/bugzilla/show_bug.cgi?id=84901)

if (NOT DEFINED TARANTOOL_LTO)
    if (NOT CMAKE_BUILD_TYPE STREQUAL "Debug")
        set(TARANTOOL_LTO TRUE)
    else()
        set(TARANTOOL_LTO FALSE)
    endif()
endif()

if(NOT CMAKE_VERSION VERSION_LESS 3.9)
    cmake_policy(SET CMP0069 NEW)
    include(CheckIPOSupported)
    check_ipo_supported(RESULT CMAKE_IPO_AVAILABLE)
else()
    set(CMAKE_IPO_AVAILABLE FALSE)
endif()

# Ensure that default value is false
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION FALSE)
if (TARANTOOL_LTO AND CMAKE_IPO_AVAILABLE)
    # linker.cmake script guaratntees that linker exists at this point
    execute_process(COMMAND ld.${TARANTOOL_LINKER} -v OUTPUT_VARIABLE linker_v)

    # e.g. GNU gold (GNU Binutils for Ubuntu 2.29.1) 1.14
    string(REGEX MATCH "^GNU (gold|ld) [^)]+[)] ([0-9.]+).*$" linker_valid ${linker_v})

    if (NOT linker_valid)
        message(FATAL_ERROR "Unsuported linker (ld or gold expected)")
    endif()

    set(linker_name ${CMAKE_MATCH_1})
    set(linker_version ${CMAKE_MATCH_2})
    message(STATUS "Found linker ${linker_name} VERSION ${linker_version}")
    if (linker_name STREQUAL "ld")
        if (NOT TARANTOOL_LINKER STREQUAL "bfd")
            message(FATAL_ERROR "bfd linker expected (gold found)")
        endif()

        if (NOT linker_version VERSION_LESS "2.31")
            set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
        endif()
    endif()
    if (linker_name STREQUAL "gold")
        if (NOT TARANTOOL_LINKER STREQUAL "gold")
            message(FATAL_ERROR "gold linker expected (bfd (ld) found)")
        endif()
        if (NOT linker_version VERSION_LESS "1.15")
            set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
        endif()
    endif()
endif()

message(STATUS "LTO enabled ${CMAKE_INTERPROCEDURAL_OPTIMIZATION}")
