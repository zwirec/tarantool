##
## Manage LTO (Link-Time-Optimization) and IPO (Inter-Procedural-Optimization)
##

# Tarantool uses both dynamic-list and lto link options, which works only
# since binutils:
#  - 2.30 for linking with gold (gold version is 1.15)
#  - last 2.30 or 2.31 in case of ld (bfd)

if (NOT DEFINED TARANTOOL_LTO)
    set(TARANTOOL_LTO FALSE)
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
if (TARANTOOL_LTO AND CMAKE_IPO_AVAILABLE AND NOT TARGET_OS_DARWIN)
    execute_process(COMMAND ld -v OUTPUT_VARIABLE linker_v)
	message(STATUS "LTO linker_v ${linker_v}")

    # e.g. GNU ld (GNU Binutils) 2.29
    string(REGEX MATCH "^GNU ld [^)]+[)] ([0-9.]+).*$" linker_valid ${linker_v})

    if (NOT linker_valid)
        message(FATAL_ERROR "Unsuported linker (ld expected)")
    endif()

    set(linker_version ${CMAKE_MATCH_1})
    message(STATUS "Found linker ld VERSION ${linker_version}")

    if (NOT linker_version VERSION_LESS "2.31")
        set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
    elseif(NOT linker_version VERSION_LESS "2.30")
        # Use gold if LTO+dynamic-list is available in gold & not in ld
        find_program(gold_available "ld.gold")
        if (gold_available)
            message(WARNING "Use gold linker (to enable LTO)")
            SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=gold")
            set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
        endif()
    endif()
endif()

if (TARANTOOL_LTO AND CMAKE_IPO_AVAILABLE AND TARGET_OS_DARWIN)
    set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
endif()

message(STATUS "LTO enabled ${CMAKE_INTERPROCEDURAL_OPTIMIZATION}")
