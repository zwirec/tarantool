# Tarantool uses compiler as a frontend to a linker.
# TARANTOOL_LINKER variable is used to determine which linker to use.
# TARANTOOL_LINKER possible values:
#   - bfd (default ld linker)
#   - gold (default) (advanced, faster linker)

if (NOT TARANTOOL_LINKER)
    set(TARANTOOL_LINKER "gold")
endif()

if (NOT TARANTOOL_LINKER STREQUAL "bfd"
        AND NOT TARANTOOL_LINKER STREQUAL "gold")
    message(FATAL_ERROR "TARANTOOL_LINKER should be `gold` or `bfd`)")
endif()

# By default the -rdynamic option is enabled, while Tarantool uses dynamic-list.
# Cleanup link flags.
set(CMAKE_SHARED_LIBRARY_LINK_CXX_FLAGS "")
set(CMAKE_SHARED_LIBRARY_LINK_C_FLAGS "")

if (TARANTOOL_LINKER STREQUAL "gold")
    find_program(gold_available "ld.gold")
    if (NOT gold_available)
        message(WARNING "gold linker is not available, use bfd (ld) instead")
        set(TARANTOOL_LINKER "bfd")
    else()
        message(STATUS "Linker `gold` selected")
        # Incremental linking cannot be applied because Tarantool uses linker
        # plugins.
        # TODO: enable this feature when all plugins can be disabled (Debug mode
        # and LTO disabled).
        SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=gold")
    endif()
endif()

if (TARANTOOL_LINKER STREQUAL "bfd")
    message(STATUS "Linker `bfd` (ld) selected")
    SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fuse-ld=bfd")
endif()
