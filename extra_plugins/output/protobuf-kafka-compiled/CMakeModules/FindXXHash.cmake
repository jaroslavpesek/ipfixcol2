# FindXXHash.cmake
# Find xxHash library
#
# This module defines:
#  XXHASH_FOUND - System has xxHash
#  XXHASH_INCLUDE_DIRS - The xxHash include directories
#  XXHASH_LIBRARIES - The libraries needed to use xxHash (if any)

find_path(XXHASH_INCLUDE_DIR
    NAMES xxhash.h
)

find_library(XXHASH_LIBRARY
    NAMES xxhash
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(XXHash
    DEFAULT_MSG
    XXHASH_INCLUDE_DIR
)

if(XXHASH_FOUND)
    set(XXHASH_INCLUDE_DIRS ${XXHASH_INCLUDE_DIR})
    if(XXHASH_LIBRARY)
        set(XXHASH_LIBRARIES ${XXHASH_LIBRARY})
    else()
        # xxHash can be header-only
        set(XXHASH_LIBRARIES "")
    endif()
endif()

mark_as_advanced(XXHASH_INCLUDE_DIR XXHASH_LIBRARY)
