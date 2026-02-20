cmake_minimum_required(VERSION 3.12)
project(ipfixcol2-protobuf-kafka-compiled-output)

# Description of the project
set(PK_DESCRIPTION
    "Output plugin for IPFIXcol2 that serializes IPFIX records to Protocol Buffers and sends them to Kafka."
)

set(PK_VERSION_MAJOR 1)
set(PK_VERSION_MINOR 0)
set(PK_VERSION_PATCH 0)
set(PK_VERSION ${PK_VERSION_MAJOR}.${PK_VERSION_MINOR}.${PK_VERSION_PATCH})

include(CMakeModules/install_dirs.cmake)
include(CheckCCompilerFlag)
include(CheckCXXCompilerFlag)

# Include custom FindXXX modules
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/CMakeModules")

# Find IPFIXcol
find_package(IPFIXcol2 2.3.0 REQUIRED)

# Find librdkafka
find_package(LibRDKafka 0.9.3 REQUIRED)

# Find Protobuf
find_package(Protobuf REQUIRED)

# Find xxHash (prefer pkg-config, fallback to custom find module)
find_package(PkgConfig QUIET)
if(PKG_CONFIG_FOUND)
    pkg_check_modules(XXHASH REQUIRED libxxhash)
else()
    find_package(XXHash REQUIRED)
endif()


# Set default build type if not specified by user
if (NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release
        CACHE STRING "Choose type of build (Release/Debug)." FORCE)
endif()

option(ENABLE_DOC_MANPAGE "Enable manual page building" ON)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Hard coded definitions
set(CMAKE_C_FLAGS            "${CMAKE_C_FLAGS} -fvisibility=hidden")
set(CMAKE_C_FLAGS_RELEASE    "-O2 -DNDEBUG")
set(CMAKE_C_FLAGS_DEBUG      "-g -O0 -Wall -Wextra -pedantic")
set(CMAKE_CXX_FLAGS          "${CMAKE_CXX_FLAGS} -fvisibility=hidden")
set(CMAKE_CXX_FLAGS_RELEASE  "-O2 -DNDEBUG")
set(CMAKE_CXX_FLAGS_DEBUG    "-g -O0 -Wall -Wextra -pedantic")

# Header files for source code building
# For extra_plugins, we need to handle both installed ipfixcol2 and local development
# When built standalone, IPFIXCOL2_INCLUDE_DIRS should point to installed headers
# When built in-tree, we need the main repo's include AND build directories
include_directories(
    "${IPFIXCOL2_INCLUDE_DIRS}"
    "${Protobuf_INCLUDE_DIRS}"
    "${XXHASH_INCLUDE_DIRS}"
)

# Also check if we're building against local repo (for api.h generated file)
if(EXISTS "${IPFIXCOL2_INCLUDE_DIRS}/../build/include")
    include_directories("${IPFIXCOL2_INCLUDE_DIRS}/../build/include")
endif()

# librdkafka include
if(LIBRDKAFKA_INCLUDE_DIRS)
    include_directories("${LIBRDKAFKA_INCLUDE_DIRS}")
endif()

# Create a linkable module
add_library(protobuf-kafka-compiled-output MODULE
    src/plugin.cpp
    src/Config.cpp
    src/TranslationTable.cpp
    src/KafkaProducer.cpp
    src/FlowConverter.cpp
    generated_schema/schema.pb.cc
)

target_link_libraries(protobuf-kafka-compiled-output
    ${LIBRDKAFKA_LIBRARIES}
    ${Protobuf_LIBRARIES}
    ${XXHASH_LIBRARIES}
    protobuf
)

install(
    TARGETS protobuf-kafka-compiled-output
    LIBRARY DESTINATION "${INSTALL_DIR_LIB}/ipfixcol2/"
)

if (ENABLE_DOC_MANPAGE)
    find_package(Rst2Man)
    if (NOT RST2MAN_FOUND)
        message(WARNING "rst2man is not available. Manual page will not be generated.")
    else()
        # Build a manual page
        set(SRC_FILE "${CMAKE_CURRENT_SOURCE_DIR}/doc/ipfixcol2-protobuf-kafka-compiled-output.7.rst")
        set(DST_FILE "${CMAKE_CURRENT_BINARY_DIR}/ipfixcol2-protobuf-kafka-compiled-output.7")

        add_custom_command(TARGET protobuf-kafka-compiled-output PRE_BUILD
            COMMAND ${RST2MAN_EXECUTABLE} --syntax-highlight=none ${SRC_FILE} ${DST_FILE}
            DEPENDS ${SRC_FILE}
            VERBATIM
        )

        install(
            FILES "${DST_FILE}"
            DESTINATION "${INSTALL_DIR_MAN}/man7"
        )
    endif()
endif()
