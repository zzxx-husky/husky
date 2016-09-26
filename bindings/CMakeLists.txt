include_directories(${PROJECT_SOURCE_DIR})

if(LIBHDFS3_FOUND)
    add_definitions(-DWITH_HDFS)
else(LIBHDFS3_FOUND)
    add_definitions(-DWITHOUT_HDFS)
endif(LIBHDFS3_FOUND)

# KAFKA
if(KAFKA_FOUND)
    include_directories(${KAFKA_INCLUDE_DIR})
    add_definitions(-DWITH_KAFKA)
endif(KAFKA_FOUND)

if(MONGOCLIENT_FOUND)
    include_directories(${MONGOCLIENT_INCLUDE_DIR})
    add_definitions(-DWITH_MONGODB)
endif(MONGOCLIENT_FOUND)

add_library(master-objs OBJECT opdag.cpp operation.cpp job.cpp opsplitter.cpp
    optimizer.cpp frontend_master_handlers.cpp)


add_library(daemon-objs OBJECT opdag.cpp operation.cpp itc.cpp task.cpp)

add_subdirectory(backend)

add_library(pyhusky-master STATIC 
    $<TARGET_OBJECTS:master-objs>
    $<TARGET_OBJECTS:pyhusky-master-objs>
    $<TARGET_OBJECTS:pyhusky-splitter-objs>)

add_library(pyhusky-bindings STATIC 
    $<TARGET_OBJECTS:daemon-objs>
    $<TARGET_OBJECTS:pyhusky-backend-objs>
    $<TARGET_OBJECTS:pyhusky-library-objs>)

### daemon

#set(husky pyhusky-bindings schusky-bindings husky-lib husky-io husky-core husky-base)
set(husky pyhusky-bindings husky-lib husky-io husky-core husky-base)

# For thirdparty libraries
set(EIGEN_INCLUDE "${PROJECT_BINARY_DIR}/include/eigen3")
include_directories(${EIGEN_INCLUDE})

add_executable(Daemon daemon.cpp)
target_link_libraries(Daemon ${husky})
target_link_libraries(Daemon ${EXTERNAL_LIB})
set_property(TARGET Daemon PROPERTY CXX_STANDARD 14)