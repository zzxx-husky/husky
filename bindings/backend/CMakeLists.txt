include_directories(${PROJECT_SOURCE_DIR})

if(LIBHDFS3_FOUND)
    add_definitions(-DWITH_HDFS)
else(LIBHDFS3_FOUND)
    add_definitions(-DWITHOUT_HDFS)
endif(LIBHDFS3_FOUND)

add_library(pyhusky-backend-objs OBJECT  pythonconnector.cpp
    threadconnector.cpp daemondriver.cpp workerdriver.cpp)
set_property(TARGET pyhusky-backend-objs PROPERTY CXX_STANDARD 14)

add_library(pyhusky-master-objs OBJECT master_handler.cpp)
set_property(TARGET pyhusky-master-objs PROPERTY CXX_STANDARD 14)

add_subdirectory(library)