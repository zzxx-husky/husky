include_directories(${PROJECT_SOURCE_DIR})

if(LIBHDFS3_FOUND)
    add_definitions(-DWITH_HDFS)
else(LIBHDFS3_FOUND)
    add_definitions(-DWITHOUT_HDFS)
endif(LIBHDFS3_FOUND)

# For thirdparty libraries
set(EIGEN_INCLUDE "${PROJECT_BINARY_DIR}/include/eigen3")
include_directories(${EIGEN_INCLUDE})

# add_library(pyhusky-library-objs OBJECT register.cpp functional.cpp)
add_library(pyhusky-library-objs OBJECT register.cpp functional.cpp)
set_property(TARGET pyhusky-library-objs PROPERTY CXX_STANDARD 14)
add_dependencies(pyhusky-library-objs eigen)

add_library(pyhusky-splitter-objs OBJECT splitter_register.cpp)
set_property(TARGET pyhusky-splitter-objs PROPERTY CXX_STANDARD 14)