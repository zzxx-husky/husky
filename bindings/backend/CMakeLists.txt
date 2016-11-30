include_directories(${PROJECT_SOURCE_DIR} ${EXTERNAL_INCLUDE})

add_library(pyhusky-backend-objs OBJECT  pythonconnector.cpp
    threadconnector.cpp daemondriver.cpp workerdriver.cpp)
husky_default_properties(pyhusky-backend-objs)

add_library(pyhusky-master-objs OBJECT master_handler.cpp)
husky_default_properties(pyhusky-master-objs)

add_subdirectory(library)