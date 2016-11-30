include_directories(${PROJECT_SOURCE_DIR} ${EXTERNAL_INCLUDE})

add_library(master-objs OBJECT opdag.cpp operation.cpp job.cpp opsplitter.cpp
    optimizer.cpp frontend_master_handlers.cpp)
husky_default_properties(master-objs)

add_library(daemon-objs OBJECT opdag.cpp operation.cpp itc.cpp task.cpp)
husky_default_properties(daemon-objs)

add_subdirectory(backend)

add_library(pyhusky-master STATIC 
    $<TARGET_OBJECTS:master-objs>
    $<TARGET_OBJECTS:pyhusky-master-objs>
    $<TARGET_OBJECTS:pyhusky-splitter-objs>)
husky_default_properties(pyhusky-master)

add_library(pyhusky-bindings STATIC 
    $<TARGET_OBJECTS:daemon-objs>
    $<TARGET_OBJECTS:pyhusky-backend-objs>
    $<TARGET_OBJECTS:pyhusky-library-objs>)
husky_default_properties(pyhusky-bindings)

### daemon

set(husky pyhusky-bindings husky-lib husky-io husky-core husky-base)

# For thirdparty libraries
set(EIGEN_INCLUDE "${PROJECT_BINARY_DIR}/include/eigen3")
include_directories(${EIGEN_INCLUDE})

add_executable(Daemon daemon.cpp)
target_link_libraries(Daemon ${husky})
target_link_libraries(Daemon ${EXTERNAL_LIB})
husky_default_properties(Daemon)