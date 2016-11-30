include_directories(${PROJECT_SOURCE_DIR} ${EXTERNAL_INCLUDE})

# For thirdparty libraries
set(EIGEN_INCLUDE "${PROJECT_BINARY_DIR}/include/eigen3")
include_directories(${EIGEN_INCLUDE})

add_library(pyhusky-library-objs OBJECT register.cpp functional.cpp linear_regression.cpp logistic_regression.cpp)
husky_default_properties(pyhusky-library-objs)
add_dependencies(pyhusky-library-objs eigen)

add_library(pyhusky-splitter-objs OBJECT splitter_register.cpp)
husky_default_properties(pyhusky-splitter-objs)