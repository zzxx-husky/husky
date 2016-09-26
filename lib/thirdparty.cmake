message(STATUS "Initialize third party libraries...")

include(ExternalProject)
ExternalProject_Add(
    eigen
    HG_REPOSITORY https://bitbucket.org/eigen/eigen
    HG_TAG 3.2.9
    PREFIX ${PROJECT_BINARY_DIR}/eigen
    CMAKE_ARGS -DCMAKE_INSTALL_PREFIX:PATH=${PROJECT_BINARY_DIR}
    UPDATE_COMMAND ""
)

# Add thirdparty libraries to custom target
add_custom_target(ThirdPartyTarget DEPENDS eigen)

# Build third party
add_library(ThirdPartyLib thirdparty_dummy.cpp)
add_dependencies(ThirdPartyLib ThirdPartyTarget)

# Just for lib/CMakeLists.txt
set(EIGEN_INCLUDE "${PROJECT_BINARY_DIR}/include/eigen3")
include_directories(${EIGEN_INCLUDE})
