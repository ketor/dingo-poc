enable_testing()

file(GLOB TEST_SRCS "test_*.cc")

message(STATUS "zhangjie --------------------------- ${ROCKSDB_INCLUDE_DIR}")
message(STATUS "zhangjie --------------------------- ${ROCKSDB_LIBRARIES}")

foreach(TEST_SRC ${TEST_SRCS})
  message(STATUS "TEST_SRC: ${TEST_SRC}")
  get_filename_component(TEST_WE ${TEST_SRC} NAME_WE)
  add_executable(${TEST_WE}
                 ${TEST_SRC}
                 $<TARGET_OBJECTS:DINGODB_OBJS>
                 $<TARGET_OBJECTS:PROTO_OBJS>
                )
  add_dependencies(${TEST_WE} ${DEPEND_LIBS})
  target_link_libraries(${TEST_WE}
                        "-Xlinker \"-(\""
                        ${GTEST_MAIN_LIBRARIES}
                        ${GTEST_LIBRARIES}
                        ${DYNAMIC_LIB}
                        # -L/home/zhangjie/0303/zj-dingo-store/xdprocks/lib -lxdprocks
                        ${ROCKSDB_LIBRARIES}
                        "-Xlinker \"-)\""
                        )
endforeach()

add_subdirectory(expr)
