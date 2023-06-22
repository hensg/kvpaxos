add_subdirectory(compresser)
add_subdirectory(request)
#add_subdirectory(evclient)
add_subdirectory(types)
add_subdirectory(storage)
add_subdirectory(scheduler)
add_subdirectory(graph)

add_executable(replica)

# set_target_properties(replica PROPERTIES COMPILE_FLAGS "-g -O0 -fno-omit-frame-pointer")
# set_target_properties(replica PROPERTIES LINK_FLAGS "-g")

target_sources(
    replica
        PRIVATE
            replica.cpp
)

target_link_libraries(
    replica
        PRIVATE
            CONAN_PKG::toml11
            request
            types
            scheduler
)
