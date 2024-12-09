set(APP_DIR ./app)
file(GLOB_RECURSE APP_SRCS ${APP_DIR}/*.c)


message(STATUS "APP_SRCS: ${APP_SRCS}")


idf_component_register(
    SRCS 
        "main.c"

        ${APP_SRCS}
    INCLUDE_DIRS
        "."
        
        ${APP_DIR}
        )

# spiffs_create_partition_image(storage ../spiffs FLASH_IN_PROJECT)


