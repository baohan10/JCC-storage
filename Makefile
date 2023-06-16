OUTPUT_BINARY_NAME = "coordinator"
OUTPUT_DIR_NAME = "coordinator"


ASSETS_DIR_NAME = "assets"
BUILD_DIR = "../../build"

build:
	go build -o ${BUILD_DIR}/${OUTPUT_DIR_NAME}/${OUTPUT_BINARY_NAME}${OUTPUT_BINARY_EXT}
	@if [ -d ${ASSETS_DIR_NAME} ] && [ -n "`ls -A ${ASSETS_DIR_NAME}`" ] ;then \
		cp -r ${ASSETS_DIR_NAME}/* ${BUILD_DIR}/${OUTPUT_DIR_NAME}/; \
	fi
	
clean:
	rm -f ${BUILD_DIR}/${OUTPUT_DIR_NAME}/${OUTPUT_BINARY_NAME}