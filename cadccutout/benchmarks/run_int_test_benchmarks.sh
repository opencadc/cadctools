#!/bin/bash

DATA_DIR="${1:-/Users/jenkinsd/Downloads}"
FCAT_DOCKER_IMAGE="opencadc/fcat:1"
CADCCUTOUT_DOCKER_IMAGE_2="opencadc/cadccutout:2.7-alpine"
CADCCUTOUT_DOCKER_IMAGE_3="opencadc/cadccutout:3.7-alpine"

BENCHMARK_FILES=("test-gmims-cube.fits" "test-alma-cube.fits" "test-cgps-cube.fits" "test-sitelle-cube.fits" "test-vlass-cube.fits" "test-hst-mef.fits")
BENCHMARK_CUTOUTS=("[200:500,100:300,100:140]" "[80:220,100:150,100:150]" "[200:400,500:1000,10:20]" "[1000:1200,800:1000,160:200]" "[500:900,300:1000,8:12]" "[SCI,10][80:220,100:150][1][10:16,70:90][106][8:32,88:112][126]")

for IDX in "${!BENCHMARK_FILES[@]}";
do
  TEST_FILE="${BENCHMARK_FILES[$IDX]}";
  CUTOUT_VAL="${BENCHMARK_CUTOUTS[$IDX]}";
  echo "Cutting out ${CUTOUT_VAL} from ${TEST_FILE}.";

  TIME_OUTPUT_FILE="${TEST_FILE}_benchmarks.txt"
  FCAT_OUTPUT_FILE="FCAT_${TEST_FILE}_benchmark.txt";

  rm -rf ${OUTPUT_FILE};
  rm -rf ${FCAT_OUTPUT_FILE};
  rm -rf ${CADCCUTOUT_OUTPUT_FILE};

  FCAT_CMD="docker run --rm -v ${DATA_DIR}:/usr/src/data ${FCAT_DOCKER_IMAGE} time fcat /usr/src/data/${TEST_FILE}${CUTOUT_VAL}";
  CADCCUTOUT_2_CMD="docker run --rm -v ${DATA_DIR}:/usr/src/data ${CADCCUTOUT_DOCKER_IMAGE_2} time cadccutout --infile /usr/src/data/${TEST_FILE} ${CUTOUT_VAL}";
  CADCCUTOUT_3_CMD="docker run --rm -v ${DATA_DIR}:/usr/src/data ${CADCCUTOUT_DOCKER_IMAGE_3} time cadccutout --infile /usr/src/data/${TEST_FILE} ${CUTOUT_VAL}";

  echo "FCAT" >> ${TIME_OUTPUT_FILE};
  echo "" >> ${TIME_OUTPUT_FILE};
  echo "${FCAT_CMD}" >> ${TIME_OUTPUT_FILE};
  echo "" >> ${TIME_OUTPUT_FILE};
  $( { ${FCAT_CMD}; } 1>/dev/null 2>>${TIME_OUTPUT_FILE} );
  echo ""
  echo ""
  echo "CADCCUTOUT PYTHON 2" >> ${TIME_OUTPUT_FILE};
  echo "" >> ${TIME_OUTPUT_FILE};
  echo "${CADCCUTOUT_2_CMD}" >> ${TIME_OUTPUT_FILE};
  echo "" >> ${TIME_OUTPUT_FILE};
  $( { ${CADCCUTOUT_2_CMD}; } 1>/dev/null 2>>${TIME_OUTPUT_FILE} );
  echo ""
  echo ""
  echo "CADCCUTOUT PYTHON 3" >> ${TIME_OUTPUT_FILE};
  echo "" >> ${TIME_OUTPUT_FILE};
  echo "${CADCCUTOUT_3_CMD}" >> ${TIME_OUTPUT_FILE};
  echo "" >> ${TIME_OUTPUT_FILE};
  $( { ${CADCCUTOUT_3_CMD}; } 1>/dev/null 2>>${TIME_OUTPUT_FILE} );
  echo "Finished writing to ${TIME_OUTPUT_FILE}.";
done
