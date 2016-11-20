INPUT_FILE="/Users/Darshan/Documents/MapReduce/sample_data" #/wikipedia-simple-html.bz2"
HADOOP_ROOT="/usr/local/hadoop"
OUTPUT_FOLDER="/Users/Darshan/Desktop/MapReduce"
CACHE_FOLDER="/Users/Darshan/tmp"

default:clean
	gradle build
	cp ./build/libs/PageRankMatrix.jar .

# No combiner program run
run-column:
	${HADOOP_ROOT}/bin/hadoop jar PageRankMatrix.jar main.ColumnMajorRun ${INPUT_FILE} ${OUTPUT_FOLDER}/output ${CACHE_FOLDER}

run-row:
	${HADOOP_ROOT}/bin/hadoop jar PageRankMatrix.jar main.RowMajorRun ${INPUT_FILE} ${OUTPUT_FOLDER}/output ${CACHE_FOLDER}

# Clean the build
clean:
	rm -rf ${OUTPUT_FOLDER}/output .gradle PageRankMatrix.jar ${OUTPUT_FOLDER}/parsing ${CACHE_FOLDER}/id ${CACHE_FOLDER}/data* ${OUTPUT_FOLDER}/matrix
