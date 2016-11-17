INPUT_FILE="/Users/Darshan/Documents/MapReduce/sample_data/wikipedia-simple-html.bz2"
HADOOP_ROOT="/usr/local/hadoop"

default:clean
	gradle build
	cp ./build/libs/PageRankMatrix.jar .

# No combiner program run
run:
	${HADOOP_ROOT}/bin/hadoop jar PageRankMatrix.jar main.Run ${INPUT_FILE} output

# Clean the build
clean:
	rm -rf output .gradle PageRankMatrix.jar parsing  