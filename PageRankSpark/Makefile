INPUT_FILE="/Users/Darshan/Documents/MapReduce/sample_data"
NUMBER_OF_PARTITIONER = 2

run:
	rm -rf output
	sbt "run-main Parser ${INPUT_FILE} output"

#Use this option to update default input file partitioner
runWithPartitioner:
	rm -rf output
	sbt "run-main Parser ${INPUT_FILE} output ${NUMBER_OF_PARTITIONER}"
    
clean:
	rm -rf output target
