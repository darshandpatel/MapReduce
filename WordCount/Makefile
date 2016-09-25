INPUT_FILE="./src/data/alice.txt"
hadoop.root="/Users/Darshan/Documents/hadoop-2.7.3"

default:
	rm -rf build target .gradle WordCount.jar 
	gradle build
	cp ./build/libs/WordCount.jar .
	
local:
	${hadoop.root}/bin/hadoop jar WordCount.jar ${INPUT_FILE} output
	
