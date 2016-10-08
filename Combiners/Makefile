INPUT_FILE="/Users/Darshan/Downloads/1912.csv"
HADOOP_ROOT="/usr/local/hadoop"

default: clean
	gradle build
	cp ./build/libs/Combiners.jar .

# No combiner program run
nocombiner: output-clean
	${HADOOP_ROOT}/bin/hadoop jar Combiners.jar code.NoCombiner ${INPUT_FILE} output

# Combiner program run	
combiner: output-clean
	${HADOOP_ROOT}/bin/hadoop jar Combiners.jar code.Combiner ${INPUT_FILE} output

# In mapper combiner run
inmapper: output-clean
	${HADOOP_ROOT}/bin/hadoop jar Combiners.jar code.InMapperCombiner ${INPUT_FILE} output

output-clean:
	rm -rf output

clean:
	rm -rf output .gradle Combiners.jar target build	
