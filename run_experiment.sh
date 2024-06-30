#!/bin/bash

PERF=0
RUNS=0
PERCENTILE_1="50.0"
PERCENTILE_2="99.9"

SERVER_TIMEOUT=30
SERVER_IP=10.90.0.20
SERVER_LAYOUT_LIST=("inline")
#SERVER_LAYOUT_LIST=("inline" "dispatcher1" "dispatcher2" "dispatcher4")
SERVER_SEED_LIST=(1646203793 986508091 193720917 1093215650 772188468 711307909 645856549 1127581467 765061083 1050115427 4231379 1000215989 1382853168 1927405477 306097907 1344972625 2098183364 323989894)
SERVER_CORES="1,3,5,7,9,11,13,15,17"

SERVER_SERVICE_TIME="1us"
SERVER_NUMBER_OF_CORES="8"

CLIENT_PCI_NIC="3b:00.0"
CLIENT_CORES="2,4,6,8"
CLIENT_INTERARRIVAL="exponential"
CLIENT_RATE_INITIAL=100000
CLIENT_RATE_INCREMENTAL=100000
CLIENT_FLOWS=16
CLIENT_KEY_SIZE=8
CLIENT_VALUE_SIZE=16
CLIENT_WARMINGUP=1000
CLIENT_DURATION=10
CLIENT_GET_PROPORTION=1.0
CLIENT_CONF_FILE="addr.cfg"
CLIENT_OUTPUT_FILE="output.dat"

TIMEOUT=$(( SERVER_TIMEOUT - CLIENT_DURATION - CLIENT_DURATION ))

error () {
	local Z=1.96
	local N=`wc -l $1 | cut -d' ' -f1`

	MEAN=`awk '{sum += $1} END {printf "%f", (sum/NR)}' $1`
	STDEV=`awk '{sum += ($1 - '$MEAN')^2} END {printf "%f", sqrt(sum/'$N')}' $1`
	ERROR=`awk 'BEGIN {printf "%f", '$Z' * '$STDEV'/sqrt('$N')}'`
}

process() {
	local N=`wc -l $1 | cut -d' ' -f1`
	echo $N > .tmp
	cat $1 >> .tmp
	echo -ne "$3\t$4\t$N\n" >> $2/n_lines.txt
	./percentile ${PERCENTILE_1} .tmp >> $2/percentiles_${PERCENTILE_1}.txt
	./percentile ${PERCENTILE_2} .tmp >> $2/percentiles_${PERCENTILE_2}.txt
}

for l in ${SERVER_LAYOUT_LIST[@]}; do
	if [ $l = "dispatcher1" ]; then
		CLIENT_FLOWS=2
		SERVER_NUMBER_OF_CORES="1 7"
	elif [ $l = "dispatcher2" ]; then
		CLIENT_FLOWS=4
		SERVER_NUMBER_OF_CORES="2 6"
	elif [ $l = "dispatcher4" ]; then
		CLIENT_FLOWS=8
		SERVER_NUMBER_OF_CORES="4 4"
	fi

	STOP=0
	CLIENT_CURRENT_RATE=${CLIENT_RATE_INITIAL}

	while [ ${STOP} -eq 0 ]; do
		DIR="results/$l/${CLIENT_CURRENT_RATE}"
		rm -rf $DIR 
		mkdir -p $DIR 
		rm -rf ${CLIENT_OUTPUT_FILE}

		for j in `seq 0 $RUNS`; do
			echo "Layout: $l -- Run: $j/$RUNS -- Rate: ${CLIENT_CURRENT_RATE}"

			## Run the server
			SERVER_SCRIPT_ARGS="${SERVER_NUMBER_OF_CORES}"
			ssh ${SERVER_IP} "cd JUNE/ROCKSDB/$l/demikernel; sh ./run_server.sh '${SERVER_CORES}' '${SERVER_SCRIPT_ARGS}' ${SERVER_TIMEOUT} ${PERF}" 1>/dev/null 2>/dev/null &

			## Sleep a while
			sleep 5

			## Run the client
			SEED=${SERVER_SEED_LIST[j]}
			sudo LD_LIBRARY_PATH=${HOME}/lib/x86_64-linux-gnu timeout $(( SERVER_TIMEOUT + 10 )) ./build/tcp-generator -a ${CLIENT_PCI_NIC} -n 4 -l ${CLIENT_CORES} -- -d ${CLIENT_INTERARRIVAL} -r ${CLIENT_CURRENT_RATE} -f ${CLIENT_FLOWS} -k ${CLIENT_KEY_SIZE} -v ${CLIENT_VALUE_SIZE} -w ${CLIENT_WARMINGUP} -t ${CLIENT_DURATION} -s ${SEED} -m ${CLIENT_GET_PROPORTION} -c ${CLIENT_CONF_FILE} -o ${CLIENT_OUTPUT_FILE} 1>/dev/null 2>/dev/null &

			## Sleep a while
			sleep $(( SERVER_TIMEOUT + 15 ))

			## Process the output file
			if [ ! -f ${CLIENT_OUTPUT_FILE} ]; then
				STOP=1
				break
			fi

			if [ ${PERF} -eq 1 ]; then
				ssh ${SERVER_IP} "sudo pkill -9 perf" 1>/dev/null 2>/dev/null
				scp ${SERVER_IP}:~/JUNE/$l/demikernel/output.perf output$j.perf 1>/dev/null 2>/dev/null
				mv output$j.perf $DIR 1>/dev/null 2>/dev/null
			fi

			mv ${CLIENT_OUTPUT_FILE} $DIR/output$j.dat 1>/dev/null 2>/dev/null
		done
		CLIENT_CURRENT_RATE=$(( CLIENT_CURRENT_RATE + CLIENT_RATE_INCREMENTAL ))
	done
done

error () {
    local Z=1.96
    local N=`wc -l $1 | cut -d' ' -f1`

    MEAN=`awk '{sum += $1} END {printf "%f", (sum/NR)}' $1`
    STDEV=`awk '{sum += ($1 - '$MEAN')^2} END {printf "%f", sqrt(sum/'$N')}' $1`
    ERROR=`awk 'BEGIN {printf "%f", '$Z' * '$STDEV'/sqrt('$N')}'`
}

percentile () {
    local N=`wc -l $1 | cut -d' ' -f1`
    echo $N > .tmp
    cat $1 | cut -d$'\t' -f1 >> .tmp
    ./percentile $2 .tmp >> $3/percentiles_$2.txt
}

for l in ${SERVER_LAYOUT_LIST[@]}; do
	CLIENT_CURRENT_RATE=$(( CLIENT_RATE_INITIAL ))
	DIR="results/$l/${CLIENT_CURRENT_RATE}"

	OUTPUT_FILE="results/$l.dat"
	rm -rf $OUTPUT_FILE 1>/dev/null 2>/dev/null

	while [ -d $DIR ]; do
		i=0
		INPUT="$DIR/output$i.dat"

		if [ ! -f $INPUT ]; then
			break
		fi

		rm -rf "$DIR/percentiles_${PERCENTILE_1}.txt" 1>/dev/null 2>/dev/null
		rm -rf "$DIR/percentiles_${PERCENTILE_2}.txt" 1>/dev/null 2>/dev/null

		while [ -f $INPUT ]; do
			percentile $INPUT ${PERCENTILE_1} $DIR
			percentile $INPUT ${PERCENTILE_2} $DIR

			i=$(( i + 1 ))
			INPUT="$DIR/output$i.dat"
		done

		error "$DIR/percentiles_${PERCENTILE_1}.txt"
		echo -ne "$CLIENT_CURRENT_RATE\t$MEAN\t$ERROR\t" >> $OUTPUT_FILE
		error "$DIR/percentiles_${PERCENTILE_2}.txt"
		echo -ne "$MEAN\t$ERROR\n" >> $OUTPUT_FILE

		CLIENT_CURRENT_RATE=$(( CLIENT_CURRENT_RATE + CLIENT_RATE_INCREMENTAL ))
		DIR="results/$l/${CLIENT_CURRENT_RATE}"
	done
	echo -ne "$CLIENT_CURRENT_RATE\t1e10\t0\t1e10\t0\n" >> $OUTPUT_FILE
done

rm -rf .tmp 1>/dev/null 2>/dev/null

curl -d "Finished..." ntfy.sh/fabricio-experiment
