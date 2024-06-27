#!/bin/bash

PERF=0
RUNS=19

SERVER_TIMEOUT=30
SERVER_IP=128.105.146.94
SERVER_LAYOUT_LIST=("inline")
SERVER_SEED_LIST=(1646203793 986508091 193720917 1093215650 772188468 711307909 645856549 1127581467 765061083 1050115427 4231379 1000215989 1382853168 1927405477 306097907 1344972625 2098183364 323989894 487415647 35498791 52157813)
SERVER_CORES="1,3,5,7,9,11,13,15,17"
SERVER_NUMBER_OF_CORES="8"

CLIENT_SERVER_ADDR="10.10.1.1"
CLIENT_SERVER_PORT=12345
CLIENT_REQUESTS=100000
CLIENT_CONNECTIONS=8
CLIENT_WARMINGUP=100
CLIENT_CORES="8,9,10,11,12,13,14,15"
CLIENT_PROPORTION=1.0
CLIENT_ROCKSDB_DIR="${HOME}/rocksdb"
CLIENT_OUTPUT_FILE="output.dat"

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
    if [ $l -eq "inline" ]; then
        CLIENT_CONNECTIONS=8
        SERVER_NUMBER_OF_CORES="8"
    elif [ $l -eq "dispatcher1" ]; then
        CLIENT_CONNECTIONS=7
        SERVER_NUMBER_OF_CORES="1 7"
    elif [ $l -eq "dispatcher2" ]; then
        CLIENT_CONNECTIONS=6
        SERVER_NUMBER_OF_CORES="2 6"
    elif [ $l -eq "dispatcher4" ]; then
        CLIENT_CONNECTIONS=4
        SERVER_NUMBER_OF_CORES="4 4"
    fi

    DIR="rocksdb_output/$l"
    rm -rf $DIR
    mkdir -p $DIR
    rm -rf ${CLIENT_OUTPUT_FILE}

    for j in `seq 0 $RUNS`; do
        echo "Layout: $l -- Run: $j/$RUNS"

        ## Run the server
        SERVER_SCRIPT_ARGS="${SERVER_NUMBER_OF_CORES}"
        # ssh ${SERVER_IP} "cd ROCKSDB/$l/demikernel; sh ./run_server.sh '${SERVER_CORES}' '${SERVER_SCRIPT_ARGS}' ${SERVER_TIMEOUT} ${PERF}" 1>/dev/null 2>/dev/null &
        ssh ${SERVER_IP} "cd $l/demikernel; sudo pkill -9 server_db.elf 1>/dev/null 2>/dev/null; sh ./run_server.sh '${SERVER_CORES}' '${SERVER_SCRIPT_ARGS}' ${SERVER_TIMEOUT} ${PERF}" 1>/dev/null 2>/dev/null &

        ## Sleep a while
        sleep 5

        ## Run the client
        SEED=${SERVER_SEED_LIST[j]}
        LD_LIBRARY_PATH=${CLIENT_ROCKSDB_DIR} timeout $(( SERVER_TIMEOUT + 10 )) ./client -h ${CLIENT_SERVER_ADDR} -p ${CLIENT_SERVER_PORT} -n ${CLIENT_REQUESTS} -c ${CLIENT_CONNECTIONS} -o ${CLIENT_OUTPUT_FILE} -s ${SEED} -w ${CLIENT_WARMINGUP} -l ${CLIENT_CORES} -f -m ${CLIENT_PROPORTION} 1>/dev/null 2>/dev/null &

        ## Sleep a while
        sleep $(( SERVER_TIMEOUT + 15 ))

        ## Process the output file
        if [ ! -f ${CLIENT_OUTPUT_FILE} ]; then
            break
        fi

        mv ${CLIENT_OUTPUT_FILE} $DIR/output$j.dat 1>/dev/null 2>/dev/null
    done

    rm -rf .tmp* 1>/dev/null 2>/dev/null
    for j in `seq 0 $RUNS`; do
        cat $DIR/output$j.dat | cut -d',' -f1 >> .tmp1
        cat $DIR/output$j.dat | cut -d',' -f2 >> .tmp2
        cat $DIR/output$j.dat | cut -d',' -f3 >> .tmp3
        cat $DIR/output$j.dat | cut -d',' -f4 >> .tmp4
    done
    error .tmp1
    echo -ne "Average \t$MEAN\t$ERROR\n" >> "rocksdb_output/$l.dat"
    error .tmp2
    echo -ne "p50\t$MEAN\t$ERROR\n" >> "rocksdb_output/$l.dat"
    error .tmp3
    echo -ne "p99.9\t$MEAN\t$ERROR\n" >> "rocksdb_output/$l.dat"
    error .tmp4
    echo -ne "Throughput $MEAN\t$ERROR" >> "rocksdb_output/$l.dat"
done

rm -rf .tmp* 1>/dev/null 2>/dev/null

curl -d "Finished..." ntfy.sh/fabricio-experiment
