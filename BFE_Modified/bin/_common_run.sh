#!/bin/bash

############################
####  PLEASE DO NOT
####      EDIT !!!!
############################

if [[ -z "$INC_NUM" ]]; then
    INC_NUM=2
fi

#### START SCRIPT ####


echo "Started in $(date +%D\ %T)"

TIMESTAMP=$(date +%y-%m-%d_%H-%M-%S)

START=$(date +%s.%N)

for METHOD in ${METHODS}; do


    printf "Running method $METHOD\n"

    LOG_FILE=${LOG_DIR}/${METHOD}\_${DATA_ID}\_${TIMESTAMP}.out
    ERR_FILE=${LOG_DIR}/${METHOD}\_${DATA_ID}\_${TIMESTAMP}.err

    echo "#################### $METHOD #####################"

    echo "### Varying numFlocks ###$(date '+%d/%m/%y %H:%M:%S')"
    for (( NUM=$MIN_NUM; NUM <= $MAX_NUM; NUM+=$INC_NUM )); do
        LOG_PREFIX=${LOG_DIR}/${METHOD}\_${DATA_ID}\_${NUM}-${DEF_DIST}-${DEF_DELTA}\_${TIMESTAMP}
        sudo sh -c 'sync; echo 3 >/proc/sys/vm/drop_caches'
        ./${BUILD_DIR}/flocks -m ${METHOD} \
            -s ${NUM} -l ${DEF_DELTA} -d ${DEF_DIST} \
            -f ${DATA_DIR}/${DATA_FILE} -p ${LOG_PREFIX} >> ${LOG_FILE} 2>> ${ERR_FILE}
    done
    echo "####################################"

    echo "### Varying distFlocks $(date '+%d/%m/%y %H:%M:%S')"
    for DIST in ${DIST_LIST} ; do
        LOG_PREFIX=${LOG_DIR}/${METHOD}\_${DATA_ID}\_${DEF_NUM}-${DIST}-${DEF_DELTA}\_${TIMESTAMP}
        sudo sh -c 'sync; echo 3 >/proc/sys/vm/drop_caches'
        ./${BUILD_DIR}/flocks -m ${METHOD} \
            -s ${DEF_NUM} -l ${DEF_DELTA} -d ${DIST} \
            -f ${DATA_DIR}/${DATA_FILE} -p ${LOG_PREFIX} >> ${LOG_FILE} 2>> ${ERR_FILE}
    done

    echo "### Varying sizeFlocks $(date '+%d/%m/%y %H:%M:%S')"
    for (( DELTA=$MIN_DELTA; DELTA <= MAX_DELTA; DELTA+=2 )); do
        LOG_PREFIX=${LOG_DIR}/${METHOD}\_${DATA_ID}\_${DEF_NUM}-${DEF_DIST}-${DELTA}\_${TIMESTAMP}
        sudo sh -c 'sync; echo 3 >/proc/sys/vm/drop_caches'
        ./${BUILD_DIR}/flocks -m ${METHOD} \
            -s ${DEF_NUM} -l ${DELTA} -d ${DEF_DIST} \
            -f ${DATA_DIR}/${DATA_FILE} -p ${LOG_PREFIX} >> ${LOG_FILE} 2>> ${ERR_FILE}
    done
    echo "############# END - $METHOD #############" >> ${LOG_FILE}


    sed -i.bak -r -e "/^Closing|^Openning/d" -e "/^$/d" ${LOG_FILE}


done

echo "Finished in $(date +%D\ %T)"
END=$(date +%s.%N)
DIFF=$(echo "$END - $START" | bc)

echo "It took $DIFF to run everything..."
echo "*************************************************"


if [ "$EMAIL" = true ] ; then
    ${PHP} ${MAILER} \
        ${SEND_TO}     \
        "<p>The machine ${MACHINE} finished processing the dataset ${DATA_ID}.</p><p>It took ${DIFF} seconds to finish.</p>" \
        "${MACHINE} - notification system"
fi
