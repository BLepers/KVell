#!/bin/bash
scriptDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
mainDir="${scriptDir}/.."
tcmalloc="env LD_PRELOAD=${HOME}/gperftools/.libs/libtcmalloc.so "

#/!\ This script will NOT work if you modify main.c

#
#Run YCSB A B C (D = A, F =B)
#4 workers per disk, 4 load injectors
#
rm -f /scratch*/kvell/*

cp ${mainDir}/main.c ${mainDir}/main.c.bak
cat ${mainDir}/main.c | perl -pe 's://.nb_load_injectors = 4:.nb_load_injectors = 4:' | perl -pe 's:[^/].nb_load_injectors = 12: //.nb_load_injectors = 12:' | perl -pe 's:[^/]ycsb_e_uniform,: //ycsb_e_uniform,:' | perl -pe 's://ycsb_a_uniform,:ycsb_a_uniform,:' | perl -pe 's://ycsb_a_zipfian,:ycsb_a_zipfian,:' > ${mainDir}/main.c.tmp
mv ${mainDir}/main.c.tmp ${mainDir}/main.c
make -C ${mainDir} -j

echo "Run 1"
${tcmalloc} ${mainDir}/main 8 4 | tee log_ycsb_1

echo "Run 2"
${tcmalloc} ${mainDir}/main 8 4 | tee log_ycsb_2

mv ${mainDir}/main.c.bak ${mainDir}/main.c

#
#Run YCSB E
#3 workers per disk, 12 load innjectors
#
rm -f /scratch*/kvell/* # Change in # workers, reset DB

cp ${mainDir}/main.c ${mainDir}/main.c.bak
cat ${mainDir}/main.c | perl -pe 's://.nb_load_injectors = 12:.nb_load_injectors = 12:' | perl -pe 's:[^/].nb_load_injectors = 4: //.nb_load_injectors = 4:' | perl -pe 's://ycsb_e_uniform, y:ycsb_e_uniform, y:' | perl -pe 's:[^/]ycsb_a_uniform,: //ycsb_a_uniform,:' | perl -pe 's:[^/]ycsb_a_zipfian,: //ycsb_a_zipfian,:' > ${mainDir}/main.c.tmp
cp ${mainDir}/main.c.tmp ${mainDir}/main.c
make -C ${mainDir} -j

echo "Run 1 (scans)"
${tcmalloc} ${mainDir}/main 8 3 | tee log_ycsb_e_1

echo "Run 2 (scans)"
${tcmalloc} ${mainDir}/main 8 3 | tee log_ycsb_e_2

mv ${mainDir}/main.c.bak ${mainDir}/main.c


#
#Show results
#
${scriptDir}/parse.pl log_ycsb_*
