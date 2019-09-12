#!/bin/bash                                                                                                                                                                                                                                                                        
fileName=$1
script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

make -C "${script_dir}"

#BW
cat ${fileName}.out | grep "C[[:space:]]*R\|C[[:space:]]*W" | awk '{print $4" "$10}' > ${fileName}.rw_get.log
${script_dir}/parse_log ${fileName}.rw_get.log 0 > ${fileName}.parsed.log
gnuplot -e "filename1='${fileName}.parsed.log'" -e "filename2='${fileName}_bw_all.pdf'" -e "leg='BW'" -e "iops=0" ${script_dir}/plot.gnuplot

cat ${fileName}.out | grep "C[[:space:]]*R" | awk '{print $4" "$10}' > ${fileName}.read_get.log
${script_dir}/parse_log ${fileName}.read_get.log 0 > ${fileName}.parsed.log
gnuplot -e "filename1='${fileName}.parsed.log'" -e "filename2='${fileName}_bw_read.pdf'" -e "leg='readBW'" -e "iops=0" ${script_dir}/plot.gnuplot

cat ${fileName}.out | grep "C[[:space:]]*W" | awk '{print $4" "$10}' > ${fileName}.write_get.log
${script_dir}/parse_log ${fileName}.write_get.log 0 > ${fileName}.parsed.log
gnuplot -e "filename1='${fileName}.parsed.log'" -e "filename2='${fileName}_bw_write.pdf'" -e "leg='writeBW'" -e "iops=0" ${script_dir}/plot.gnuplot


#IOPS
cat ${fileName}.out | grep "C[[:space:]]*R\|C[[:space:]]*W" | awk '{print $4" "$10}' > ${fileName}.rw_get.log
${script_dir}/parse_log ${fileName}.rw_get.log 1 > ${fileName}.parsed.log
gnuplot -e "filename1='${fileName}.parsed.log'" -e "filename2='${fileName}_iops_all.pdf'" -e "leg='BW'" -e "iops=1" ${script_dir}/plot.gnuplot

cat ${fileName}.out | grep "C[[:space:]]*R" | awk '{print $4" "$10}' > ${fileName}.read_get.log
${script_dir}/parse_log ${fileName}.read_get.log 1 > ${fileName}.parsed.log
gnuplot -e "filename1='${fileName}.parsed.log'" -e "filename2='${fileName}_iops_read.pdf'" -e "leg='readBW'" -e "iops=1" ${script_dir}/plot.gnuplot

cat ${fileName}.out | grep "C[[:space:]]*W" | awk '{print $4" "$10}' > ${fileName}.write_get.log
${script_dir}/parse_log ${fileName}.write_get.log 1 > ${fileName}.parsed.log
gnuplot -e "filename1='${fileName}.parsed.log'" -e "filename2='${fileName}_iops_write.pdf'" -e "leg='writeBW'" -e "iops=1"  ${script_dir}/plot.gnuplot

