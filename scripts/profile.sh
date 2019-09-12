#!/bin/bash

fileName=""                # filename of the log
memoryLimit=-1             # cgroup limits
commandPrefix=""           # prefix to write before the command
deviceName="nvme1n1"           # device name
devicePart="/dev/nvme1n1p1"    # where is the database?

function usage() {
   echo "Usage ./profile -f logName -m memoryLimit -- <command>"
   exit -1
}

# Parse the arguments
while [[ $# -gt 0 ]]; do
    case $1 in
       -f) fileName=$2;;                              # -f
       -m) memoryLimit=$2;;                           # -m
       -h) usage;;                                    # -h
       --) shift; break;;                             # --
       *) echo "Unrecognized argument $1"; usage;;    # error
    esac
    shift
    shift
done

# We need a name for the log
if [[ "$fileName" == "" ]]; then
   usage
else
   echo "#Writing output in $fileName"
fi

# Create and configure the cgroups
if [[ "$memoryLimit" != "-1" ]]; then
   if [ ! -f /sys/fs/cgroup/memory/ooc/memory.kmem.limit_in_bytes ]; then
      sudo sh -c "cgcreate -g memory:ooc"
   fi
   mem=$((memoryLimit * 1024 * 1024 * 1024))
   sudo sh -c "echo $mem > /sys/fs/cgroup/memory/ooc/memory.kmem.limit_in_bytes"
   sudo sh -c "echo $mem > /sys/fs/cgroup/memory/ooc/memory.limit_in_bytes"
   mem=`cat /sys/fs/cgroup/memory/ooc/memory.kmem.limit_in_bytes`
   echo "#Cgroup configured to use $mem bytes";
   commandPrefix="sudo cgexec -g memory:ooc $commandPrefix"
fi


# Clear caches
sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'

# Dump how many bytes have been read/written on the device so far
iostat | grep ${deviceName} |  awk '{print "l1 "$5" "$6}' > ${fileName}.iostat

# Trace IOs
sudo sh -c "blktrace -d ${devicePart} -w 1000 -o ${fileName}" &

#Execute the command
echo "#$commandPrefix" "$@"
eval $commandPrefix "$@"

# Kill blktrace
sudo sh -c "killall -w blktrace"

# Append how many bytes have been read/written on the device so far
iostat | grep ${deviceName} |  awk '{print "l2 "$5" "$6}' >> ${fileName}.iostat
cat ${fileName}.iostat| awk '{print $2}' | paste -sd- - | bc | awk -F '-' '{printf "GB_READ %.3f \n",$2/1024./1024.}' > ${fileName}.bw
cat ${fileName}.iostat| awk '{print $3}' | paste -sd- - | bc | awk -F '-' '{printf "GB_WRITTEN %.3f \n",$2/1024./1024.}' >> ${fileName}.bw
cat ${fileName}.bw

# Parse blktrace output
blkparse -i ${fileName}.blktrace.0 -d ${fileName}.bin -o ${fileName}.out
#iowatcher -t ${fileName} -o ${fileName}.svg

# Use custom scripts to parse blktrace
scriptDir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
${scriptDir}/plot.sh "${fileName}"


#Cleanup
sudo sh -c "rm *blktrace* *.dump *.bin ${fileName}.parsed.log ${fileName}.*get.log ${fileName}.out ${fileName}.iostat"
