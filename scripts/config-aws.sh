#!/bin/bash

if [ $EUID -ne 0 ]; then
   echo "This script must be run as root" 
   exit 1
fi

#Disable frequency scaling to limit stddev
echo 1 > /sys/devices/system/cpu/intel_pstate/no_turbo
echo 0 > /proc/sys/kernel/numa_balancing
systemctl disable ondemand
for i in $(seq 0 35); do
   echo "performance" > /sys/devices/system/cpu/cpu$i/cpufreq/scaling_governor
done

#Disable hyperthreading
for i in $(seq 36 71); do
   echo 0 > /sys/devices/system/cpu/cpu$i/online
done

#Create mounts
used=`df -h | grep nvme | perl -pe 's/.*?([0-9]).*/\1/'` # For some reason / is sometimes mounted on an NVMe, so discard it
j=0
for i in 0 1 2 3 4 5 6 7 8; do
   [ "$i" = "$used" ] && continue
   mkfs -t ext4 /dev/nvme${i}n1
   mkdir /scratch${j}
   mount -o rw,noatime,nodiratime,block_validity,delalloc,nojournal_checksum,barrier,user_xattr,acl /dev/nvme${i}n1 /scratch${j}/
   mkdir /scratch${j}/kvell
   sudo chown ubuntu:ubuntu /scratch${j}/kvell
   j=$((j+1))
done

#That's what we use for other systems
#pvcreate /dev/nvme[12345678]n1
#vgcreate vol_e27  /dev/nvme[12345678]n1
#lvcreate --extents 100%FREE --stripes 8 --stripesize 256 --name root vol_e27
#mkfs -t ext4 /dev/mapper/vol_e27-root
#mkdir /scratch
#mount -o rw,noatime,nodiratime,block_validity,delalloc,nojournal_checksum,barrier,user_xattr,acl /dev/mapper/vol_e27-root /scratch/

ulimit -n 4096
