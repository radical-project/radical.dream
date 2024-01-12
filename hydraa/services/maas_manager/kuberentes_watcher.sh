#!/bin/bash

set -e

# Function returning resources usage on current kubernetes cluster
node_count=0
total_percent_cpu_req=0
total_percent_mem_req=0
total_percent_cpu_lim=0
total_percent_mem_lim=0
total_percent_cpu_usage=0
total_percent_mem_usage=0
nodes=( $(kubectl get nodes --no-headers -o custom-columns=NAME:.metadata.name) )

# add any other CLI arguments here
while getopts f: flag
do
      case "${flag}" in
             f) output_file=${OPTARG};;
      esac
done


echo -e "TimeStamp,NodeID,CPUsAllocatable(M),MemoryAllocatable(Ki),CPUsRequest(M),MemoryRequest(Ki),CPUsLimit(M),MemoryLimit(Ki),CPUsUsage(%),MemoryUsage(%)" > "$output_file"


function kusage() {

    
    while true; do
        for node in ${nodes[@]}; do
            local requests=$(kubectl describe node $node | grep -A2 -E "Resource" | tail -n1 | tr -d '(%)')
            local abs_cpu=$(echo $requests | awk '{print $2}')
            local percent_cpu_req=$(echo $requests | awk '{print $3}')
            local node_cpu_req=$(echo $abs_cpu $percent_cpu_req | tr -d 'mKi' | awk '{print int($1/$2*100)}')
            local allocatable_cpu=$(echo $node_cpu_req $abs_cpu | tr -d 'mKi' | awk '{print int($1 - $2)}')
            local percent_cpu_lim=$(echo $requests | awk '{print $5}')
            local requests=$(kubectl describe node $node | grep -A3 -E "Resource" | tail -n1 | tr -d '(%)')
            local abs_mem=$(echo $requests | awk '{print $2}')
            local percent_mem_req=$(echo $requests | awk '{print $3}')
            local node_mem_req=$(echo $abs_mem $percent_mem_req | tr -d 'mKi' | awk '{print int($1/$2*100)}')
            local allocatable_mem=$(echo $node_mem_req $abs_mem | tr -d 'mKi' | awk '{print int($1 - $2)}')
            local percent_mem_lim=$(echo $requests | awk '{print $5}')
            local percent_cpu_usage=$(kubectl top nodes | grep $node | awk '{print $3}' | rev | cut -c2- | rev)
            local percent_mem_usage=$(kubectl top nodes | grep $node | awk '{print $5}' | rev | cut -c2- | rev)
            echo -e "$(date +%s),$node,${allocatable_cpu},${allocatable_mem},${percent_cpu_req},${percent_mem_req},${percent_cpu_lim},${percent_mem_lim},${percent_cpu_usage},${percent_mem_usage}" >> "$output_file"


            node_count=$((node_count + 1))
            total_percent_cpu_req=$((total_percent_cpu_req + percent_cpu_req))
            total_percent_mem_req=$((total_percent_mem_req + percent_mem_req))
            total_percent_cpu_lim=$((total_percent_cpu_lim + percent_cpu_lim))
            total_percent_mem_lim=$((total_percent_mem_lim + percent_mem_lim))
            total_percent_cpu_usage=$((total_percent_cpu_usage + percent_cpu_usage))
            total_percent_mem_usage=$((total_percent_mem_usage + percent_mem_usage))
        done

        local avg_percent_cpu_req=$((total_percent_cpu_req / node_count))
        local avg_percent_mem_req=$((total_percent_mem_req / node_count))

        local avg_percent_cpu_lim=$((total_percent_cpu_lim / node_count))
        local avg_percent_mem_lim=$((total_percent_mem_lim / node_count))

        local avg_percent_cpu_usage=$((total_percent_cpu_usage / node_count))
        local avg_percent_mem_usage=$((total_percent_mem_usage / node_count))

        echo -e "$(date +%s),AVG,NA,NA,${avg_percent_cpu_req},${avg_percent_mem_req},${avg_percent_cpu_lim},${avg_percent_mem_lim},${avg_percent_cpu_usage},${avg_percent_mem_usage}" >> "$output_file"

        # register the metrics every 2 seconds
        sleep 2

    done

}

# start the resource usage monitoring
kusage
