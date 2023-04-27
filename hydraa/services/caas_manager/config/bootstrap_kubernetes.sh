#!/bin/bash

git clone --depth=1 https://github.com/kubernetes-sigs/kubespray.git

KUBESPRAYDIR=$(pwd)/kubespray
VENVDIR="$KUBESPRAYDIR/.venv"
ANSIBLE_VERSION=2.12

virtualenv  --python=$(which python3) $VENVDIR

source $VENVDIR/bin/activate
cd $KUBESPRAYDIR

pip install -U -r requirements-$ANSIBLE_VERSION.txt

cp -rfp inventory/sample inventory/mycluster

while getopts i:u:k: flag
do
      case "${flag}" in
             i) ips=${OPTARG};;
             u) user=${OPTARG};;
             k) key=${OPTARG};;
      esac
done

declare -a IPS=($ips)

CONFIG_FILE=inventory/mycluster/hosts.yml python3 contrib/inventory_builder/inventory.py ${IPS[@]}

ansible-playbook -i inventory/mycluster/hosts.yml --private-key=$key -u $user --become cluster.yml