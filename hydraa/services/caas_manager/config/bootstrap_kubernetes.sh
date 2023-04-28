#!/bin/bash

set -x #echo on

git clone --depth=1 https://github.com/kubernetes-sigs/kubespray.git

KUBESPRAYDIR=$(pwd)/kubespray
VENVDIR="$KUBESPRAYDIR/.venv"
ANSIBLE_VERSION=2.12

virtualenv  --python=$(which python3) $VENVDIR

source $VENVDIR/bin/activate
cd $KUBESPRAYDIR

pip install -U -r requirements-$ANSIBLE_VERSION.txt

# copy the sample inventory definitions from the repo.
cp -rfp inventory/sample inventory/mycluster

while getopts i:u:k: flag
do
      case "${flag}" in
             i) ips=${OPTARG};;
             u) user=${OPTARG};;
             k) key=${OPTARG};;
      esac
done

# number of control plane nodes
export KUBE_CONTROL_HOSTS=1

# set the nodes ips (from controller to workers)
declare -a IPS=($ips)
CONFIG_FILE=inventory/mycluster/hosts.yml python3 contrib/inventory_builder/inventory.py ${IPS[@]}

sed -i "s/\boverride_system_hostname: true\b/override_system_hostname: false/g" "roles/bootstrap-os/defaults/main.yml"

# invoke the ansible playbook
ansible-playbook -i inventory/mycluster/hosts.yml --private-key=$key -u $user --become cluster.yml  1>> ansible.out 2>> ansible.err