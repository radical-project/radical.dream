#!/bin/bash

# set kubespray repo var
KUBE_REPO=https://github.com/kubernetes-sigs/kubespray.git

# get the python version
PYTHON_VERSION=$(python3 -c "import sys; print('.'.join(map(str, sys.version_info[:2])))")

# convert the python version to a number
NUM_PYTHON_VERSION=$(echo "$PYTHON_VERSION" | tr -d '.')

# https://github.com/kubernetes-sigs/kubespray/issues/10255
if [ "$NUM_PYTHON_VERSION" -eq 38 ]; then
    git clone --depth=1 -b "release-2.22" $KUBE_REPO

elif [ "$NUM_PYTHON_VERSION" -ge 39 ]; then
    git clone --depth=1 $KUBE_REPO

else
    echo "Not supported Python version: $PYTHON_VERSION" 1>> k8s_bootstrap.err
fi

KUBESPRAYDIR=$(pwd)/kubespray
VENVDIR="$KUBESPRAYDIR/.venv"

declare -a PIP=$(which pip3)
declare -a PYTHON=$(which python3)

# support both venv and virtualenv
if virtualenv --python=$PYTHON $VENVDIR; then
        echo "$VENVDIR is created with virtualenv" 1>> k8s_bootstrap.out
else
    echo "failed to create $VENVDIR with virtualenv" 1>> k8s_bootstrap.err

    # try to create venv
    if $(which python3) -m venv $VENVDIR; then
       echo "$VENVDIR is created with venv" 1>> k8s_bootstrap.out
    else
       echo "failed to create $VENVDIR with venv, exiting" 1>> k8s_bootstrap.err
       exit 1
    fi
fi

source $VENVDIR/bin/activate
cd $KUBESPRAYDIR

declare -a PIP=$(which pip3)
declare -a PYTHON=$(which python3)

# https://stackoverflow.com/q/34819221/5977059
$PIP install wheel
$(which python3) setup.py bdist_wheel

$PIP install -U -r requirements.txt 1>> k8s_bootstrap.out 2>> k8s_bootstrap.err

# copy the sample inventory definitions from the repo.
cp -rfp inventory/sample inventory/mycluster

while getopts m:u:k: flag
do
      case "${flag}" in
             m) map=${OPTARG};;
             u) user=${OPTARG};;
             k) key=${OPTARG};;
      esac
done

# number of control plane nodes
export KUBE_CONTROL_HOSTS=1

# set the nodes names and ips (from controller to workers)
declare -a MAP=($map)
CONFIG_FILE=inventory/mycluster/hosts.yml $PYTHON contrib/inventory_builder/inventory.py ${MAP[@]}

sed -i "s/\boverride_system_hostname: true\b/override_system_hostname: false/g" "roles/bootstrap-os/defaults/main.yml"

# start the ansible playbook
ansible-playbook -i inventory/mycluster/hosts.yml --private-key=$key -u $user --become cluster.yml  1>> k8s_bootstrap.out 2>> k8s_bootstrap.err

# setup the master node kube config
mkdir -p $HOME/.kube
sudo cp -i /etc/kubernetes/admin.conf $HOME/.kube/config
sudo chown $(id -u):$(id -g) $HOME/.kube/config
