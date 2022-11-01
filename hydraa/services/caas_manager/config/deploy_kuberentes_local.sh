#!/bin/bash

boot_kubernetes_ubuntu_PM () {

    #before install check if there is a kuberentes cluster
    REQUIRED_PKG="microk8s"
    PKG_OK=$(dpkg-query -W --showformat='${Status}\n' $REQUIRED_PKG|grep "install ok installed")
    echo Checking for $REQUIRED_PKG: $PKG_OK
    if [ "" = "$PKG_OK" ]; then
        echo "No $REQUIRED_PKG. Setting up $REQUIRED_PKG."
        sudo snap install $REQUIRED_PKG --classic --channel=1.18/stable
    fi

    # You need to configure your firewall to allow pod-to-pod and pod-to-internet communication
    sudo ufw allow in on cni0 && sudo ufw allow out on cni0

    sudo ufw default allow routed

    # Enable storage addons
    sudo microk8s enable dns dashboard storage

    # Access the cluster namespace
    sudo microk8s kubectl get all --all-namespaces

    # check the cluster resources
    sudo microk8s kubectl describe node

    sudo microk8s kubectl get all --all-namespaces

    sudo microk8s.status
}


boot_kubernetes_ubuntu_VM () {

    #before install check if there is a kuberentes cluster
    REQUIRED_PKG="microk8s"
    PKG_OK=$(dpkg-query -W --showformat='${Status}\n' $REQUIRED_PKG|grep "install ok installed")
    echo Checking for $REQUIRED_PKG: $PKG_OK
    if [ "" = "$PKG_OK" ]; then
        echo "No $REQUIRED_PKG. Setting up $REQUIRED_PKG."
        sudo snap install $REQUIRED_PKG --classic --channel=1.25
    fi

    sudo usermod -a -G microk8s $USER
    sudo chown -f -R $USER ~/.kube

    #sudo ufw allow in on cni0 && sudo ufw allow out on cni0

    #sudo ufw default allow routed

    # Access the cluster namespace
    sudo microk8s kubectl get all --all-namespaces

    # check the cluster resources
    sudo microk8s kubectl describe node

    sudo microk8s.status
}

if [  -n "$(uname -a | grep Ubuntu)" ]; then
    if [ "" = "$(systemd-detect-virt)"  ]; then
        echo "Physical Machine Detected"
        boot_kubernetes_ubuntu_PM
    
    else
        echo "Virtual Machine Detected"
        boot_kubernetes_ubuntu_VM
    fi

else
    echo "non ubuntu OS is not supported yet"
fi