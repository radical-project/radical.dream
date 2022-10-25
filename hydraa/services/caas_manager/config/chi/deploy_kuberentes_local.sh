#!/bin/bash

boot_kubernetes_ubuntu () {
              # install Kubernetes on Ubuntu only via snap
              sudo snap install microk8s --classic --channel=1.18/stable

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
}


if [  -n "$(uname -a | grep Ubuntu)" ]; then
    boot_kubernetes_ubuntu
else
    echo "non ubuntu OS is not supported"
fi

