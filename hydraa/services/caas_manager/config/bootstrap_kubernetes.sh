#!/bin/bash

# install microk8s
sudo snap install microk8s --classic

# add your user to the microk8s group
sudo usermod -aG microk8s $USER

# create a .kube directory with:
mkdir ~/.kube

# give the new directory the necessary permissions with:
sudo chown -f -R $USER ~/.kube

# check microk8s status
sudo microk8s status

# the default ttl for Microk8s cluster to keep the historical event is 5m we increase it to 24 hours
sudo sed -i s/--event-ttl=5m/--event-ttl=1440m/ /var/snap/microk8s/current/args/kube-apiserver

sudo microk8s stop

sudo microk8s start

touch $HOME/kubectl
sudo echo -e "#!/bin/sh \n sudo microk8s kubectl $"@"" >  $HOME/kubectl
chmod +x $HOME/kubectl
sudo mv $HOME/kubectl /usr/local/bin

