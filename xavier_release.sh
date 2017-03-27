#!/bin/bash
set -ex

if [[ "$1" == "" ]]; then
	echo "Usage: ./xavier_release.sh TAG"
	exit 1
fi

TAG=$1 # 1.5-cinder_v2_api-os-verbose-b4

cd /Users/xlange/code/viasat/workspace/src/k8s.io/kubernetes

bash build/run.sh make cross KUBE_FASTBUILD=true ARCH=amd64
cd /Users/xlange/code/viasat/workspace/src/k8s.io/kubernetes/cluster/images/hyperkube/
make VERSION=$TAG  ARCH=amd64
docker tag gcr.io/google_containers/hyperkube-amd64:$TAG registry.usw1.viasat.cloud/hyperkube-amd64:$TAG
docker push registry.usw1.viasat.cloud/hyperkube-amd64:$TAG