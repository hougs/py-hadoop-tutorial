#!/bin/bash

sudo apt-get install git
git clone https://github.com/jhlch/py-hadoop-tutorial.git

mkdir miniconda
cd miniconda
curl -O https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh
chmod +x Miniconda3-latest-Linux-x86_64.sh
./Miniconda3-latest-Linux-x86_64.sh
export PATH=/home/srowen/miniconda3/bin:$PATH
conda create --name pyhadoop pandas
source activate pyhadoop
pip install -r py-hadoop-tutorial/requirements.txt

