a.) add 'apt_preserve_sources_list: true' to /etc/cloud/cloud.cfg
##     or do the same in user-data
## b.) add sources in /etc/apt/sources.list.d
## c.) make changes to template file /etc/cloud/templates/sources.list.tmpl
#

#!/bin/sh
cd ~/Downloads

# copy data to server
# scp -i ~/Downloads/alee7135.pem -r ~/Documents/Zipfian/FINAL/yelp/ ec2-user@ec2-54-172-94-166compute-1.amazonaws.com:

# login to server
# ssh -t -t -i "alee7135.pem" ec2-user@ec2-54-174-112-59.compute-1.amazonaws.com 'echo "rootpass" | sudo -Sv && bash -s' < package_install2.sh

ssh -t -t -i "alee7135.pem" ec2-user@ec2-54-174-112-59.compute-1.amazonaws.com <<'ENDSSH'
sudo yum update

sudo yum install python27

curl -O https://bootstrap.pypa.io/get-pip.py

sudo python27 get-pip.py

sudo ln -s /usr/local/bin/pip /usr/bin/
sudo yum -y install gcc-c++ python27-devel atlas-sse3-devel lapack-devel
wget https://pypi.python.org/packages/source/v/virtualenv/virtualenv-1.11.2.tar.gz
tar xzf virtualenv-1.11.2.tar.gz
python27 virtualenv-1.11.2/virtualenv.py sk-learn
. sk-learn/bin/activate
pip install numpy

sudo pip install ipython
sudo pip install beautifulsoup4
sudo pip install pymongo
sudo yum install emacs
sudo pip install oauth2
sudo pip install boto

ENDSSH
scp -i alee7135.pem results/standard ec2-user@ec2-54-172-94-166.compute-1.amazonaws.com:
mongoimport --db yelp --collection businesses --file standard

exit 0
# cd yelp/code
# sudo mkdir -p /data/db
# screen -S mongo
# screen -S mongo -d -X sudo mongod
# ipython get_restaurants.py
# screen -S run -d -m ipython yelp_scraper_V2.py 100 200


# ssh -i "alee7135.pem" ec2-user@eec2-54-86-97-110.compute-1.amazonaws.com
