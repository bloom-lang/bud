#!/bin/sh

sudo apt-get install rubygems
sudo apt-get install build-essential

sudo apt-get install ruby1.8-dev

rm -rf bud
svn co https://svn.declarativity.net/bud

cd bud

gem build bud.gemspec
sudo gem install bud-0.0.1.gem
cd ..

exit

sudo gem install treetop
sudo gem install parse_tree
sudo gem install ruby2ruby

