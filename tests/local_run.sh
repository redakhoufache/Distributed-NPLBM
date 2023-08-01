#!/bin/bash 
export DIS_NPLBM=$(pwd)

tar -xvf jdk-8u202-linux-x64.tar.gz;

sudo mv jdk1.8.0_202 /lib/jvm;
cd /lib/jvm;
sudo mv jdk1.8.0_202 jdk-8;
cd ;
sudo update-alternatives --install /bin/java java /lib/jvm/jdk-8/bin/java 1;
sudo update-alternatives --install /bin/javac javac /lib/jvm/jdk-8/bin/javac 1;
sudo update-alternatives --install /bin/jar jar /lib/jvm/jdk-8/bin/jar 1;
sudo update-alternatives --config java <<< "1";

cd $DIS_NPLBM;
./cs install scala:2.12.15 && ./cs install scalac:2.12.15
export PATH="$PATH:$HOME/.local/share/coursier/bin:$DIS_NPLBM/apache-maven-3.9.1/bin"
cd ../;
mvn package -DskipTests ;
cd target;

scala -J-Xmx32g -J-XX:ActiveProcessorCount=16 ./NPLBM-1.0-jar-with-dependencies.jar local[16] 32 10 5.0 5.0 0 $DIS_NPLBM 1 2 1 1 False 1
