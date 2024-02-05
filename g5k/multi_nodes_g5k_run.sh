#!/bin/bash 
export DIS_NPLBM=$(pwd)
export PATH="$PATH:$DIS_NPLBM:$DIS_NPLBM/terraform_0.13.5_linux_amd64:$DIS_NPLBM/linux-amd64"

cd terraform-provider-grid5000/examples/kubernetes;

terraform init;

# To modify :
# terraform-provider-grid5000/examples/kubernetes/main.tf 
# defautl : 
# nodes_count="6"
# site = "lyon"
# nodes_selector = "{cluster = 'taurus'}"
# walltime = "2"

terraform apply -auto-approve;

cd $DIS_NPLBM;
chmod +x linux-amd64/helm
chmod +x kubectl
chmod +x make_jar.sh
export KUBECONFIG=$DIS_NPLBM/terraform-provider-grid5000/examples/kubernetes/kube_config_cluster.yml

# To modify :
# help:  https://github.com/bitnami/charts/tree/main/bitnami/spark
# number of cpu= number of threads per node (-2) 
# memory of pod= memory of node
# worker.replicaCount= number of worker nodes

helm install spark-release spark/ --set master.resources.requests.cpu=30 --set master.resources.requests.memory=64Gi --set worker.resources.requests.cpu=30 --set worker.resources.requests.memory=64Gi --set worker.replicaCount=2

# spark-release-master-0 is the name of latest pod
# need to wait even you see "Error from server (NotFound): pods "spark-release-worker-3" not found"

while [[ $(kubectl get pods spark-release-master-0 -o 'jsonpath={..status.conditions[?(@.type=="Ready")].status}') != "True" ]]; do echo "waiting for pod" && sleep 1; done

# copy project in to master

kubectl cp ../../Distributed-NPLBM spark-release-master-0:/opt/bitnami/spark/
chmod +x apache-maven-3.9.1/bin/mvn
kubectl cp apache-maven-3.9.1/ spark-release-master-0:/opt/bitnami/spark/

# produce jar in master

kubectl exec spark-release-master-0 -- bash -c Distributed-NPLBM/tests/make_jar.sh

# copy NPLBM-1.0-jar-with-dependencies.jar to .


kubectl cp spark-release-master-0:/opt/bitnami/spark/Distributed-NPLBM/target/DisNPLBM-1.0-jar-with-dependencies.jar DisNPLBM-1.0-jar-with-dependencies.jar

# spark-release-worker-3 is the name of latest pod
# need to wait even you see "Error from server (NotFound): pods "spark-release-worker-3" not found"

while [[ $(kubectl get pods spark-release-worker-1 -o 'jsonpath={..status.conditions[?(@.type=="Ready")].status}') != "True" ]]; do echo "waiting for pod" && sleep 1; done

# To modify :
# copy tests to all workers and master
# kubectl cp ../tests spark-release-worker-?:/opt/bitnami/spark/ 

kubectl cp ../g5k spark-release-master-0:/opt/bitnami/spark/
kubectl cp ../g5k spark-release-worker-0:/opt/bitnami/spark/
kubectl cp ../g5k spark-release-worker-1:/opt/bitnami/spark/
#kubectl cp ../tests spark-release-worker-2:/opt/bitnami/spark/
#kubectl cp ../tests spark-release-worker-3:/opt/bitnami/spark/

# -----------------------------------------------------------------------------------------------------------------------
# spark-submit --class DAVID.Main \ -->																					|
# --master spark://spark-release-master-0.spark-release-headless.default.svc.cluster.local:7077 \ -->					|
# --num-executors 4 \ -->																								|
# --executor-memory 1G \ -->																							|
# --executor-cores 1 \ -->																								|
# --driver-memory 2G \ -->																								|
# --driver-cores 4 \ -->																								|
# --conf spark.jars.ivy=/tmp/.ivy \																						|
# local:/opt/bitnami/spark/tests/NPLBM-1.0-jar-with-dependencies.jar \ -->												|
# app parameters:																										|
# 	spark://spark-release-master-0.spark-release-headless.default.svc.cluster.local:7077 -> master url					|
# 	8 -> partitions																										|
# 	100 -> iterations																									|
# 	5.0 -> master_alphaprior pour DisNPLBM (i am not use it)															|
# 	5.0 -> worekr_alphaprior pour DisNPLBM (i am not use it)															|
# 	0 -> index of dataset line in the dataset_glob.csv file																|
# 	/opt/bitnami/spark/tests ->path of the folder where the dataset_glob.csv  file is located and the data/ folder 		|
# 	1 -> number of Launches																								|
# 	2 -> (1,0,2) (NPLBM,DisNPLBM,DisNPLBMRow)																			|
# 	1 -> number iterations master for DisNPLBM 																			|
# 	1 -> number iterations worker for DisNPLBM 																			|
# 	False -> shuffle																									|
# 	1 -> number of cores/task																							|
# -----------------------------------------------------------------------------------------------------------------------


kubectl exec spark-release-master-0 -- bash -c "spark-submit --class Coclustering.Main \
--master spark://spark-release-master-0.spark-release-headless.default.svc.cluster.local:7077 \
--num-executors 4 \
--executor-memory 1G \
--executor-cores 16 \
--driver-memory 10G \
--driver-cores 24 \
--conf spark.jars.ivy=/tmp/.ivy \
local:/opt/bitnami/spark/g5k/DisNPLBM-1.0-jar-with-dependencies.jar \
spark://spark-release-master-0.spark-release-headless.default.svc.cluster.local:7077  synthetic_20000_20_30 /opt/bitnami/spark/g5k 32 1 100 1 5.0 2.0"

# copy result from master to local(frome kubernetes to lyon node) machine

kubectl cp spark-release-master-0:/opt/bitnami/spark/g5k/results/ ../results/


cd $DIS_NPLBM/terraform-provider-grid5000/examples/kubernetes;

# free resources

terraform destroy -auto-approve
