# DisNPLBM tests

There are many environments to test the **DisNPLBM** algorithm with synthetic data sets.
Ensure you have downloaded the correct versions of **Java (jdk-8u202)**, **Scala (2.12.15)**, and **Terraform (0.13.5)**.
All the tests are made in Linux Os; this document doesn't avoid necessary configuration to Mac or Windows.


## Requires

you need to download :

 - Java  : [(jdk-8u202)](https://www.oracle.com/java/technologies/javase/javase8-archive-downloads.html) [jdk-8u202-linux-x64]
 - cs : 
 ```
 curl -fL https://github.com/coursier/coursier/releases/latest/download/cs-x86_64-pc-linux.gz | gzip -d > cs && chmod +x cs
 ```
 - Terraform  : [0.13.5](https://releases.hashicorp.com/terraform/0.13.5/) [terraform_0.13.5_linux_amd64]
```
curl https://releases.hashicorp.com/terraform/0.13.5/terraform_0.13.5_linux_amd64.zip --output terraform_0.13.5_linux_amd64.zip
```
```
unzip terraform_0.13.5_linux_amd64.zip -d terraform_0.13.5_linux_amd64
```

Move them all to /tests folder




## Data generation

We provide python code to generate synthetic data using **Gaussian normal distribution** case, this data is generated using block format, the size of each block is random with `sum_row = [number of observations]` and `sum_colmun = [number of features]`, more detail in `dataset_glob.csv`.

You need to create folder named `data` in the `tests/` folder by :
```
mkdir data
```
To generate data, you need to call `python3 generate.py [number of observations] [number of features] [number of clusters row] [number of clusters column] [dimension]`.

You need to have **scikit-learn**, **scipy** and **numpy** installed.

You can just run bash code `generate_data_run.sh` to generate 20k x 20 dataset with 10 row clusters and 4 column clusters.

Each dataset is added to `/data` folder and its index in `dataset_glob.csv`.
Example :
`synthetic_1000000_20_40.csv,99675/100134/99725/100156/99981/100295/100378/99429/100123/100104,7/5/3/5`
Dataset name is `synthetic_1000000_20_40.csv` and the sizes of row clusters are  `99675/100134/99725/...` and the sizes of column clusters are `7/5/3/5`. The first block cluster has size `99675*7` and second one `99675*5`... 
## Local Machine test

In local machine, you can use multi thread environment using this `local_run.sh`.
To precise the number of cores used in spark environment, you need to change `local[?]` in `local_run.sh`.
To precise the number of cores used in Java virtual machine (JVM) environment, you need to change `-J-XX:ActiveProcessorCount=?` and for memory `-J-Xmx?g`.
The algorithm parameters are :

 - `local[16]` : spark master 
 - `32 `: number of workers (partitions)
 - `10` : iterations
 - `5.0 `: master_alphaprior pour DisNPLBM (it's unused now)
 - `5.0` : worekr_alphaprior pour DisNPLBM (it's unused now)
 - `0 `: index of dataset line in the dataset_glob.csv file 
 - `$DIS_NPLBM`:  path of the folder where the `dataset_glob.csv`  file is located and the `/data` folder
 - `1`: number of Launches
 - `2` : chose algorithm (NPLBM,DisNPLBM,DisNPLBMRow)
 - `1` : number iterations master for DisNPLBM
 - `1 `: number iterations worker for DisNPLBM
 - `False`:  shuffle data
 - `1`: number of cores/task for spark configuration
 - `1`: dimension
 - `False`:  verbose mode
 - `False`:  Print Scores (Ari,Ri,NMi and nbCluster) each iteration
 - `False':  Compute the Likelihood each iteration

Scala command:
 ```
 scala -J-Xmx32g -J-XX:ActiveProcessorCount=16 ./NPLBM-1.0-jar-with-dependencies.jar local[16] 32 10 5.0 5.0 0 $DIS_NPLBM 1 2 1 1 False 1 1 False False Flase

 ```

## One Node test Grid5000

In one node test Grid5000 machine, you can use multi thread environment using this `local_run_g5k.sh`.
To run the experiment you need to generate data by modifying `generate_data_run.sh` and execute the job in passive way.
```
oarsub -t -l walltime=00:10 "/bin/bash generate_data_run.sh"
```

## Multi Nodes test Grid5000

For multi nodes test, we prefer to use Kubernetes cluster with standalone mode , you can use Yarn or  Mesos cluster with some code modification.
To deploy Kubernetes cluster in Grid5000, we chose to use **Terraform** with **helm**. For more details, you can take look in [terraform-provider-grid5000](https://github.com/pmorillon/terraform-provider-grid5000) and  [helm spark chart](https://github.com/bitnami/charts/tree/main/bitnami/spark). 
We have made spark docker image modification in `spark/values.yaml`.
```
image:
  registry: docker.io
  repository: bitnami/spark
  tag: 3.3.0-debian-11-r10
  ...
``` 
You have to give the execution permission to `multi_machines_g5k_run.sh` and `generate_data_run.sh`.
You must change `terraform-provider-grid5000/examples/kubernetes/main.tf` to chose number of nodes, site name and other input parameters [details ](https://registry.terraform.io/modules/pmorillon/k8s-cluster/grid5000/latest?tab=inputs).
Example in Grenoble site using `dahu` clusters:
```
module "k8s_cluster" {
    source = "pmorillon/k8s-cluster/grid5000"
    version = "~> 0.0.1"
    nodes_count="6"
    site = "grenoble"
    nodes_selector = "{cluster = 'dahu'}"
    walltime = "4"
}
```
The CPU resource is measured in _CPU_ units. One CPU, in Kubernetes, is equivalent to:

-   1 AWS vCPU
-   1 GCP Core
-   1 Azure vCore
-   1 Hyperthread on a bare-metal Intel processor with Hyperthreading

By choosing Intel CPU, Kubernetes CPU unit will be threads, that why in `helm install`, we ask number of thread in physical node. (`-2`) for Kubernetes management overheads.
```
helm install spark-release spark/ \
--set master.resources.requests.cpu=60 \
--set master.resources.requests.memory=64Gi \
--set worker.resources.requests.cpu=60 \
--set worker.resources.requests.memory=64Gi \
--set worker.replicaCount=9
```
The `dahu` node has `Intel Xeon Gold 6130 (Skylake, 2.10GHz, 2 CPUs/node, 16 cores/CPU)` with `64 threads`, that why we select  `master.resources.requests.cpu=60` and `worker.resources.requests.cpu=60` and we ensure that there is only one pod per node. You can get this information by executing the command:
```
kubectl get pod -o wide
```
You need to modify `multi_machines_g5k_run.sh` for your configuration.

## Scoring process
We examine the quality of our algorithms using three clustering metrics [**Adjusted Rand Score** (ARi)](https://scikit-learn.org/stable/modules/generated/sklearn.metrics.adjusted_rand_score.html#sklearn.metrics.adjusted_rand_score), [**Rand Score** (Ri)](https://scikit-learn.org/stable/modules/generated/sklearn.metrics.rand_score.html#sklearn.metrics.rand_score) , and [**Adjusted Mutual Info Score**(NMi)](https://scikit-learn.org/stable/modules/generated/sklearn.metrics.adjusted_mutual_info_score.html#sklearn.metrics.adjusted_mutual_info_score).
After having the result of co-clustering in `tests/result`, you may use `scors.py [true_partitions] [result file name] [number of iterations]`.
```
python3 scors.py 1896/2012/2022/1976/2061/2008/2026/1995/1977/2027,1/8/5/6 file_synthetic_20000_20_40.csv_32_Dis_NPLBMRow.out 10
```
You will find result at the end of result file.
## Video demo

 1. [Local_run ](https://youtu.be/BhfYNq7LaXs)
 2. [Local_run_g5k](https://youtu.be/yzcM79us8_I)
 3. [multi_machines_g5k_run](https://youtu.be/sa6FW7-d544)
