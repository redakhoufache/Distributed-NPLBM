# DisNPLBM: Distributed Non-Parametric Latent Block Model

This repository contains our implementation of DisNPLBM proposed in the paper "Distributed MCMC inference for Bayesian Non-Parametric Latent Block Model" (accepted to The Pacific-Asia Conference on Knowledge Discovery and Data Mining (PAKDD 2024)).
## Local run
### Requirements

* Java (jdk-8u202)
* Scala (2.12.15)
  
### Build the program

The script build.sh is provided to build an executable jar containing all the dependencies. 
Use the following command to build it: 

```
/bin/bash build.sh
```

### Run the program

In order to run the built jar use the following code:

```
scala -J-Xmx1024m ./target/DisNPLBM-1.0-jar-with-dependencies.jar local[*] <dataset name> <path to data> <partition number> 1 <number of iterations> <dimension of observation space> <concentration parameter $\alpha$> <concentration parameter $\beta$>
```

Example of execution:

```
scala -J-Xmx1024m ./target/DisNPLBM-1.0-jar-with-dependencies.jar local[*] synthetic_100_100_9 $DATAPATH 4 1 100 1 5.0 2.0
```
The above code will perform  100 iterations on synthetic_100_100_9 dataset (provided in data file) on local mode with 4 partitions.

## Multi nodes run (on grid5000 cluster)

## Outputs

## Visualization
![Clusters visualization](results/coclustExample.png)
