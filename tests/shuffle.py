import sys
import matplotlib.pyplot as plt
import numpy as np
url=sys.argv[1]
with open("data/"+url,"r") as f:
    lines = f.readlines()
lines=lines[1:]

mat=[x.split(",") for x in lines]
data=np.array([np.array(x).astype(float) for x in [y for y in mat]]) 
rng = np.random.RandomState(0)
row_idx = rng.permutation(data.shape[0])
col_idx = rng.permutation(data.shape[1])
print(row_idx)
print(col_idx)
data = data[row_idx][:, col_idx]
with open("data/synthetic_"+str(url.split("_")[1])+"_"+str(url.split("_")[2])+"_"+str(url.split("_")[3].split(".")[0])+".csv", 'w') as f:
    f.write(','.join([str(x) for x in range(data.shape[1])]))
    f.write("\n")
    for i in range(data.shape[0]):
        line_str=list()
        line=(','.join([str(y) for y in data[i]]))
        f.write(line)
        f.write("\n")
with open("data/synthetic_"+str(url.split("_")[1])+"_"+str(url.split("_")[2])+"_"+str(url.split("_")[3].split(".")[0])+"_labels.csv", 'w') as f:
    f.write(','.join([str(x) for x in row_idx]))
    f.write("\n")
    f.write(','.join([str(x) for x in col_idx]))
    f.write("\n")
