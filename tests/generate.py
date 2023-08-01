from sklearn.datasets import make_checkerboard, make_blobs
from sklearn.cluster import SpectralBiclustering
from sklearn.metrics import consensus_score
from scipy.stats import multivariate_normal
import numpy as np, numpy.random
import sys

observations=int(sys.argv[1])
features=int(sys.argv[2])
cluster=(int(sys.argv[3]),int(sys.argv[4]))
X, y ,modes= make_blobs(n_samples=25000, centers=cluster[0]*cluster[1], n_features=1,random_state=0,return_centers= True)
print("centers=",len(modes))
print(type(modes))
data_shape=(observations,features)

nb_row,nb_colums=data_shape
while(True):
    row_sizes=np.random.multinomial(nb_row, [1/float(cluster[0])]*cluster[0], size=1)[0]
    if (0 in row_sizes)==False:
        break
while(True):
    col_sizes=np.random.multinomial(nb_colums, [1/float(cluster[1])]*cluster[1], size=1)[0]
    if (0 in col_sizes)==False:
            break
print(row_sizes)
print(col_sizes)
d = [1, 1]
A = np.diag(d)
dim=modes[0].size
data=list()
membersip=list()
for i in range(cluster[0]):
    row_block=list()
    row_block_membersip=list()
    for j in range(cluster[1]):
        block_size=col_sizes[j]*row_sizes[i]
        rv = multivariate_normal.rvs(mean=modes[i*cluster[1]+j], cov=(0.5+(2.0-0.5)*np.random.random()),size=block_size)
        x = np.arange(block_size, dtype=int)
        block_membersip=np.full_like(x, (i*cluster[1]+j), dtype=int,shape=(row_sizes[i],col_sizes[j]))
        rv=rv.reshape(row_sizes[i],col_sizes[j],dim)
        row_block.append(rv)
        row_block_membersip.append(block_membersip)
    data.append(np.concatenate(row_block, axis=1))
    membersip.append(np.concatenate(row_block_membersip, axis=1))
data=np.concatenate(data, axis=0)
membersip=np.concatenate(membersip, axis=0)
print(data.shape)
print(membersip.shape)
with open("data/synthetic_"+str(data.shape[0])+"_"+str(data.shape[1])+"_"+str(cluster[0]*cluster[1])+".csv", 'w') as f:
    f.write(','.join([str(x) for x in range(data.shape[1])]))
    f.write("\n")
    for i in range(data.shape[0]):
        line_str=list()
        for j in range(data.shape[1]):
            line_str.append(':'.join([str(x) for x in data[i][j].tolist()]))
        line=(','.join([y for y in line_str]))
        f.write(line)
        f.write("\n")
datasets=""
col_size_str='/'.join([str(y) for y in col_sizes])
row_size_str='/'.join([str(y) for y in row_sizes])
datasets+="synthetic_"+str(data_shape[0])+"_"+str(data_shape[1])+"_"+str(cluster[0]*cluster[1])+".csv"+","+row_size_str+","+col_size_str+"\n"
file=open("dataset_glob.csv","a")
file.write(datasets)
file.close()
