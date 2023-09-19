import sys
import numpy as np
from matplotlib import pyplot as plt

from sklearn.datasets import make_checkerboard, make_blobs
from sklearn.cluster import SpectralBiclustering
from sklearn.metrics import consensus_score
from scipy import stats
import matplotlib.pyplot as plt
from scipy.stats import multivariate_normal
import csv
import numpy as np, numpy.random
shuffled=bool(sys.argv[4])
true=sys.argv[1]
true=true.split(",")
true_rows=[int(x) for x in true[0].split("/")]
true_cols=[int(x) for x in true[1].split("/")]
data=sys.argv[2]
number_iterations=int(sys.argv[3])+2
with open(data,"r") as f:
    lines = f.readlines()
    rows=[int(x) for x in lines[number_iterations][30:-3].split(",")]
    cols=[int(x) for x in lines[number_iterations+1][30:-3].split(",")]
    f.close()
row_size=len(true_rows)
true_block=list()
flaten_true_rows=list()
for j in range(len(true_rows)):
    fl_true_rows=[j for x in range(true_rows[j])]
    flaten_true_rows.append(fl_true_rows)
flaten_true_cols=list()
for i in range(len(true_cols)):
    fl_true_col=[i for x in range(true_cols[i])]
    flaten_true_cols.append(fl_true_col)

flaten_true_cols=[item for sublist in flaten_true_cols for item in sublist]    
flaten_true_rows=[item for sublist in flaten_true_rows for item in sublist]
if shuffled:
    with open("../data/label_"+str(data.split("_")[2])+"_"+str(data.split("_")[3])+"_"+str(data.split("_")[4])+"_Shuffled.csv","r") as f:
        lines = f.readlines()
    flaten_true_cols=[int(x) for x in lines[1].split(",")]
    flaten_true_rows=[int(x) for x in lines[0].split(",")]

max_rows=max(rows)+1
max_cols=max(cols)+1
test_block=list()
for i in cols:
    for j in rows:
        cluster_id=i*max_rows+j
        test_block.append(cluster_id)
for i in flaten_true_cols:
    for j in flaten_true_rows:
        cluster_id=i*max_rows+j
        true_block.append(cluster_id)


import json
from sklearn.metrics import adjusted_rand_score, normalized_mutual_info_score,rand_score

def acc(labels_true, labels_pred):
    """
    References
    -------
    Yang, Yi, et al. "Image clustering using local discriminant models and global integration."
    IEEE Transactions on Image Processing 19.10 (2010): 2761-2773.
    """
    labels_true = np.array(labels_true)
    labels_pred = np.array(labels_pred)
    max_label = int(max(labels_pred.max(), labels_true.max()) + 1)
    match_matrix = np.zeros((max_label, max_label), dtype=np.int64)
    for i in range(labels_true.shape[0]):
        match_matrix[int(labels_true[i]), int(labels_pred[i])] -= 1
    indices = linear_sum_assignment(match_matrix)
    acc = -np.sum(match_matrix[indices]) / labels_pred.size
    return acc
lines[number_iterations+2]="(ariDis_NPLBMRow = "+str(adjusted_rand_score(true_block, test_block))+")\n"
lines[number_iterations+3]="(riDis_NPLBMRow = "+str(rand_score(true_block, test_block))+")\n"
lines[number_iterations+4]="(nmiDis_NPLBMRow = "+str(normalized_mutual_info_score(true_block, test_block))+")\n"
lines[number_iterations+5]="(nClusterDis_NPLBMRow="+str(max_cols*max_rows)+")\n"
with open(data,"w") as f:
    f.writelines(lines)
    f.close()