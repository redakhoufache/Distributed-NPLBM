import sys
import re
import os
url=sys.argv[1]
file = open(url, "r")
ari=list()
nmi=list()
nbcluster=list()
for line in file:
    if re.search("^\(ari,", line):
        ari.append(line[5:-2])
    if re.search("^\(nmi,", line):
        nmi.append(line[5:-2])
    if re.search("^\(nCluster,", line):
        nbcluster.append(line[10:-2])

file.close()

# print(ari)
# print(nmi)
# print(nbcluster)

file = open("file_"+url.split("_")[2]+"_"+url.split("_")[3]+"_"+url.split("_")[4].split(".")[0]+"_scoresEachIteration.csv", "w")
file.write("ari,")
file.write(','.join([str(x) for x in ari]))
file.write("\nnmi,")
file.write(','.join([str(x) for x in nmi]))
file.write("\nnbcluster,")
file.write(','.join([str(x) for x in nbcluster]))
file.close()