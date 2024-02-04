import sys
import re
import os
url=sys.argv[1]
file = open(url, "r")
likelihood=list()
for line in file:
     if re.search("likelihood", line):
         likelihood.append(line[14:-2])
file.close()
file = open("file_"+url.split("_")[2]+"_"+url.split("_")[3]+"_"+url.split("_")[4].split(".")[0]+"_likelihood.csv", "w")
file.write(','.join([str(x) for x in likelihood]))
file.close()
