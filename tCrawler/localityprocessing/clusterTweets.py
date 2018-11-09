# -*- coding: utf-8 -*-

#import pymongo
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
#from datasketch import MinHash, MinHashLSH
from sklearn.cluster import KMeans

#myclient = pymongo.MongoClient("mongodb://localhost:27017/")
#mydb = myclient["social_crawler_db"]

#mycol = mydb["StreamRestMergedCollection"]

#cursor = mycol.find()
#count = mycol.count_documents({})
#doc_text_vector = {}
#index = 0
#for document in cursor: 
 #   doc_text_vector[index,0] = document['_id']
  #  doc_text_vector[index,1] = document['text']
   # index = index + 1


fig = plt.figure()  # create a new figure

data = np.genfromtxt('vectors.csv', delimiter='\t')
#print(data.shape)
data2 = data[:,0:-1]
#print(data2.shape)

# Number of clusters
kmeans = KMeans(n_clusters=36)
# Fitting the input data
kmeans = kmeans.fit(data2)
# Getting the cluster labels
labels = kmeans.predict(data2)
# Centroid values
centroids = kmeans.cluster_centers_

#print(centroids)
#print(centroids.shape)
#print(labels.shape)

np.savetxt("cluster_centroid.csv",centroids,newline = ' ',delimiter=',')
np.savetxt("centroid_label.txt",labels,newline = ' ',delimiter=',')
labels_uq, counts = np.unique(labels[labels>=0], return_counts=True)
np.savetxt("labels_uq.txt",labels_uq,newline = ' ',delimiter=',')
np.savetxt("counts.txt",counts,newline = ' ',delimiter=',')

ax = fig.add_subplot(1, 1, 1) 

ax.bar(labels_uq,counts)