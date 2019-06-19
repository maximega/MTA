import numpy as np
import pandas as pd
import matplotlib.pyplot as matplot
import sklearn
from sklearn.feature_extraction import DictVectorizer
from sklearn.metrics import pairwise_distances_argmin_min
from sklearn.metrics.pairwise import euclidean_distances
import matplotlib.pyplot as plt

from sklearn.cluster import KMeans

import json
from sklearn import preprocessing 

def createScatter(df):
    matplot.subplot(1,2,1)
    matplot.scatter(x=df['latitude'], y=df['longitude'], s=1)
    matplot.title('Lat vs Long')
    

def vectorizeAndCluster(data, labels, k):
    # convert dictionary to vector
    v = DictVectorizer(sparse=False)
    # running k means
    X = v.fit_transform(data)

    #error function to find optimal k
    # plotErrors(X)

    kmeans = KMeans(n_clusters=k)
    pred = kmeans.fit_predict(X)
    # print labels
    # printLabels(pred, labels, k)
    return [kmeans, X]

def getLabels(cats, kmeans):
    asc_order_centroids = kmeans.cluster_centers_.argsort()
    order_centroids = asc_order_centroids[:,::-1]
    labels = []
    for i in range(len(order_centroids)):
        list_arr = order_centroids[i].tolist()
        list_arr.remove(15)
        list_arr.remove(16)
        labels.append(cats[list_arr[0]])
    return labels

def printLabels(pred_classes, labels, k):
    for cluster in range(k):
        print('cluster: ', cluster)
        print(labels[np.where(pred_classes == cluster)])
            
            
def formatIntoVectors(dataf, k):
    # create list of object
    vector_arr = []
    labels = np.array([])
    for (idx, row) in dataf.iterrows():
        temp_obj = {}
        temp_obj['latitude'] = row['scaled_latitude']
        temp_obj['longitude'] = row['scaled_longitude']
        temp_obj['income'] = row['scaled_income']
        labels = np.append(labels, row['ntaname'])
        vector_arr.append(temp_obj)
        
    return vectorizeAndCluster(vector_arr, labels, k)
    
def scalePoints(df):
    # scale between 0-1
    min_max_scaler = preprocessing.MinMaxScaler()
    counts_minmax = min_max_scaler.fit_transform(df['x'].values.reshape(-1,1))
    counts_minmax = pd.DataFrame(counts_minmax)
    return counts_minmax

def scaleAllData(dataf):
    
    lat = pd.DataFrame(dataf['latitude'].values.tolist(), columns=['x'])
    lon = pd.DataFrame(dataf['longitude'].values.tolist(), columns=['x'])
    inc = pd.DataFrame(dataf['income'].values.tolist(), columns=['x'])

    lat_scaled = scalePoints(lat)
    long_scaled = scalePoints(lon)
    income_scaled = scalePoints(inc)
    
    dataf['scaled_latitude'] = lat_scaled[0].values.tolist()
    dataf['scaled_longitude'] = long_scaled[0].values.tolist()
    dataf['scaled_income'] = income_scaled[0].values.tolist()
    
    return dataf
    
def plotRealAndClustered(df, clusters):
    color_list = ['red', 'blue', 'green', 'yellow', 'orange', 'pink', 'lightblue']
    # for i in range(len(color_list)):
    #     print(labels[i] + " = " + color_list[i])
    colors = np.array(color_list)
    matplot.subplot(1,2,1)
    matplot.scatter(x=df['longitude'], y=df['latitude'], c=colors[clusters.labels_], s=10)
    matplot.title('Clusters')
    matplot.show()


def plotErrors(X):
    print("plotting errors")
    # ----------------- Error
    error = np.zeros(25)
    for k in range(1,25):
      kmeans = KMeans(init='k-means++', n_clusters=k, n_init=100)
      kmeans.fit(X)
      error[k] = kmeans.inertia_

    plt.scatter(range(1,len(error)),error[1:])
    plt.xlabel('Number of clusters')
    dummy = plt.ylabel('Error')
    plt.show()
    
def detectOutliers(df, labels, centroids, X):
    count = 0
    distances = []
    cluster_labels = []
    for (idx, row) in df.iterrows():
        dist = euclidean_distances([centroids[labels[count]]], [X[count]])[0][0]
        distances.append(dist)
        cluster_labels.append(labels[count])
        count += 1
    df['distances'] = distances
    df['cluster_labels'] = cluster_labels
    return df

def run_lat_long_kmeans(data, k):
    data = pd.DataFrame(data, columns=['ntaname', 'longitude', 'latitude', 'income'])

    data_scaled = scaleAllData(data) # scale lat and long points

    # print(data_scaled.tail())

    return formatIntoVectors(data_scaled, k)[0]

    # clusters = formatIntoVectors(data_scaled, k)[0]

    # plotRealAndClustered(data_scaled, clusters)

    # print(clusters.labels_)

    # X = formatIntoVectors(data_w_scaled, top_categories, cat_dict, k)[1]
    # labels = getLabels(top_categories, clusters)
    # plotRealAndClustered(data_w_scaled, clusters, labels)
    # return [data_w_scaled, clusters, X]
