# -*- coding: utf-8 -*-


import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
from sklearn.datasets.samples_generator import make_blobs
from sklearn.cluster import KMeans
from mpl_toolkits import mplot3d
#import matplotlib.pyplot as plt

#stdout()


shot_df = pd.read_csv('../../Dataset/nba-shot-logs/shot_logs.csv')
shot_df = shot_df[(shot_df['player_name'] == 'stephen curry')]
shot_df_clean = shot_df.dropna(axis = 0, how = 'all', subset = ['SHOT_CLOCK'])

##james harden
##lebron james
##chris paul
##stephen curry    -- 968rows

fig = plt.figure()
ax = plt.axes(projection='3d')

x = shot_df_clean['SHOT_DIST']
y = shot_df_clean['CLOSE_DEF_DIST']
z = shot_df_clean['SHOT_CLOCK']
ax.scatter(x, y, z, cmap='viridis', linewidth=0.5);
ax.set_title('stephen curry')
ax.set_xlabel('SHOT_DIST')
ax.set_ylabel('CLOSE_DEF_DIST')
ax.set_zlabel('SHOT_CLOCK')




df_kmeans = shot_df_clean[['SHOT_DIST', 'CLOSE_DEF_DIST', 'SHOT_CLOCK']]


kmeans = KMeans(n_clusters=4, random_state=0).fit(df_kmeans)


df_kmeans['cluster'] = kmeans.labels_



#plt.scatter(df_kmeans[:, 0], X[:, -1])

fig = plt.figure()
ax1 = plt.axes(projection='3d')

x = df_kmeans['SHOT_DIST']
y = df_kmeans['CLOSE_DEF_DIST']
z = df_kmeans['SHOT_CLOCK']
ax1.scatter(x, y, z, c = df_kmeans['cluster'],  cmap='viridis', linewidth=0.5);
ax1.set_title('stephen curry')
ax1.set_xlabel('SHOT_DIST')
ax1.set_ylabel('CLOSE_DEF_DIST')
ax1.set_zlabel('SHOT_CLOCK')

plt.scatter(kmeans.cluster_centers_[:, 0], kmeans.cluster_centers_[:, 1], kmeans.cluster_centers_[:, 2], c='red', marker='x')



shot_df_clean['cluster'] = kmeans.labels_


kmeans_rate = []
for i in range(4):
    rate = shot_df_clean[shot_df_clean['cluster'] == i]
    
    make = rate[(rate['SHOT_RESULT'] == 'made')]['SHOT_RESULT'].count()
    total = rate['SHOT_RESULT'].count()
    kmeans_rate.append(make / total)


print(kmeans.cluster_centers_)
