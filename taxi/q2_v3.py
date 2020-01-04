import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os


root_path = '../../data/taxi/2014-07-01'

"""
above could be changed
"""
def makedir(path):
    isExists=os.path.exists(path)
    if not isExists:
        os.makedirs(path)
        print("folder create success!")
        return True
    else:
        print("folder is exist!")
        return False

for f in os.listdir(root_path):
    if f=='table_export.txt':
        continue
    df = pd.read_table(os.path.join(root_path, f), sep='\t', encoding='GBK')
    print(df[["CLBH", "JQBH", "JD", "WD"]].head(10))

    df1 = df[["CLBH", "JQBH", "JD", "WD"]].drop_duplicates().dropna()
    df1 = df1[(df1['JQBH']=='重车$')|(df1['JQBH']=='空车$')]
    df1 = df1[(df1['JD'] < 119)&(df1['WD'] < 25)&(df1['JD'] > 116)&(df1['WD'] > 24)]
    d_car = {}
    for i in range(len(df1)):
        CLBH = df1['CLBH'].iloc[i]
        JQBH = df1['JQBH'].iloc[i]
        JD = df1['JD'].iloc[i]
        WD = df1['WD'].iloc[i]
        if CLBH in d_car:
            last_JQBH = d_car[CLBH][-1][2]
            if last_JQBH != JQBH:
                d_car[CLBH].append([WD, JD, JQBH])
        else:
            d_car[CLBH] = [[WD, JD, JQBH]]
    plt.figure(figsize=(15,15))
    for CLBH in d_car:
        plt.plot([d_car[CLBH][0][1], d_car[CLBH][-1][1]], [d_car[CLBH][0][0], d_car[CLBH][-1][0]], '--',
                  linewidth=0.2, color='gray')
    for CLBH in list(d_car.keys())[:-1]:
        plt.scatter(d_car[CLBH][0][1], d_car[CLBH][0][0], c='r')
        plt.scatter(d_car[CLBH][-1][1], d_car[CLBH][-1][0], c='b')
    CLBH = list(d_car.keys())[-1]
    plt.scatter(d_car[CLBH][0][1], d_car[CLBH][0][0], c='r', label='start')
    plt.scatter(d_car[CLBH][-1][1], d_car[CLBH][-1][0], c='b', label='end')
    plt.xlim(117.8, 118.4)
    plt.ylim(24.4, 24.76)
    plt.xticks(np.arange(117.8, 118.4, 0.02))  # 设置x刻度
    plt.yticks(np.arange(24.4, 24.76, 0.012))  # 设置y刻度
    plt.grid(True,linestyle = "--",color = 'gray' ,linewidth = '0.1',axis='both')
    plt.title(f.split('.')[0])
    plt.legend()
    new_dir="%s-q2-figs"%(f.split('.')[0][:-2]) #2014-07-01-q2
    makedir(new_dir)
    save_path="%s/%s.png"%(new_dir,f.split('.')[0])
    plt.savefig(save_path)
    plt.show()
    print("save & show %s finished "%(f.split('.')[0]+'.png'))