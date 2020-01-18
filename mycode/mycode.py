import networkx as nx
from numpy import *
import numpy as np
import time
import copy
import sys
import matplotlib.pyplot as plt


lamda = 0.3      #相似度阈值（0-1）

### 构造图，存储其节点间关系
def create_Graph(path):
    #print("读取路径： "+path)
    G = nx.Graph()
    fileContent = open(path, "rb")
    with fileContent as f:
        for row in f:
            u, v= [i for i in row.decode().split('\t', -1)]
            u = int(u)
            v = int(v)
            G.add_edge(u, v)
    return G



###写入结果到文件
def writeOutFile(community, outputPath):
    outFile = open(outputPath, "w")
    outFile.write(str(sorted(community)))
    outFile.close()


### 将节点按照度数从小到大排序
def sort_by_degree(G):
    nodes = {}
    for n in G.nodes:
        d = G.degree(n)
        nodes[n] = d
    nodes = sort_by_value(nodes)
    return nodes

def sort_by_value(d):
    items=d.items()
    backitems=[[v[1],v[0]] for v in items]
    backitems.sort()
    return [ backitems[i][1] for i in range(0,len(backitems))]




### 获取社区的邻居集
def get_com_neighbors(G, community):
    nei_set = set()
    for i in community:
        # nei_set.add(i)
        nei_set |= set(G.neighbors(i))
    nei_set -= community
    return nei_set


### 计算节点的jaccard相似系数
def get_jaccard(G,u,v):
    nei_u = set(G.neighbors(u))
    nei_v = set(G.neighbors(v))
    sim = float(len(nei_u & nei_v)) / len(nei_u | nei_v)
    return sim


### 获取社区的边缘节点   !!!时间复杂度
def findBoundaryNode(G,community):
    boundaryNodeSet = set()
    Com_graph = G.subgraph(community)
    for n in Com_graph.nodes:
        if G.neighbors(n)-Com_graph.nodes:
            boundaryNodeSet.add(n)
    return boundaryNodeSet




def get_com_tightness(G,community):
    in_tightness = 0
    out_tightness = 0
    bdyNodeSet = findBoundaryNode(G,community)
    Com_graph = G.subgraph(community)

    for (u,v) in Com_graph.edges:   #内部紧密度
        in_tightness += get_jaccard(G,u,v)
    #print("neibu"+str(in_tightness))

    for b in bdyNodeSet:   #外部紧密度
        exNei=list()
        exNei=G.neighbors(b)-Com_graph.nodes
        for l in exNei:
            out_tightness += get_jaccard(G,b,l)
    #print("waibu"+str(out_tightness))
    if ( out_tightness==0):
        tightness=0
    else:
        tightness = float(in_tightness) / ( out_tightness)  #社区紧密度
    return tightness



###  计算社区的局部模块度密度QL
def get_QL_comm(G,community):
    indD,dD,outD,QL=0,0,0,0
    if (len(community)==1):
        QL=0
    else:
      Com_graph = G.subgraph(community)
      Pc=float(2*Com_graph.number_of_edges()) / (Com_graph.number_of_nodes()*(Com_graph.number_of_nodes()-1))  #密度系数pc
      #print(Pc)
      for n in Com_graph:
          indD += Com_graph.degree(n)  #内部度indD
          dD += G.degree(n)  #度dD
          outD = dD-indD  #外部度outD
      com_Nei=get_com_neighbors(G,community)
      QL = float((Pc*indD/dD)-((outD/dD/Pc)**2))
    return QL




###  计算节点社区的局部模块度密度QL
def get_QL(G,community,i):
    indD,dD,outD,tot_CinV,avg_CinV,QL=0,0,0,0,0,0
    if (len(community)==1):
        QL=0
    else:
      Com_graph = G.subgraph(community)
      Pc=float(2*Com_graph.number_of_edges()) / (Com_graph.number_of_nodes()*(Com_graph.number_of_nodes()-1))  #密度系数pc
      #print(Pc)
      for n in Com_graph:
          indD += Com_graph.degree(n)  #内部度indD
          dD += G.degree(n)  #度dD
          outD = dD-indD  #外部度outD
      com_Nei=get_com_neighbors(G,community)
      tot_CinV = compute_CinV(G,community,i)
      QL = float((Pc*indD/dD)-((outD/dD/Pc)**2)+(tot_CinV))
    return QL




def compute_CinV(G,community,node):
    interNei=set()
    interNei=set(G.neighbors(node)) & community #节点node的社区内部邻居集
    #print(interNei)
    interNei_graph = G.subgraph(interNei) #节点node的社区内部邻居集中的节点所构成的边
    #print(interNei_graph.edges)
    if (interNei_graph.number_of_nodes()==1):
        cinV=0
    else:
        cinV = float (2*interNei_graph.number_of_edges()) / ((interNei_graph.number_of_nodes())*(interNei_graph.number_of_nodes()-1))
    #print(cinV)
    return cinV



###  计算节点的相似度
def get_simNode(G,u,v):
    nei_u = set(G.neighbors(u))
    nei_v = set(G.neighbors(v))
    simNode = float(len(nei_u & nei_v)) / min(len(nei_u),len(nei_v))
    return simNode


###  计算节点与社区的相似度
def get_simCom(G,u,community):
    nei_u = set(G.neighbors(u))
    nei_com = set(get_com_neighbors(G,community))
    simCom = float(len(nei_u & nei_com)) / min(len(nei_u),len(nei_com))
    return simCom




#使用deltatight以及deltaQL:
def run(inputPath,outputPath,seed):

    start_time = time.clock()

    G = create_Graph(inputPath)
    community_ori=set() #初始社区
    community_ori.add(seed)
    #print("上面是只有初始节点的社区： "+str(community_ori))


    flag = True
    while (flag):
        com_Neibghbors=get_com_neighbors(G,community_ori)
        #start_com_tight = get_com_tightness(G, community_ori)  # 初始时刻社区紧密度
        tight = get_com_tightness(G, community_ori)  # 初始时刻社区紧密度

        # com_ori_QL = get_QL(G, community_ori)
        candicate = set()  # 每次while都要清空
        for n in com_Neibghbors:
            #temp_tight=0
            com_tight_temp=copy.deepcopy(community_ori)
            com_tight_temp.add(n)
            temp_tight=get_com_tightness(G,com_tight_temp)
            delta_temp_tight=temp_tight-tight
            #print(delta_temp_tight)
            if (delta_temp_tight >= 0):
                candicate.add(n)
            com_tight_temp.clear()
        #print(candicate)

        if (len(candicate) != 0):  ##如果等于怎么办！！！
            node_deltaQL=dict()
            maxQL = set()
            for m in candicate:
                temp_QL = get_QL(G,community_ori,m)
                node_deltaQL.update({m:temp_QL})
                temp_QL = 0
            max_deltaQL_value = max(node_deltaQL.values())
            for key, value in node_deltaQL.items():
                if (value == max_deltaQL_value):
                    maxQL.add(key)
            #print(maxQL)
            community_ori = community_ori | maxQL
            #print("上面是添加节点后的社区： " + str(community_ori))
            maxQL.clear()
        else:
            flag = False
        #此时新一轮的循环 社区中应该包含前面添加的元素
        candicate.clear()
        end_com_tight = get_com_tightness(G,community_ori)
        if end_com_tight > 1:
            flag=False

    #————————————————社区扩展——————————————


    flag = True
    while (flag):
        com_Neibghbors_expand = get_com_neighbors(G, community_ori)
        tight_expand = get_com_tightness(G, community_ori)  # 初始时刻社区紧密度

        # com_ori_QL = get_QL(G, community_ori)
        candicate_expand = set()  # 每次while都要清空
        for n in com_Neibghbors_expand:
            com_tight_temp_expand = copy.deepcopy(community_ori)
            com_tight_temp_expand.add(n)
            temp_tight_expand = get_com_tightness(G, com_tight_temp_expand)
            delta_temp_tight_expand = temp_tight_expand - tight_expand
            # print(delta_temp_tight)
            if (delta_temp_tight_expand >= 0):
                candicate_expand.add(n)
            com_tight_temp_expand.clear()


        if (len(candicate_expand) != 0):  ##如果等于怎么办！！！
            node_sim = dict()
            maxadd = set()
            wait_for_add1=set()
            for m in candicate_expand:
                simNodeCom = get_simCom(G, m, community_ori)
                #print(simNodeCom)
                if (simNodeCom >= lamda):
                    node_sim.update({m: simNodeCom})
        else:
            break


        if (len(node_sim)!=0):
            max_deltaQL_value_expand = max(node_sim.values())
            for key, value in node_sim.items():
                if (value == max_deltaQL_value_expand):
                    maxadd.add(key)
            # print(maxQL)
            community_ori = community_ori | maxadd
            # print("上面是添加节点后的社区： " + str(community_ori))
            maxadd.clear()
        else:
            flag = False
        # 此时新一轮的循环 社区中应该包含前面添加的元素
        candicate_expand.clear()
        end_com_tight1 = get_com_tightness(G, community_ori)
        if ((end_com_tight1 < 1)&(len(candicate_expand) == 0)) :
            flag = False



    # # ————————————————社区优化——————————————
    add_core_node_set = set()
    node_degree_dict = {}
    NBSet = set(get_com_neighbors(G,community_ori))
    for node in NBSet:
        node_neighbors = set(G.neighbors(node))
        node_k_in = len(node_neighbors & community_ori)  # c.nodes 会不断的减小
        dep = float(node_k_in) / float(G.degree(node))  # 社区影响力tight
        if (dep >= lamda) :  # 有问题
            community_ori.add(node)

    community_temp=copy.deepcopy(community_ori)
    for i in community_temp:
        if i == seed:
            continue
        else:
            com_delete_QL = get_com_tightness(G, community_ori)
            com_delete_temp= copy.deepcopy(community_ori)
            com_delete_temp.remove(i)
            temp_delete_QL = get_com_tightness(G, com_delete_temp)
            delta_delete_QL = temp_delete_QL - com_delete_QL
            sub_graph = G.subgraph(com_delete_temp)
            num_of_sub=len(list(nx.connected_components(sub_graph)))
            if (delta_delete_QL > 0 and num_of_sub==1):
                community_ori.remove(i)
    community_temp.clear()





    end_time = time.clock()
    community_ori=str(community_ori)

    file = open(outputPath, 'a')
    file.write(community_ori+"\n")
    file.close()
    #print(str(seed) + "社区"+str(community_ori))

    #print("original cost time: %s" % (end_time - start_time))
    return community_ori



if __name__ == "__main__":
    miu=[2, 8, 14, 20, 31, 39, 52, 66, 82, 96, 5901, 5907, 5912, 5928, 5952, 5960, 5973, 5975, 5996, 5997]
    for i in miu:
        run("input/lfr/network/LFR12.txt", "output/LFR12_miu7_4.txt", i)
 