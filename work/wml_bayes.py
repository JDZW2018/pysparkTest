import numpy as np
import re
import os
import random
import numpy as np

# 处理给定路径下的文件
def load_data(folder_path):
	os.chdir(folder_path)
	doc_list = []
	label = []
	for i in range(1, 26):
		file_name = 'spam/{0}.txt'.format(i)
		# 将文件转换成单词列表
		words_list = doc2words_list(open(file_name).read())
		# 将所有单词放到一个列表中，并制定类别
		doc_list.append(words_list)
		label.append(1)
		file_name = 'ham/{0}.txt'.format(i)
		words_list = doc2words_list(open(file_name).read())
		doc_list.append(words_list)
		label.append(0)
	return doc_list, label

# 将文件转换成单词列表
def doc2words_list(doc):
	string_list = re.split(r'\W*', doc)
	words_list = [word.lower() for word in string_list if len(word) > 2]
	return words_list

# 创建单词字典
def create_vocab_list(data_set):
	vocab_set = set([])
	for document in data_set:
		vocab_set = vocab_set | set(document)
	return list(vocab_set)


# 词集模型
def word2vec(vocab_list, input_set):
	return_vec = [0] * len(vocab_list)
	for word in input_set:
		if word in vocab_list:
			return_vec[vocab_list.index(word)] = 1
		else:
			print('The word {0} is not in my vocab_list'.format(word))
	return return_vec


# 词袋模型
def bag_word2vec(vocab_list, input_set):
	return_vec = [0] * len(vocab_list)
	for word in vocab_list:
		return_vec[vocab_list.index(word)] += 1
	return return_vec

# 训练贝叶斯
def train_bayes(train_mat, label):
	num_train_docs = len(train_mat)
	num_words = len(train_mat[0])
	# 非侮辱性文档的概率
	p0 = sum(label) / num_train_docs
	# 每个类别中每个单词出现的次数
	p0_num = np.ones(num_words)
	p1_num = np.ones(num_words)
	# 每个类别中所有单词数目
	p0_denom = 2.0
	p1_denom = 2.0
	for i in range(num_train_docs):
		if label[i] == 1:
			p1_num += train_mat[i]
			p1_denom += sum(train_mat[i])
		else:
			p0_num += train_mat[i]
			p0_denom += sum(train_mat[i])
	# 条件概率：每个类别中某个单词出现次数/每个类别所有单词数
	p1_vec = np.log(p1_num/p1_denom)
	p0_vec = np.log(p0_num/p0_denom)
	return p0, p1_vec, p0_vec

# 贝叶斯分类器
def bayes_classify(test_arr, p0_vec, p1_vec, p0):
	p1 = sum(test_arr * p1_vec) + np.log(1 - p0)
	p0 = sum(test_arr * p0_vec) + np.log(p0)
	if p1 > p0:
		return 1
	else:
		return 0


if __name__ == '__main__':

	folder_path = '/Users/mac/PycharmProjects/pysparkTest/work/email/'
	doc_list, label = load_data(folder_path)
	vocab_list = create_vocab_list(doc_list)

	# 产生交叉验证集
	all_set = set(range(50))
	test_set = set()
	for i in range(10):
		test_set.add(random.randint(0, 49))
	train_set = all_set - test_set

	# 单词转换成词集向量
	word_set = []
	for i in range(50):
		word_set.append(word2vec(vocab_list, doc_list[i]))

	# 产生训练集和测试集
	train_mat = []
	test_mat = []
	train_label = []
	test_label = []
	print('train_set:', train_set)
	print('test_set', test_set)
	for i in train_set:
		train_mat.append(word_set[i])
		train_label.append(label[i])
	for i in test_set:
		test_mat.append(word_set[i])
		test_label.append(label[i])
	

	# 训练并测试
	p0, p1_vec, p0_vec = train_bayes(train_mat, train_label)
	error_cnt = 0
	for i in range(len(test_mat)):
		label_bayes = bayes_classify(test_mat[i], p0_vec, p1_vec, p0)
		if label_bayes != test_label[i]:
			error_cnt += 1
	print(error_cnt/len(test_set))


	
	