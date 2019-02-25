from neo4j.v1 import GraphDatabase, basic_auth
import pandas as pd

driver = GraphDatabase.driver('bolt://10.29.4.242:7687', auth=basic_auth("neo4j", "wsqxy8219338"))
data_list = driver.session().run("match (x:小区) where x.name in ['澳林春天五期','澳景花庭','澳林春天二期','融域嘉园','澳林春天六期',"
                                 "'天月园','上林世家','美然动力A2区','典雅庄园'] return x.name,x.comnmunity_id")

posted_data = {'1111027377378': '京师园'}
returned_data = {}

for data in data_list:

    # print(data_treat['x.comnmunity_id'])
    # print(data_treat['x.name'])
    comnmunity_id = str(data['x.comnmunity_id'])
    comnmunity_name = str(data['x.name'].replace("'", ""))
    # print(comnmunity_id,comnmunity_name)
    returned_data[comnmunity_id] = comnmunity_name

df = pd.read_csv('小区_newest_V3.csv', header=None, sep=',',error_bad_lines=False)
print(df.iloc[0])
# print(df.head())

# 生成一个字典，用于查询小区Id和CSV中index的对应关系（key：小区id，value：CSV中的index）

catalogue = {}
for index, row in df.iterrows():
    if index > 0:
        catalogue[row[1]] = index


# 数据所在的列
column_index = [3, 6, 7, 8, 10, 12, 14, 15, 16, 17, 19, 22, 25, 27, 29, 30, 31, 32, 33]
# 不同数据的权重
column_value = [1, 5, 7, 8, 3, 4, 5, 9, 9, 1, 8, 7, 6, 5, 4, 10, 4, 8, 6]
# print(len(column_index),len(column_value))

# 数据权重的dict
weight_dict ={}
key_value = zip(column_index,column_value)
for tuple_data in key_value:
    weight_dict[tuple_data[0]] = tuple_data[1]

# 找到“查询”小区的位置(key为小区ID)

posted_data_index_num = list(posted_data.keys())[0]
returned_data_index_num = list(returned_data.keys())

similarity_dict ={}

for returned_data_index in returned_data_index_num:

    similarity = 0

    temp_posted_data = []
    temp_returned_data = []
    for column in column_index:

        posted_data_csv_index = catalogue[posted_data_index_num]
        returned_data_csv_index = catalogue[returned_data_index]

        if column in [3, 15, 16, 30, 31, 32, 33]:

            posted_data_value = eval(df.iloc[posted_data_csv_index, column])
            returned_data_value = eval(df.iloc[returned_data_csv_index, column])
            if len (posted_data_value) ==0:
                similarity_xishu = 0
                similarity += weight_dict[column] * similarity_xishu
            else:
                similarity_xishu = len(list(set(posted_data_value) & set(returned_data_value)))/len(posted_data_value)
                similarity += weight_dict[column]*similarity_xishu

        else:
            posted_data_value = df.iloc[posted_data_csv_index, column]
            returned_data_value = df.iloc[returned_data_csv_index, column]

            # temp_posted_data.append(posted_data_value)
            # temp_returned_data.append(returned_data_value)

            if posted_data_value == returned_data_value:
                similarity += weight_dict[column]

    similarity_dict[returned_data[returned_data_index]] = similarity
    print(posted_data[posted_data_index_num],temp_posted_data)
    print(returned_data[returned_data_index],temp_returned_data)


    # print(xiangsidu)

print(sorted(similarity_dict.items(), key=lambda item: item[1], reverse=True))
# print(xiangsidu)