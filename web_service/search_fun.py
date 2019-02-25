from neo4j.v1 import GraphDatabase, basic_auth
import pandas as pd
import datetime

df = pd.read_csv('小区_newest_V3.csv', header=None, sep=',', error_bad_lines=False)


def init_dict(df):
    result = {}
    rows = df.itertuples()
    next(rows)
    for row in rows:
        inner_result = {}
        for i in range(len(row)):
            if i == 0: continue
            if (i - 1) in [3, 15, 16, 30, 31, 32, 33]:
                inner_result[i - 1] = eval(row[i])
            else:
                inner_result[i - 1] = row[i]
        result[row[2]] = inner_result
    return result

community_info_dict = init_dict(df)


def search_similiar_community(input_xiaoqu, driver, column_index, column_value):
    start_time = datetime.datetime.now()

    searched_sentence = 'MATCH n=(m:小区{name:"%s"})-[*..2]-(x:小区) with m, x, count(*) as ctn where ctn > 5 ' \
                        'return m.name,m.comnmunity_id,x.name,x.comnmunity_id' % input_xiaoqu

    connect_database = datetime.datetime.now()
    print("建立连接时间：%s" % (connect_database - start_time))

    data_list = driver.session().run(searched_sentence)

    returned_data_time = datetime.datetime.now()
    print("查询数据花费时间：%s" % (returned_data_time - connect_database))
    print("查询数据总共花费时间：%s" % (returned_data_time - start_time))

    posted_data = {}
    returned_data = {}
    for data in data_list:
        # print(data_treat['x.comnmunity_id'])
        # print(data_treat['x.name'])
        comnmunity_id = str(data['x.comnmunity_id'])
        comnmunity_name = str(data['x.name'].replace("'", ""))
        # print(comnmunity_id,comnmunity_name)
        returned_data[comnmunity_id] = comnmunity_name
        posted_data[data['m.comnmunity_id']] = data['m.name']

    # print(returned_data)
    # print(posted_data)

    # print(df.iloc[0])
    # print(df.head())

    # 数据权重的dict

    weight_dict = {}
    key_value = zip(column_index, column_value)
    for tuple_data in key_value:
        weight_dict[tuple_data[0]] = tuple_data[1]

    # 找到“查询”小区的位置(key为小区ID)
    posted_data_index_num = str(list(posted_data.keys())[0])
    returned_data_index_num = list(returned_data.keys())

    similarity_dict = {}

    post_inner_map = community_info_dict.get(posted_data_index_num)

    for returned_data_index in returned_data_index_num:
        similarity = 0

        returned_inner_map = community_info_dict.get(returned_data_index)
        for column in column_index:

            posted_data_value = post_inner_map.get(column)
            returned_data_value = returned_inner_map.get(column)

            if column in [3, 15, 16, 30, 31, 32, 33]:
                similarity_xishu = 0 \
                    if (len(posted_data_value) == 0) else \
                    len(set(posted_data_value).intersection(set(returned_data_value))) / len(posted_data_value)
            else:
                similarity_xishu = 1 if (posted_data_value == returned_data_value) else 0

            similarity += weight_dict[column] * similarity_xishu

        similarity_dict[returned_data[returned_data_index]] = similarity
    # cost = datetime.datetime.now() - begin
    # print(cost)

    # print(sorted(similarity_dict.items(), key=lambda item: item[1], reverse=True))
    final_data = [x[0] for x in (sorted(similarity_dict.items(), key=lambda item: item[1], reverse=True)[0:5])]
    final_data_time = datetime.datetime.now()

    print("计算相似小区花费时间：%s" % (final_data_time - returned_data_time))
    print("总共花费时间：%s" % (final_data_time - start_time))
    return final_data


if __name__ == '__main__':
    # 数据所在的列
    column_index = [3, 6, 7, 8, 10, 12, 14, 15, 16, 17, 19, 22, 25, 27, 29, 30, 31, 32, 33]
    # 不同数据的权重
    column_value = [1, 5, 7, 8, 3, 4, 5, 7, 7, 1, 8, 7, 6, 5, 8, 10, 4, 8, 6]

    driver = GraphDatabase.driver('bolt://10.29.4.242:7687', auth=basic_auth("neo4j", "wsqxy8219338"))
    df = pd.read_csv('小区_newest_V3.csv', header=None, sep=',', error_bad_lines=False)
    data = search_similiar_community('望京新城', driver, column_index, column_value)
    print(data)