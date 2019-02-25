from neo4j.v1 import GraphDatabase, basic_auth
import time
import datetime

# cypher_sentences = ['MATCH n=(:小区{name:"新龙城"})-[*..2]-() return n', 'MATCH n=(:小区{name:"新龙城"})-[*..2]-(x:小区) return x',
#                     'MATCH n=(x:小区)-[*..1]-() where x.name in ["金域华府一期","新龙城","富力桃园C区","旗胜家园","枫丹丽舍","莱圳家园"]  return n',
#                     'MATCH n=(:小区{name:"新龙城"})-[*..2]-(x:小区) return x.name', 'MATCH (p1:小区 {name:"富力桃园C区"}),(p2:小区 {name:"旗胜家园"}), p=allshortestpaths((p1)-[*..10]-(p2)) RETURN p']
#
# uri = "bolt://10.29.4.242:7687"
# driver = GraphDatabase.driver(uri, auth=("neo4j", "wsqxy8219338"))
#
# def cyphertx(cypher):
#     with driver.session() as session:
#         with session.begin_transaction() as tx:
#             returned_data = tx.run(cypher)
#     return returned_data
#
# for sentence in cypher_sentences:
#     start_time = datetime.datetime.now()
#     print(sentence)
#     data = cyphertx(sentence)
#     end_time = datetime.datetime.now()
#     print("耗时%s秒" % (end_time-start_time))

####################################################

driver = GraphDatabase.driver('bolt://10.29.4.242:7687', auth=basic_auth("neo4j", "wsqxy8219338"))
data_list = driver.session().run('MATCH n=(m:小区{name:"嘉铭桐城E区"})-[*..2]-(x:小区) return m.name,m.comnmunity_id,x.name,x.comnmunity_id')

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


xiaoqu_list = list(returned_data.keys())
print(len(set(xiaoqu_list)))
# print(posted_data)