import search_fun
from flask import Flask
from neo4j.v1 import GraphDatabase, basic_auth
import pandas as pd

app = Flask(__name__)

driver = GraphDatabase.driver('bolt://10.29.4.242:7687', auth=basic_auth("neo4j", "wsqxy8219338"))

df = pd.read_csv('小区_newest_V3.csv', header=None, sep=',', error_bad_lines=False)

# 数据所在的列
column_index = [3, 6, 7, 8, 10, 12, 14, 15, 16, 17, 19, 22, 25, 27, 29, 30, 31, 32, 33]
# 不同数据的权重
column_value = [1, 5, 7, 8, 3, 4, 5, 7, 7, 1, 8, 7, 6, 5, 8, 10, 4, 8, 6]

def init_dict(df):
    result = {}
    rows = df.itertuples()
    next(rows)
    for row in rows:
        inner_result = {}
        for i in range(len(row)):
            if i == 0 : continue
            if (i - 1) in [3, 15, 16, 30, 31, 32, 33]:
                inner_result[i - 1] = eval(row[i])
            else:
                inner_result[i - 1] = row[i]
        result[row[2]] = inner_result
    return result

community_info_dict = init_dict(df)

@app.route('/similiar/<xiaoquname>')
def index(xiaoquname):
    xiaoquname = str(xiaoquname)
    data = search_fun.search_similiar_community(xiaoquname, driver, column_index, column_value)
    return str(data)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')