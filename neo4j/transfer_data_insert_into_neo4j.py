import pandas as pd
import numpy as np
import datetime
from collections import Counter
from neo4j.v1 import GraphDatabase

df = pd.read_csv('小区_newest_V3.csv',header=None,sep=',',error_bad_lines=False)
print(df.iloc[0])
# print(df.head())

def generate_community(df):

    stat_sentence_list = []

    start_time = datetime.datetime.now()

    for index, row in df.iterrows():

        if index > 0:

            name = row[0]
            comnmunity_id = row[1]
            city_name = row[2]
            district_name = row[8]
            bizcircle_name = row[7]
            building_types = row[3]
            louling_phase = row[6]
            building_count_phase = row[10]
            house_count_phase = row[12]
            ratio_house_count_building_count = row[14]
            deal_properties = row[15]
            stat_function = row[16]
            cycle_line_name = row[17]
            properties_treated = row[19]
            developers_treated = row[22]
            car_ratio_phase = row[25]
            green_rate_phase = row[27]
            tenement_fees_phase = row[29]
            subway = row[30]
            hospital = row[31]
            life = row[32]
            fun = row[33]

            dict_statment = "{name:'%s',comnmunity_id:%s,city_name:'%s',district_name:'%s',bizcircle_name:'%s',building_types:%s," \
                            "louling_phase:'%s',building_count_phase:'%s',house_count_phase:'%s',ratio_house_count_building_count:'%s'," \
                            "deal_properties:%s,stat_function:%s,cycle_line_name:'%s',properties_treated:'%s',developers_treated:'%s'," \
                            "car_ratio_phase:'%s',green_rate_phase:'%s',tenement_fees_phase:'%s',subway:%s,hospital:%s," \
                            "life:%s,fun:%s}" % \
                            (name, comnmunity_id, city_name, district_name, bizcircle_name, building_types, louling_phase,
                             building_count_phase, house_count_phase, ratio_house_count_building_count,deal_properties,
                             stat_function, cycle_line_name, properties_treated, developers_treated, car_ratio_phase, green_rate_phase,
                             tenement_fees_phase, subway, hospital, life, fun)

            cypher_statment = "create ({}:小区{})".format('小区' + name.replace('·', '').replace('（', '').replace('）', '').
                                                        replace('(', '').replace(')', '').replace('、', '').replace('?', '').
                                                        replace('﹒', '').replace('+', '').replace('@', '').replace('.', ''), dict_statment)

            # print(index, name, comnmunity_id, city_name, district_name, bizcircle_name, building_types, louling_phase, building_count_phase,
            #       house_count_phase, ratio_house_count_building_count, deal_properties, stat_function, cycle_line_name, properties_treated,
                  # developers_treated, car_ratio_phase, green_rate_phase, tenement_fees_phase, subway, hospital, life, fun)

            stat_sentence_list.append(cypher_statment)

            # print(cypher_statment)

    end_time = datetime.datetime.now()
    # print(stat_sentence_list, len(stat_sentence_list))
    print(end_time-start_time)

    return stat_sentence_list


def district_name(df):

    district_name_list = []
    for index, row in df.iterrows():

        if index >0:

            district_name = row[8]
            city_name = row[2]
            dict_statment = "{name:'%s',city_name:'%s'}" % (district_name,city_name)
            cypher_statment = "create ({}:区域{})".format(district_name, dict_statment)
            district_name_list.append(cypher_statment)

    data = list(set(district_name_list))

    print(data)

    return data


def part_area_name (df):

    district_name_list = []

    for index, row in df.iterrows():

        if index > 0:

            part_area_name = row[7]
            district_name = row[8]
            part_area_statment = "{name:'%s',district_name:'%s'}" % (part_area_name,district_name)
            cypher_statment = "create ({}:片区{})".format(part_area_name.replace('(', '').replace(')', ''), part_area_statment)
            district_name_list.append(cypher_statment)

    data = list(set(district_name_list))

    print(data)

    return data


def building_types(df):

    building_type_list = []

    for index, row in df.iterrows():
        if index > 0:
            building_types = row[3]
            building_types_list = eval(building_types)

            for building_type in building_types_list:
                building_types_statment = "{name:'%s'}" % building_type
                cypher_statment = "create({}:建筑结构{})".format(building_type, building_types_statment)
                building_type_list.append(cypher_statment)
    # print(Counter(building_types_list))

    data = list(set(building_type_list))
    print(data)

    return data


def deal_properties(df):

    deal_properties_list = []

    for index, row in df.iterrows():
        if index > 0:
            deal_properties = row[15]
            specified_properites = eval(deal_properties)
            for property_name in specified_properites:
                building_types_statment = "{name:'%s'}" % property_name
                cypher_statment = "create ({}:产权性质{})".format(property_name, building_types_statment)
                deal_properties_list.append(cypher_statment)

    data = list(set(deal_properties_list))

    # print(Counter(building_types_list))
    # print(list(set(building_types_list)))

    print(data)
    print(len(data))

    return data


def stat_function(df):

    stat_function_list = []

    for index, row in df.iterrows():
        if index > 0:

            stat_functions = row[16]

            specified_stat_functions = eval(stat_functions)
            for stat_function in specified_stat_functions:
                building_types_statment = "{name:'%s'}" % stat_function
                cypher_statment = "create ({}:使用性质{})".format(stat_function.replace('/', '').replace('（', '').replace('）', ''), building_types_statment)
                stat_function_list.append(cypher_statment)

    data = list(set(stat_function_list))

    print(data)

    return data


def louling_phase(df):

    louling_phase_list = []

    for index, row in df.iterrows():

        if index > 0:
            louling_phase = row[6]
            statment = "{name:'%s'}" % louling_phase
            cypher_statment = "create ({}:楼龄{})".format("楼龄" + louling_phase.replace('-', '到').replace('+', '以上'), statment)
            louling_phase_list.append(cypher_statment)

    qu_chong_data = list(set(louling_phase_list))
    print(qu_chong_data)

    return qu_chong_data


def building_count_phase(df):

    building_count_phase_list = []

    for index, row in df.iterrows():

        if index > 0:
            building_count_phase = row[10]
            statment = "{name:'%s'}" % building_count_phase
            cypher_statment = "create ({}:楼栋规模{})".format(building_count_phase, statment)
            building_count_phase_list.append(cypher_statment)

    qu_chong_data = list(set(building_count_phase_list))
    print(qu_chong_data)

    return qu_chong_data


def house_count_phase(df):

    house_count_phase_list = []

    for index, row in df.iterrows():

        if index > 0:

            house_count_phase = row[12]

            if house_count_phase == '-1':
                house_count_phase = '暂无'
                statment = "{name:'%s'}" % house_count_phase
                cypher_statment = "create ({}:居住规模{})".format('户数' + house_count_phase, statment)
                house_count_phase_list.append(cypher_statment)
            else:
                statment = "{name:'%s'}" % house_count_phase
                cypher_statment = "create ({}:居住规模{})".format('户数' + house_count_phase.replace('-', '到').replace('+', '以上'), statment)
                house_count_phase_list.append(cypher_statment)


    qu_chong_data = list(set(house_count_phase_list))
    print(qu_chong_data)

    return qu_chong_data


def ratio_house_count_building_count(df):

    ratio_house_count_building_count_list = []

    for index, row in df.iterrows():

        if index > 0:

            ratio_house_count_building_count = row[14]

            if pd.notnull(ratio_house_count_building_count) is False:

                ratio_house_count_building_count = '暂无'

                statment = "{name:'%s'}" % ratio_house_count_building_count
                cypher_statment = "create ({}:住户密度{})".format('密度'+ ratio_house_count_building_count, statment)
                ratio_house_count_building_count_list.append(cypher_statment)

            else:
                statment = "{name:'%s'}" % ratio_house_count_building_count
                cypher_statment = "create ({}:住户密度{})".format('密度'+ str(ratio_house_count_building_count).replace('+', '以上').replace('-', '到'), statment)
                ratio_house_count_building_count_list.append(cypher_statment)

    qu_chong_data = list(set(ratio_house_count_building_count_list))
    print(qu_chong_data)

    return qu_chong_data


def cycle_line_name(df):

    cycle_line_name_list = []

    for index, row in df.iterrows():

        if index > 0:
            cycle_line = row[17]

            if pd.notnull(cycle_line) is False:
                cycle_line = '暂无'
                statment = "{name:'%s'}" % cycle_line
                cypher_statment = "create ({}:环线{})".format(cycle_line, statment)
                cycle_line_name_list.append(cypher_statment)

            else:
                statment = "{name:'%s'}" % cycle_line
                cypher_statment = "create ({}:环线{})".format(cycle_line, statment)
                cycle_line_name_list.append(cypher_statment)

    qu_chong_data = list(set(cycle_line_name_list))
    print(qu_chong_data)

    return qu_chong_data


def properties_treated(df):

    properties_treated_list = []

    for index, row in df.iterrows():

        if index > 0:

            properties_treated = row[19]

            if pd.notnull(properties_treated) is False:

                properties_treated = '暂无'
                statment = "{name:'%s'}" % properties_treated
                cypher_statment = "create ({}:物业公司{})".format(properties_treated, statment)
                properties_treated_list.append(cypher_statment)

            else:
                statment = "{name:'%s'}" % properties_treated
                cypher_statment = "create ({}:物业公司{})".format(properties_treated.replace('·', '').replace('（', '').replace('）', '').
                                                                 replace('(', '').replace(')', '').replace('、', '').replace('?', '').
                                                                 replace('﹒', '').replace('+', '').replace('@', '').replace('.', ''), statment)
                properties_treated_list.append(cypher_statment)

    qu_chong_data = list(set(properties_treated_list))
    print(qu_chong_data)
    print(len(qu_chong_data))

    return qu_chong_data


def developers_treated(df):

    developers_treated_list = []

    for index, row in df.iterrows():

        if index > 0:

            developers_treated = row[22]

            if pd.notnull(developers_treated) is False:

                developers_treated = '暂无'
                statment = "{name:'%s'}" % developers_treated
                cypher_statment = "create ({}:开发商{})".format(developers_treated, statment)
                developers_treated_list.append(cypher_statment)

            else:
                statment = "{name:'%s'}" % developers_treated
                cypher_statment = "create ({}:开发商{})".format(developers_treated.replace('·', '').replace('（', '').replace('）', '').
                                                                                  replace('(', '').replace(')', '').replace('、', '').replace('?', '').
                                                                                  replace('﹒', '').replace('+', '').replace('@', '').replace('.', '').
                                                                                  replace(',', '').replace(' ', ''), statment)
                developers_treated_list.append(cypher_statment)

    qu_chong_data = list(set(developers_treated_list))
    print(qu_chong_data)
    print(len(qu_chong_data))

    return qu_chong_data


def car_ratio_phase(df):

    car_ratio_phase_list = []

    for index, row in df.iterrows():

        if index > 0:

            car_ratio_phase = row[25]

            if car_ratio_phase == '-1':
                car_ratio_phase = '暂无'
                statment = "{name:'%s'}" % car_ratio_phase
                cypher_statment = "create ({}:车位比{})".format('车位比' + car_ratio_phase, statment)
                car_ratio_phase_list.append(cypher_statment)
            else:
                statment = "{name:'%s'}" % car_ratio_phase
                cypher_statment = "create ({}:车位比{})".format('车位比' + car_ratio_phase.replace('.', '点').replace('-','到').replace('+','以上'), statment)
                car_ratio_phase_list.append(cypher_statment)


    qu_chong_data = list(set(car_ratio_phase_list))
    print(qu_chong_data)

    return qu_chong_data


def green_rate_phase(df):

    green_rate_phase_list = []

    for index, row in df.iterrows():

        if index > 0:

            green_rate_phase = row[27]

            if green_rate_phase == '-1':
                green_rate_phase = '暂无'
                statment = "{name:'%s'}" % green_rate_phase
                cypher_statment = "create ({}:绿化率{})".format('绿化率' + green_rate_phase, statment)
                green_rate_phase_list.append(cypher_statment)
            else:
                statment = "{name:'%s'}" % green_rate_phase
                cypher_statment = "create ({}:绿化率{})".format('绿化率' + green_rate_phase.replace('-','到').replace('+','以上'), statment)
                green_rate_phase_list.append(cypher_statment)


    qu_chong_data = list(set(green_rate_phase_list))
    print(qu_chong_data)

    return qu_chong_data


def tenement_fees_phase(df):

    tenement_fees_phase_list = []

    for index, row in df.iterrows():

        if index > 0:

            tenement_fees_phase = row[29]

            if tenement_fees_phase == '-1':
                tenement_fees_phase = '暂无'
                statment = "{name:'%s'}" % tenement_fees_phase
                cypher_statment = "create ({}:物业费{})".format('物业费' + tenement_fees_phase, statment)
                tenement_fees_phase_list.append(cypher_statment)
            else:
                statment = "{name:'%s'}" % tenement_fees_phase
                cypher_statment = "create ({}:物业费{})".format('物业费' + tenement_fees_phase.replace('-', '到').replace('+', '以上'), statment)
                tenement_fees_phase_list.append(cypher_statment)


    qu_chong_data = list(set(tenement_fees_phase_list))
    print(qu_chong_data)

    return qu_chong_data


def subway(df):

    subway_list = []

    for index, row in df.iterrows():
        if index > 0:

            subway = row[30]

            specified_subway_stations = eval(subway)
            if specified_subway_stations:

                for station in specified_subway_stations:
                    statment = "{name:'%s'}" % station
                    cypher_statment = "create ({}:地铁站{})".format(station.replace('(', '').replace(')', ''), statment)
                    subway_list.append(cypher_statment)

    data = list(set(subway_list))

    print(data)

    return data


def hospital(df):

    hospital_list = []

    for index, row in df.iterrows():

        if index > 0:

            hospital_names = row[31]

            hospitals = eval(hospital_names)

            if hospitals:

                for hospital in hospitals:
                    statment = "{name:'%s'}" % hospital
                    cypher_statment = "create ({}:医院{})".format(hospital.replace('(', '').replace(')', '').replace('（', '').replace('）', ''), statment)
                    hospital_list.append(cypher_statment)

    data = list(set(hospital_list))

    print(data)
    print(len(data))

    return data


def life(df):

    life_list = []

    for index, row in df.iterrows():

        if index > 0:

            life_names = row[32]

            lifes = eval(life_names)

            if lifes:

                for life in lifes:
                    statment = "{name:'%s'}" % life
                    cypher_statment = "create ({}:生活配套{})".format(life.replace('(', '').replace(')', '').replace('（', '').
                                                                          replace('）', '').replace(' ', '').replace('·', ''), statment)
                    life_list.append(cypher_statment)

    data = list(set(life_list))

    print(data)
    print(len(data))

    return data


def fun(df):

    fun_list = []

    for index, row in df.iterrows():

        if index > 0:

            fun_names = row[33]

            funs = eval(fun_names)

            if funs:

                for fun in funs:
                    statment = "{name:'%s'}" % fun
                    cypher_statment = "create ({}:娱乐配套{})".format(fun.replace('(', '').replace(')', ''), statment)
                    fun_list.append(cypher_statment)

    data = list(set(fun_list))

    print(data)
    print(len(data))

    return data



# generate_community(df)  # 执行小区
# district_name(df)  # 执行地区字段
# part_area_name (df) # 执行片区
# building_types(df) # 建筑类型
# louling_phase(df) # 小区楼龄
# building_count_phase(df) # 楼栋数据
# house_count_phase(df) # 户数数据
# ratio_house_count_building_count(df) # 住户密度
# deal_properties(df) # 产权性质
# stat_function(df) # 使用性质
# cycle_line_name(df) # 环线
# properties_treated(df) # 物业公司
# developers_treated(df) # 开发商
# car_ratio_phase(df) # 车位比
# green_rate_phase(df) # 绿化率
# tenement_fees_phase(df) # 物业费
# subway(df) # 地铁线
# hospital(df) # 医院
# life(df) # 生活配套
# fun(df) # 娱乐配套

uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=("neo4j", "wsqxy8219338"))

def cyphertx(cypher):
    with driver.session() as session:
        with session.begin_transaction() as tx:
            tx.run(cypher)


data = fun(df)

for sentence in data:
    cyphertx(sentence)

print('done')