import requests
from lxml import etree
import pandas
from pandas import DataFrame
from collections import deque
import time
import random

'''种子'''
# url = 'http://beijing.gongjiao.com/lines_all.html'
#
# all_lines_page = requests.get(url).text
# all_lines_info_list = etree.HTML(all_lines_page).xpath('//ul[@class="f-cb"]/li')
#
#
#
# dd = open('dad.txt', 'w+', encoding='utf-8')
#
# for line in all_lines_info_list:
#
#
#     line_name = line.xpath('a/text()')[0].replace('公交车路线', '')
#     line_url = line.xpath('a/@href')[0]
#     data = [line_name, line_url]
#     dd.write((',').join(data) + '\n')
#
# dd.close()
'''所有站点信息'''
# with open('dad.txt', 'r', encoding='utf-8') as f:
#     lines_list = [{x.rstrip().split(',')[0]:x.rstrip().split(',')[1]} for x in f.readlines()]
#
# dd = open('dadd.txt', 'w+', encoding='utf-8')
#
#
# def get_station_data(line, dd):
#     line_page = requests.get(list(line.values())[0])
#     bus_station_list = etree.HTML(line_page.text).xpath('//ul[@class="gj01_line_img JS-up clearfix"]/li')
#     # print(bus_station_list)
#     for bus_station in bus_station_list:
#         station_name = bus_station.xpath('a/text()')[0]
#         station_url = bus_station.xpath('a/@href')[0]
#         data = [list(line.keys())[0], list(line.values())[0], station_name, station_url]
#         dd.write(','.join(data) + '\n')
#
#
# lines_deque = deque(lines_list)
#
# time_count = 0
# fail_count = 0
#
# while lines_deque:
#
#     line = lines_deque.popleft()
#
#     try:
#
#         get_station_data(line,dd)
#         time_count += 1
#         time.sleep(0.1)
#         print(time_count)
#
#     except:
#
#         print("此调线路：%s获取失败，稍后重试" % (list(line.keys())[0]))
#         lines_deque.append(line)
#         time.sleep(2)
#         fail_count += 1
#
#     if time_count % 100 == 0:
#         sleep_time = random.randint(1, 3)
#         print('已获取' + str(time_count) + '条数据')
#         time.sleep(sleep_time)
#
#     if fail_count >= 100:
#         break
#
# dd.close()


# '''处理站点数据'''

# station_info = pandas.read_csv('dadd.txt', sep=',', low_memory=False, header=None)
# print(list(set(station_info[2])))