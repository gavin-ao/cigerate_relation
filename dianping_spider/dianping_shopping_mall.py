import requests
from lxml import etree
import json
from pymongo import MongoClient
import math
import time
import re
from collections import deque
import random


'''来自于微信小程序的测试'''

'''全局变量'''

Headers ={
          'charset':'utf-8',
          'Accept-Encoding':'gzip',
          'platformversion':'4.4.2',
          'content-type':'application/json',
          'platform':'Android',
          'minaname':'dianping-wxapp',
          'token':'',
          'minaversion':'3.5.6',
          'sdkversion':'2.1.2',
          'openid':'HpRqo9zgct9bYaClf5mV7UnH2_xZsHU39zEUuc7f5Z0',
          'referer':'https://servicewechat.com/wx734c1ad7b3562129/63/page-frame.html',
          'wechatversion':'6.6.7',
          'User-Agent':'Mozilla/5.0 (Linux; Android 4.4.2; SM-G955F Build/JLS36C) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Mobile Safari/537.36 MicroMessenger/6.6.7.1321(0x26060739) NetType/WIFI Language/zh_CN MicroMessenger/6.6.7.1321(0x26060739) NetType/WIFI Language/zh_CN',
          'Host':'m.dianping.com',
          'Connection':'Keep-Alive'
         }

client = MongoClient('mongodb://localhost:27017/')
db = client.meituan
requests.packages.urllib3.disable_warnings()

# url = 'http://m.dianping.com/wxmapi/search?dpid=164357815e269-18180958dbe542-0-0-164357815e3c8&_=1529977181920&cityId=2&locatecityid=2&mylng=116.45032&mylat=39.909313&cookieid=156be29627b889fa177c5b24a993b6e152f970bd1f91947be8d6715689c81c56&start={}&categoryid=119&regionid={}&parentCategoryId='

# data = requests.get(url.format(0),headers = Headers )
# next = json.loads(data.text)['data']['nextStartIndex']


'''美团数据字典'''
# with open('dianping.txt','r',encoding='utf-8') as f:
#     data_json = f.readlines()
# data = json.loads(data_json[0])
# print(data['data']['list'])

total_shop = 0

'''通过微信小程序接口获取商场信息（不包含详细信息，下一步获取详细信息），数据存储在mongoDB中'''


# for info in data['data']['selector'][0]:
#     if 'count' in info.keys():
#         if '全部' in info['name'] and '全部商区' not in info['name']:
#
#             total_shop += info['count']
#
#             print(info)
#             times = int(math.ceil(info['count']/25))
#             for i in range(0, times):
#                 next_index = i * 25
#                 # print(next_index, info['name'], info['parentId'])
#                 # print(url.format(next_index, info['parentId']))
#                 data = requests.get(url.format(next_index, info['parentId']), headers=Headers)
#
#                 for each_shop_info in json.loads(data.text)['data']['list']:
#                     print(each_shop_info)
#                     each_shop_info['_id'] = info['name'] + each_shop_info['shopInfo']['id']
#                     db.shop_info_list.insert_one(each_shop_info)
#
#                 time.sleep(2)
# print(total_shop)
# print('done')


'''通过微信小程序接口获取商场的详细信息，存储在mongoDB中'''

# Headers_shop_detail ={
#           'charset': 'utf-8',
#           'Accept-Encoding': 'gzip',
#           'platformversion': '4.4.2',
#           'ismicromessenger': 'true',
#           'network-type': 'wifi',
#           'content-type': 'application/json',
#           'platform': 'Android',
#           'appname': 'dianping-wxapp',
#           'token': '',
#           'appversion': '3.5.6',
#           'dpid': 'HpRqo9zgct9bYaClf5mV7UnH2_xZsHU39zEUuc7f5Z0',
#           'referer': 'https://servicewechat.com/wx734c1ad7b3562129/64/page-frame.html',
#           'micromsgversion': '6.6.7',
#           'User-Agent': 'Mozilla/5.0 (Linux; Android 4.4.2; SM-G955F Build/JLS36C) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/30.0.0.0 Mobile Safari/537.36 MicroMessenger/6.6.7.1321(0x26060739) NetType/WIFI Language/zh_CN MicroMessenger/6.6.7.1321(0x26060739) NetType/WIFI Language/zh_CN',
#           'Host': 'mapi.dianping.com',
#           'Connection': 'Keep-Alive',
#           'phone-brand':'samsung',
#           'phone-model': 'SM-G955F',
#
#          }
#
# info_list = db.shop_info_list
# data_list = info_list.find()
# seed_list = []
#
# for info in data_list:
#     seed_list.append(info)

'''获取商场地理位置信息'''

url = 'https://m.dianping.com/wxmapi/shop/shopmapinfo?shopUuid={}'

info_list = db.shop_info_list
data_list = info_list.find()
# print(len(list(data_list)))
count = 0

for info in data_list:

    try:

        shop_id = info['shopInfo']['shopUuid']

        # print(shop_id)

        shop_data = requests.get(url.format(shop_id), headers=Headers, verify=False)
        # print(shop_data.)
        insert_data = shop_data.json()
        insert_data['_id'] = shop_id
        db.shop_poi.insert_one(insert_data)

        time.sleep(0.5)

        count += 1
        print(count)

    except:
        print('失败跳过')
        continue

print('done')


'''数据获取函数'''
# print(len(list(seed_list)))


# def get_data(info):
#     shop_url = 'https://mapi.dianping.com/mapi/wechat/shop.bin?shopUuid={}&shopType=20&online=1&token=undefined&lat=39.909313&lng=116.45032 '
#     re_compile = re.compile(u'[\u4e00-\u9fff]+')
#     shop_id = info['shopInfo']['shopUuid']
#     shop_region_name = re_compile.findall(info['_id'])[0]
#     shop_data = requests.get(shop_url.format(shop_id), headers=Headers_shop_detail, verify=False)
#
#     temp_data = json.loads(shop_data.text)
#     temp_data['region_name'] = shop_region_name
#     temp_data['_id'] = shop_region_name + str(shop_id)
#
#     db.shop_info_detail.insert_one(temp_data)
#
#
# shop_info_deque = deque(seed_list)
#
# time_count = 0
# fail_count = 0

'''利用消息队列来获取数据'''

# while shop_info_deque:
#
#     shop_info = shop_info_deque.popleft()
#
#     # get_xiaoqu_other_info(xiaoq_id, db)
#
#     try:
#
#         get_data(shop_info)
#         time_count += 1
#         time.sleep(1)
#         print(time_count)
#
#     except:
#
#         print("此ID：%s获取失败，稍后重试" % shop_info)
#         shop_info_deque.append(shop_info)
#         time.sleep(10)
#         fail_count += 1
#
#
#     if time_count % 100 == 0:
#         sleep_time = random.randint(5, 10)
#         print('已获取' + str(time_count) + '条数据')
#         time.sleep(sleep_time)
#
#     if fail_count >= 100:
#         break




'''PC页面处理起来比较麻烦，所以改用微信小程序接口，以下代码仅供参考'''
# start_url 获取

# headers ={
#             'Host': 'www.dianping.com',
#             'Connection': 'keep-alive',
#             'Cache-Control': 'max-age=0',
#             'Upgrade-Insecure-Requests': '1',
#             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.87 Safari/537.36',
#             'Accept': 'text/html,application/xhtml+xml,application/xml;lianjia_spider=0.9,image/webp,image/apng,*/*;lianjia_spider=0.8',
#             'Referer': 'http://www.dianping.com/',
#             'Accept-Encoding': 'gzip, deflate',
#             'Accept-Language': 'zh-CN,zh;lianjia_spider=0.9'
#           }
#
# url = 'http://www.dianping.com/beijing/ch20/g119'
#
# data = requests.get(url, headers = headers)
# dom_tree = etree.HTML(data.text)
#
# region_name = dom_tree.xpath('//div[@id ="region-nav"]/a/@data-click-title')
# region_url = dom_tree.xpath('//div[@id ="region-nav"]/a/@href')
# print(region_name,region_url)

# print(data.text)

'''每个url_page_num获取'''

# region_name_list = ['西城区', '东城区', '海淀区', '石景山区', '朝阳区', '丰台区', '顺义区', '昌平区', '大兴区', '房山区', '通州区', '密云区', '延庆区', '平谷区', '门头沟区', '怀柔区']
# region_url_list = ['http://www.dianping.com/beijing/ch20/g119r16', 'http://www.dianping.com/beijing/ch20/g119r15', 'http://www.dianping.com/beijing/ch20/g119r17', 'http://www.dianping.com/beijing/ch20/g119r328', 'http://www.dianping.com/beijing/ch20/g119r14', 'http://www.dianping.com/beijing/ch20/g119r20', 'http://www.dianping.com/beijing/ch20/g119r9158', 'http://www.dianping.com/beijing/ch20/g119r5950', 'http://www.dianping.com/beijing/ch20/g119r5952', 'http://www.dianping.com/beijing/ch20/g119r9157', 'http://www.dianping.com/beijing/ch20/g119r5951', 'http://www.dianping.com/beijing/ch20/g119c434', 'http://www.dianping.com/beijing/ch20/g119c435', 'http://www.dianping.com/beijing/ch20/g119c4455', 'http://www.dianping.com/beijing/ch20/g119c4454', 'http://www.dianping.com/beijing/ch20/g119c4453']
#
# region_dict = {}
#
# region_set_list = zip(region_name_list, region_url_list)
#
# for set_data in region_set_list:
#     region_dict[set_data[0]] = set_data[1]
#
# print(region_dict)
#
# url = 'http://www.dianping.com/beijing/ch20/g119c434'
#
# data = requests.get(url, headers = headers)
# dom_tree = etree.HTML(data.text)
#
# page_num = dom_tree.xpath('//div[@class ="page"]/a/text()')[-2]
#
# for i in range(1, int(page_num)+1):
#     detail_url = url + 'p' + str(i)
#     print(detail_url)

'''每个url中有效信息获取，名称、地址、评分、评论数量、url'''

# url = 'http://www.dianping.com/beijing/ch20/g119c434p1'
#
# data = requests.get(url, headers = headers)
# cookie = data.headers['Set-Cookie']
# fin_cookie = cookie.replace('; path=/', '').replace('HttpOnly, ', '')
# print(fin_cookie)

# dom_tree = etree.HTML(data.text)

# shop_info_list = dom_tree.xpath('//div[@class ="shop-list J_shop-list shop-all-list"]/ul/li')
#
# for shop_info in shop_info_list:
#     shop_name = shop_info.xpath('//div[@class ="txt"]/div[@class ="tit"]/a/@title')
#     # shop_level = shop_info.xpath('div[@class ="comment"]/span/@title')
#     # shop_comments_num = shop_info.xpath('a[@class ="review-num"]/b/text()')
#     # shop_mean_cost = shop_info.xpath('a[@class ="mean-price"]/b/text()')
#     # shop_type = shop_info.xpath('div[@class ="tag-addr"]/a[1]/span/text()')
#     # shop_adr = shop_info.xpath('div[@class ="tag-addr"]/a[2]/span/text()')
#     # shop_quality = shop_info.xpath('span[@class ="comment-list"]/span[1]/b/text()')
#     # shop_environment = shop_info.xpath('span[@class ="comment-list"]/span[2]/b/text()')
#     # shop_service = shop_info.xpath('span[@class ="comment-list"]/span[3]/b/text()')
#
#     print(shop_name)
