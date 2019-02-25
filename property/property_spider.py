import requests
from collections import deque
from lxml import etree
import time
import random


last_page_num = 108
url = 'http://www.ecpmi.org.cn/MemberSearch.aspx?NewsClassID=4020&page=%s'
seed_list = [(url % str(x+1)) for x in range(last_page_num)]
data_target_file = open('property.txt', 'w+', encoding='utf-8')


def get_property_data(url, data_target_file):
    line_page = requests.get(url)
    property_list = etree.HTML(line_page.text).xpath('//div[@class="list"]/table/tbody/tr')
    for property in property_list:
        property_id = property.xpath('td[1]/text()')[0]
        property_url = property.xpath('td[2]/a/@href')[0]
        property_name = property.xpath('td[2]/a/text()')[0]
        property_level = property.xpath('td[3]/text()')[0]
        property_time = property.xpath('td[4]/text()')[0]

        data = [property_id, property_url, property_name, property_level, property_time]

        data_target_file.write('|'.join(data) + '\n')


seed_deque = deque(seed_list)

time_count = 0
fail_count = 0

while seed_deque:

    seed = seed_deque.popleft()

    try:

        get_property_data(seed, data_target_file)
        time_count += 1
        time.sleep(0.1)
        print(time_count)

    except:

        print("此seed：%s获取失败，稍后重试" % (seed))
        seed_deque.append(seed)
        time.sleep(2)
        fail_count += 1

    if time_count % 100 == 0:
        sleep_time = random.randint(1, 3)
        print('已获取' + str(time_count) + '条数据')
        time.sleep(sleep_time)

    if fail_count >= 100:
        break


data_target_file.close()