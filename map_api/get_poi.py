import requests

with open('text.txt',encoding='utf-8') as f:
    xiaoquname = f.readlines()

xiaoqu_list = [name.split()[0] for name in xiaoquname]
# print(xiaoqu_list)

url2="http://api.map.baidu.com/place/v2/suggestion?query={community}&region={city}&city_limit=true&output=json&ak=ORupmlhHCos57O4hfrXaO16aCzmk7Dup"

poi_data = open('poi_data.txt',mode='w+',encoding='utf-8')
poi_data.write('city_name district community_name lat lng'+'\n')

url1 = url2.format(community='富力桃园C区',city = '北京')
data = requests.get(url1)

location_info = eval(data.text)
print(location_info)

# for xiaoqu in xiaoqu_list:
#
#     url1 = url2.format(community=xiaoqu,city = '衡水')
#     data_treat = requests.get(url1)
#
#     location_info = eval(data_treat.text)
#
#     if location_info['result']:
#         first_loc = location_info['result'][0]
#         data_treat = first_loc['city']+' '+first_loc['district']+' '+first_loc['name']+' '+str(first_loc['location']['lat'])+' '+str(first_loc['location']['lng'])+'\n'
#         print(data_treat)
#         poi_data.write(data_treat)
#
#     else:
#         spe_data = '衡水'+xiaoqu+'\n'
#         print(spe_data)
#         poi_data.write(spe_data)
#
# poi_data.close()


