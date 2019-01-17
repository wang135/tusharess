import json
import requests
from bs4 import BeautifulSoup
import time
import re
import pymysql.cursors
import pymysql

connection = pymysql.connect(host='58edd9c77adb6.bj.cdb.myqcloud.com',
                             port=5432,
                             user='root',
                             password='1160329981wang',
                             db='qianmancang',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)
# lists = ["zzgz_qh","szgz_qh" "sngz_qh","gz_qh","engz_qh","qz_qh","cff","yy_qh","rzjb_qh","zj_qh","lq_qh","hj_qh",
#         "by_qh","xing_qh","ry_qh","xj_qh","lv_qh","tong_qh","xc_qh","lwg_qh","qian_qh","ni_qh","xi_qh","zly_qh","jbx_qh","xwb_qh","jhb_qh","jd_qh","tks_qh",
#           "jm_qh","pvc_qh","yec_qh","lldpe_qh","dy_qh","dd_qh","hym_qh","de_qh","dp_qh","zly_qh","jt_qh","ymdf_qh","dsq_qh",
#           "zc_qh","gt_qh","ms_qh","xpg_qh","mg_qh","wxd_qh","jdm_qh","dlm_qh","bl_qh","mh_qh","czy_qh","czp_qh","ycz_qh","pta_qh","bst_qh"
#         ,"qm_qh","zxd_qh"]
lists = ["jm_qh", "pvc_qh", "yec_qh", "lldpe_qh", "dy_qh", "dd_qh", "hym_qh", "de_qh", "dp_qh", "zly_qh", "jt_qh",
         "ymdf_qh", "dsq_qh",
         "zc_qh", "gt_qh", "ms_qh", "xpg_qh", "mg_qh", "wxd_qh", "jdm_qh", "dlm_qh", "bl_qh", "mh_qh", "czy_qh",
         "czp_qh", "ycz_qh", "pta_qh", "bst_qh"
    , "qm_qh", "zxd_qh"]
for wz in lists:

    r = requests.get(
        'http://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/Market_Center.getHQFuturesData?page=1&num=40&sort=symbol&asc=1&node=' + wz + '&_s_r_a=init')
    time.sleep(3)
    infos = r.text

    p1 = re.compile(r'[{](.*?)[}]', re.S)
    p2 = re.compile(r':.*', re.S)
    list_all = re.findall(p1, infos)
    for jsons in list_all:
        list_jsons = jsons.split(',')
        codess = list_jsons[0]
        codes = re.findall(p2, codess)  # 选区#后面的字符串
        code_re = codes[0].replace(':', '')  # 去掉#
        code = re.sub('"', '', code_re)  # 去掉“”

        marketss = list_jsons[1]
        markets = re.findall(p2, marketss)  # 选区#后面的字符串
        market_re = markets[0].replace(':', '')  # 去掉#
        market = re.sub('"', '', market_re)  # 去掉“”

        namess = list_jsons[3]
        names = re.findall(p2, namess)  # 选区#后面的字符串
        name_re = names[0].replace(':', '')  # 去掉#
        name = re.sub('"', '', name_re)  # 去掉“”

        contractss = list_jsons[2]
        contracts = re.findall(p2, contractss)  # 选区#后面的字符串
        contract_re = contracts[0].replace(':', '')  # 去掉#
        contract = re.sub('"', '', contract_re)  # 去掉“”
        print(code, market, contract, name)
        rs = requests.get(
            'https://stock2.finance.sina.com.cn/futures/api/jsonp.php/var%20_P02019_1_11=/InnerFuturesNewService.getDailyKLine?symbol=' + code + '&_=2019_1_15')
        time.sleep(3)
        infos = rs.text
        p1 = re.compile(r'[[](.*?)[]]', re.S)
        p2 = re.compile(r':.*', re.S)
        list_all = re.findall(p1, infos)
        try:
            listall = list_all[0].split(',')
            listall = listall[-6:]
            num = int(len(listall) / 6)
            for i in range(num):
                timess = listall[6 * i]
                times = re.findall(p2, timess)  # 选区#后面的字符串
                times_re = times[0].replace(':', '')  # 去掉#
                times = re.sub('"', '', times_re)  # 去掉“”

                opensss = listall[6 * i + 1]
                openss = re.findall(p2, opensss)  # 选区#后面的字符串
                opens_re = openss[0].replace(':', '')  # 去掉#
                opens = re.sub('"', '', opens_re)  # 去掉“”

                highsss = listall[6 * i + 2]
                highss = re.findall(p2, highsss)  # 选区#后面的字符串
                highs_re = highss[0].replace(':', '')  # 去掉#
                highs = re.sub('"', '', highs_re)  # 去掉“”

                lowsss = listall[6 * i + 3]
                lowss = re.findall(p2, lowsss)  # 选区#后面的字符串
                lows_re = lowss[0].replace(':', '')  # 去掉#
                lows = re.sub('"', '', lows_re)  # 去掉“”

                closesss = listall[6 * i + 4]
                closess = re.findall(p2, closesss)  # 选区#后面的字符串
                closes_re = closess[0].replace(':', '')  # 去掉#
                closes = re.sub('"', '', closes_re)  # 去掉“”

                volumesss = listall[6 * i + 5]
                volumess = re.findall(p2, volumesss)  # 选区#后面的字符串
                volumes_re = volumess[0].replace(':', '')  # 去掉#
                volumes1 = re.sub('"', '', volumes_re)  # 去掉“”
                volumes = re.sub('}', '', volumes1)

                print(times, opens, highs, lows, closes, volumes)
                try:
                    with connection.cursor() as cursor:

                        # 执行sql语句，插入记录
                        SQL = """insert into qihuoday(times, opens, closes, high, low, volume, contract, market, codes, namess)
                        values
                        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                        cursor.execute(SQL, (times, opens, closes, highs, lows, volumes, contract, market, code, name))
                        # 没有设置默认自动提交，需要主动提交，以保存所执行的语句
                        connection.commit()
                except Exception as e:
                    print('***** Logging failed with this error:', str(e))
        except:
            print('wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww')