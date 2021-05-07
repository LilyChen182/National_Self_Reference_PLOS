#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# created by Chen on 9 Jan, 2020____Preproccessing of several tables stored in Mysql database (inquiring and updating)

import pymysql.cursors
import pickle

# Connect to the database
connection = pymysql.connect(host='localhost',
                             port=3306,
                             user='root',
                             password='laochen92!',
                             db='Self_citation_PLOS',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


def save_data(data_list, file_name):
    file_path = '/Users/chenliyue/Desktop/PLOS_pknew/' + file_name
    with open(file_path, 'wb+') as f:
        pickle.dump(data_list, f)

def read_data(file_name):
    df = open('/Users/chenliyue/Desktop/PLOS_pknew/' + file_name, 'rb')
    data1 = pickle.load(df)
    df.close()
    return data1


# 1 Marking papers used in Table 'PLOS_191204_paper', MySQL（matched = 1）--------------------------------
# cursor = connection.cursor()
#
# sql_wos = """select id from PLOS_191204_Paper
#              where exists(
#                 select * FROM PLOS_191204_citance2
#                 where target_UT = PLOS_191204_Paper.ut)
#           """
# sql_wos1 = """select id from PLOS_191204_Paper"""
#
# cursor.execute(sql_wos1)
#
# wos = cursor.fetchall()
#
# citance1 = []
# for role in wos:
#     citance1.append(role['id'])
#
# save_data(citance1, 'allid.pk')
#
#
# id_list1 = read_data('citance1.pk')
# id_list2 = read_data('citance2.pk')
# id_all = read_data('allid.pk')
#
# subset_list = id_list1 + id_list2
#
# differ_list = list(set(id_all).difference(set(subset_list)))
#
# sql_update = """update PLOS_191204_paper set matched = 0
#                 where id = %s"""
#
# cursor = connection.cursor()
# for num in differ_list:
#     cursor.execute(sql_update, num)
# connection.commit()
# cursor.close()


# 2 Querying the number of papers contatining four main sections -------------------------
# sql_location = """select id, target_UT, loca_normal from PLOS_191204_location1"""
# sql_UT = """select distinct target_UT from PLOS_191204_location1"""
#
# cursor = connection.cursor()
# cursor.execute(sql_location)
# loca_list = cursor.fetchall()
# cursor.execute(sql_UT)
# UT_list = cursor.fetchall()
#
# match_all = []
# for item in UT_list:
#     match_all.append(item['target_UT'])
#
# UT = '000207443600001'
# num = 0
# match1_list = []
# for unit in loca_list:
#     if unit['target_UT'] == UT:
#        if unit['loca_normal'] in ('Introduction', 'Results', 'Discussion', 'Conclusion', 'Results and Discussion', 'Materials and Methods', 'Discussion and Conclusion'):
#            num = num + 1
#     else:
#         UT = unit['target_UT']
#         num = 0
#         if unit['loca_normal'] in ('Introduction', 'Results', 'Discussion', 'Conclusion', 'Results and Discussion', 'Materials and Methods', 'Discussion and Conclusion'):
#            num = num + 1
#
#     if num == 3 and unit['target_UT'] not in match1_list:
#         match1_list.append(unit['target_UT'])
#         print(unit['id'], 'is done!')


# UT = '000207443600001'
# num = 0
# match2_list = []
# for unit in loca_list:
#     if unit['target_UT'] == UT:
#        if unit['loca_normal'] in ('Introduction', 'Results', 'Discussion', 'Materials and Methods', 'Conclusion'):
#            num = num + 1
#     else:
#         UT = unit['target_UT']
#         num = 0
#         if unit['loca_normal'] in ('Introduction', 'Results', 'Discussion', 'Materials and Methods', 'Conclusion'):
#            num = num + 1
#
#     if num == 4 and unit['target_UT'] not in match2_list:
#         match2_list.append(unit['target_UT'])
#         print(unit['id'], 'is done!')

# list_all = match1_list
# print(len(list(set(match_all).difference(set(list_all)))))
# print(list(set(match_all).difference(set(list_all))))


# 3 Normalizing the 'country' column in Table 'PLOS_191204_paper', MySQL ------------------

sql_paper = """select Id, UT from PLOS_191204_paper                         
                where Spain = 3 """                   # （1) Modifying the cloumn name

# Query the country of the first author
sql_ins = """select CountryNorm from PLOS_191204_Institute
             where UT = %s
             order by id asc limit 1 """

# Query the country of the corresponding author
sql_reprint = """select CountryNorm from PLOS_191204_ReprintAuthor
                 where UT = %s"""


sql_update = """update PLOS_191204_paper set Spain = %(type)s
                where Id = %(Id)s"""                  # （2）Modifying the column name after set

cursor = connection.cursor()
cursor.execute(sql_paper)
UT_dict = cursor.fetchall()
cursor.close()

id_list = []
UT_list = []
for role in UT_dict:
    UT_list.append(role['UT'])
    id_list.append(role['Id'])

cursor = connection.cursor()
dict_temp = {}
for i, element in enumerate(UT_list):
    dict_temp['Id'] = id_list[i]
    cursor.execute(sql_ins%element)
    try:
        FC = cursor.fetchall()[0]['CountryNorm']
    except:
        FC = ''
    cursor.execute(sql_reprint%element)
    try:
        CC = cursor.fetchall()[0]['CountryNorm']
    except:
        CC = ''
    if FC == 'Spain' or CC == 'Spain':                 # （3) Modifying the country
        dict_temp['type'] = 2
        cursor.execute(sql_update, dict_temp)
    if i % 500 == 0:
        print('No. ', i, ' finished!')


connection.commit()
cursor.close()

