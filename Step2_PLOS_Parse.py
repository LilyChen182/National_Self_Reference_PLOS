#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# created by Chen on 6 Dec, 2019____step2 Paper parse and import into mysql

from bs4 import BeautifulSoup
import re
import nltk.data
import pymysql.cursors
import sys
# from mysqlDB_connection import *


tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')

# Connect to the database
connection = pymysql.connect(host='localhost',
                             port=3306,
                             user='root',
                             password='xxxx',
                             db='Self_citation_PLOS',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


def read_xml(path):
    """
    Parse soup from xml path
    Return the root of the XML file
    """
    with open(path, "r") as f:
        text = f.read()
        text_new1 = re.sub(r'<bold>|</bold>|</italic>|<italic>', '', text)
        text_new2 = re.sub(r'c\.f\.|e\.g\.|e\. g\. |etc\.|i\.e\.|cf\.|ibid\.|viz\.', 'for example', text_new1)
        text_new3 = re.sub(r'et al\.', 'et al', text_new2)
        text_new4 = re.sub(r'Fig. |Figs. ', 'Fig', text_new3)
        with open(path, "w") as s:
            s.write(text_new4)
    try:
        soup = BeautifulSoup(open(path), "xml")


    except:
        soup = 'No path'
        print("Error: it was not able to read a path!")
    return soup


def title_normal(title):
    title_new = title
    if re.search(r'introduc|inrtoduc|background|motivation|objective|main text|overview|literature review|^related work|^theory', title, re.I):
        title_new = 'Introduction'
    elif re.search(r'supporting information', title, re.I):
        title_new = 'Supporting Information'
    elif re.search(r'results and discussion|result and discussion|results & discussion|discussion and result|analysis and discussion|results\/discussion', title, re.I):
        title_new = 'Results and Discussion'
    elif re.search(r'method and result|methods and result|models and result|model and result', title, re.I):
        title_new = 'Method and Results'
    elif re.search(r'discussions and conclusion|discussion and conclusion', title, re.I):
        title_new = 'Discussion and Conclusion'
    elif re.search(r'result|analysis|analyses|finding|^description', title, re.I):
        title_new = 'Results'
    elif re.search(r'discussion|disscussion|dicussion|disucussion|dsicussion|disscusion', title, re.I):
        title_new = 'Discussion'
    elif re.search(r'conclusion|limitation|future|concluding remark|summary', title, re.I):
        title_new = 'Conclusion'
    elif re.search(r'method|material|experimental procedure|^data|data$|solution|model|guideline|^design|design$|^simulation|^algorithm|algorithm$', title, re.I):
        title_new = 'Materials and Methods'
    elif re.search(r'^appendix|^appendice', title, re.I):
        title_new = 'Appendix'
    elif re.search(r'^acknowle', title, re.I):
        title_new = 'Acknowledgement'
    elif re.search(r'^experiment|^study |experiment&', title, re.I):
        title_new = 'Experiment'


    return title_new


def parse_section_citance(soup):
    """
    Parsed the body of aritcles and saved the whole article as a dict list.
    :param soup:
    :return dict_list, allsent_num, doi:
    """
    sec_list = []
    for child in soup.body.children:
        try:
            if child.name !='sec':
                continue
            else:
                sec_list.append(child['id'])
        except:
            continue
    article_body = soup.find('body')
    article_back = soup.find('back')
    section_id = re.compile('s\d$')
    allsent_num = 0
    dict_list = []
    refer_list = []
    doi = soup.find('article-id', attrs={"pub-id-type": "doi"}).text.strip()

    # renamed the reference label[X] to [referX]; saved full-text as dictionary list, article_body.find_all('sec', attrs={"id": section_id})
    for sec_num in sec_list:
        if len(sec_num) >=10:
            continue
        child = article_body.find('sec', attrs={"id": sec_num})
        dict_temp = {}
        dict_temp['Sec_name'] = child.find('title').text
        dict_temp['Sec_nm'] = title_normal(dict_temp['Sec_name'])
        dict_temp['Sent_set'] = []
        for element in child.find_all('p'):
            for cite in element.find_all('xref', attrs={"ref-type": "bibr"}):
                cite_num = cite.text.replace('[','').replace(']','')
                cite.string = '[refer' + cite_num + ']'
            all_text = element.text
            next_text = re.sub(r'refersee ref\. |refersee also ref\. |reviewed in refs\. ', 'refer', all_text)
            then_text = re.sub(r'\[referrefer', '[refer', next_text)
            new_text = re.sub(r'refs\. |ref\. ', 'refer', then_text)
            tokens = tokenizer.tokenize(new_text)
            allsent_num = allsent_num + len(tokens)
            dict_temp['Sent_set'] = dict_temp['Sent_set'] + tokens
        dict_list.append(dict_temp)

    for i, refer in enumerate(article_back.find_all('ref')):
        refer_temp = {}
        if refer.find('label'):
            refer_temp['refer_id'] = refer.find('label').text
        else:
            refer_temp['refer_id'] = str(i + 1)
        try:
            refer_temp['refer_title'] = refer.find('article-title').text
        except:
            refer_temp['refer_title'] = ''
        try:
            refer_temp['refer_year'] = int(refer.find('year').text)
        except:
            refer_temp['refer_year'] = 0
        refer_list.append(refer_temp)

    return dict_list, allsent_num, doi, refer_list


def parse_citance_feature(dict_list, sent_num, doi, wos):
    """
    :param dict_list:
    :param sent_num:
    :param doi:
    :param wos:
    :return citance_dict_list:
    """
    citance_dict_list = []
    index_temp = 0
    pattern_s = re.compile(r'\[refer\d+\]')
    pattern_p = re.compile(r'\[refer\d+\]–\[refer\d+\]')
    pattern = re.compile(r'\d+')

    for i, section in enumerate(dict_list):
        for j, sent in enumerate(section['Sent_set']):                      # enumerate从0开始计数
            if re.search(pattern_s, sent):
                citance_dict = {}
                citance_dict['Sec_name'] = section['Sec_name']              # 章节名称
                citance_dict['Sec_nm'] = section['Sec_nm']                  # 章节名称规范化
                citance_dict['Sec_num'] = i + 1                             # 章节序号
                citance_dict['Sent_id'] = index_temp + j + 1                # 引用句的编号
                citance_dict['Text_per'] = round((index_temp + j + 1)/sent_num*100, 3)    # 引用句的text percentage位置
                citance_dict['Sent_content'] = sent                         # 引用句的文本
                citance_dict['Citing_doi'] = doi                            # 施引文献的DOI号
                citance_dict['Citing_wos'] = wos
                citance_dict['refer_list'] = []                             # 引用句中的参考文献序号list
                if re.search(pattern_p, sent):                              # 引用句中的参考文献数量
                    p_num = 0
                    p_list = pattern_p.findall(sent)
                    for member in p_list:
                        temp = pattern.findall(member)
                        p_num = p_num + int(temp[1]) - int(temp[0]) + 1
                        for num in range(int(temp[0]), int(temp[1])+1):
                            citance_dict['refer_list'].append(num)
                    sent_temp = re.sub(pattern_p, "", sent)
                    if re.search(pattern_s, sent_temp):
                        s_temp = pattern_s.findall(sent_temp)
                        citance_dict['Bib_num'] = p_num + len(s_temp)
                        for num in s_temp:
                            citance_dict['refer_list'].append(int(pattern.findall(num)[0]))
                    else:
                        citance_dict['Bib_num'] = p_num
                else:
                    s_temp = pattern_s.findall(sent)
                    citance_dict['Bib_num'] = len(s_temp)
                    for num in s_temp:
                        citance_dict['refer_list'].append(int(pattern.findall(num)[0]))

                citance_dict_list.append(citance_dict)
            else:
                continue
        index_temp = index_temp + len(section['Sent_set'])

    return citance_dict_list


def preprocess_citance(citance_dict_list, refer_list):
    """
    列举每个参考文献及其基本特征，同时预处理引用句中的引用点为统一标识符（TREF, TGREF）
    :param citance_dict_list:
    :return citance_dict_new:
    """
    citance_dict_new = []
    pattern_p = re.compile(r'\[refer\d+\]–\[refer\d+\]')
    pattern_n = re.compile(r'\d+')
    for dict in citance_dict_list:
        text = dict['Sent_content']
        group_refer = re.search(pattern_p, text)
        if group_refer:
            for suite in re.findall(pattern_p, text):
                group_temp = ''
                num_list = re.findall(pattern_n, suite)
                for e in range(int(num_list[0]), int(num_list[1])+1):
                    if e == int(num_list[1]):
                        group_temp = group_temp + 'refer' + str(e)
                    else:
                        group_temp = group_temp + 'refer' + str(e) + ', '
                patt = re.compile('\[refer' + num_list[0] + '\]–\[refer' + num_list[1] + '\]')
                text = re.sub(patt, '['+group_temp+']', text)
        text_new = re.sub(r'(?<=\d)\], \[refer|(?<=\d)\],\[refer', ', refer', text)
        pattern = re.compile(r'\[refer.*?\]')
        bracket_list = pattern.findall(text_new)

        if len(dict['refer_list']) == len(set(dict['refer_list'])):
            for refer in dict['refer_list']:  #修改！！
                temp_dict = {}
                temp_dict['Bib_title'] = ''
                temp_dict['Bib_year'] = 0
                for babe in refer_list:
                    if babe['refer_id'] == str(refer):
                        temp_dict['Bib_title'] = babe['refer_title']
                        temp_dict['Bib_year'] = babe['refer_year']
                temp_dict['Sec_name'] = dict['Sec_name']
                temp_dict['Sec_nm'] = dict['Sec_nm']
                temp_dict['Sec_num'] = dict['Sec_num']
                temp_dict['Bib_id'] = refer
                temp_dict['Bib_all'] = len(dict['refer_list'])
                temp_dict['Sent_id'] = dict['Sent_id']
                temp_dict['Text_per'] = dict['Text_per']
                temp_dict['Citing_doi'] = dict['Citing_doi']
                temp_dict['Target_UT'] = dict['Citing_wos']
                temp_dict['UT_bib'] = dict['Citing_wos'] + '_' + str(refer)
                for bracket in bracket_list:
                    RT1 = 'refer'+str(refer)+','
                    RT2 = 'refer'+str(refer)+']'
                    if (RT1 in bracket or RT2 in bracket) and ',' in bracket:
                        temp_dict['Group_num'] = bracket.count(',')+1
                        temp_dict['Sent_content'] = text_new.replace(bracket, '[TGREF]')
                    elif (RT1 in bracket or RT2 in bracket) and ',' not in bracket:
                        temp_dict['Group_num'] = 1
                        temp_dict['Sent_content'] = text_new.replace(bracket, '[TREF]')

                if len(temp_dict) == 14:
                    citance_dict_new.append(temp_dict)
        else:
            refer_temp1 = []            # 唯一出现
            refer_temp2 = []            # 重复出现
            for refer in dict['refer_list']:
                if dict['refer_list'].count(refer) == 1:
                    refer_temp1.append(refer)
                else:
                    refer_temp2.append(refer)

            for refer1 in refer_temp1:
                temp_dict = {}
                temp_dict['Bib_title'] = ''
                temp_dict['Bib_year'] = 0
                for babe in refer_list:
                    if babe['refer_id'] == str(refer1):
                        temp_dict['Bib_title'] = babe['refer_title']
                        temp_dict['Bib_year'] = babe['refer_year']
                temp_dict['Sec_name'] = dict['Sec_name']
                temp_dict['Sec_nm'] = dict['Sec_nm']
                temp_dict['Sec_num'] = dict['Sec_num']
                temp_dict['Bib_id'] = refer1
                temp_dict['Bib_all'] = len(dict['refer_list'])
                temp_dict['Sent_id'] = dict['Sent_id']
                temp_dict['Text_per'] = dict['Text_per']
                temp_dict['Citing_doi'] = dict['Citing_doi']
                temp_dict['Target_UT'] = dict['Citing_wos']
                temp_dict['UT_bib'] = dict['Citing_wos'] + '_' + str(refer1)
                for bracket in bracket_list:
                    RT1 = 'refer'+str(refer1)+','
                    RT2 = 'refer'+str(refer1)+']'
                    if (RT1 in bracket or RT2 in bracket) and ',' in bracket:
                        temp_dict['Group_num'] = bracket.count(',')+1
                        temp_dict['Sent_content'] = text_new.replace(bracket, '[TGREF]')
                    elif (RT1 in bracket or RT2 in bracket) and ',' not in bracket:
                        temp_dict['Group_num'] = 1
                        temp_dict['Sent_content'] = text_new.replace(bracket, '[TREF]')
                if len(temp_dict) == 14:
                    citance_dict_new.append(temp_dict)

            for refer2 in set(refer_temp2):
                seq = 'refer'+str(refer2)
                pattern_refer2 = re.compile('refer' + str(refer2) + '(?!\d)')
                temp_list = pattern_refer2.split(text_new)
                for i in range(0,refer_temp2.count(refer2)):
                    temp_dict = {}
                    temp_dict['Bib_title'] = ''
                    temp_dict['Bib_year'] = 0
                    for babe in refer_list:
                        if babe['refer_id'] == str(refer2):
                            temp_dict['Bib_title'] = babe['refer_title']
                            temp_dict['Bib_year'] = babe['refer_year']
                    temp_dict['Sec_name'] = dict['Sec_name']
                    temp_dict['Sec_nm'] = dict['Sec_nm']
                    temp_dict['Sec_num'] = dict['Sec_num']
                    temp_dict['Bib_id'] = refer2
                    temp_dict['Bib_all'] = len(dict['refer_list'])
                    temp_dict['Sent_id'] = dict['Sent_id']
                    temp_dict['Text_per'] = dict['Text_per']
                    temp_dict['Citing_doi'] = dict['Citing_doi']
                    temp_dict['Target_UT'] = dict['Citing_wos']
                    temp_dict['UT_bib'] = dict['Citing_wos'] + '_' + str(refer2)

                    if re.search(r'\[$', temp_list[i]) and re.search(r'^\]', temp_list[i+1]):
                        temp_part = []
                        temp_part.append(temp_list[i] + 'TREF' + temp_list[i+1])
                        temp_sent = temp_list[0:i] + temp_part  + temp_list[i+2:]
                        temp_dict['Group_num'] = 1
                        temp_dict['Sent_content'] = seq.join(temp_sent)
                        if len(temp_dict) == 14:
                            citance_dict_new.append(temp_dict)
                    else:
                        # 需要匹配前后两个字符串末尾的参考文献情况，并替换它们，同时统计group number
                        try:
                            temp_part = []
                            if re.search(r'\[$', temp_list[i]):
                                if re.search(r'.*?\]', temp_list[i+1]) is None:
                                    temp_dict['Group_num'] = len(re.findall(r'refer\d', temp_list[i+1])) + 1
                                    temp_part.append(temp_list[i] + 'TGREF]' + temp_list[i+1])
                                else:
                                    temp_dict['Group_num'] = re.search(r'.*?\]', temp_list[i+1]).group().count(',')+1
                                    temp_part.append(temp_list[i] + re.sub(r'.*?\]', 'TGREF]', temp_list[i+1]))
                            elif re.search(r'^\]', temp_list[i+1]):
                                text_fresh = temp_list[i]
                                if temp_list[i].count('[') >=2:
                                    for j in range(1,temp_list[i].count('[')):
                                        text_fresh = re.sub(r'\[', '',temp_list[i], count=1)
                                search_str = re.search(r'\[.*?$', text_fresh).group()
                                temp_dict['Group_num'] = search_str.count(',')+1
                                temp_part.append(temp_list[i].replace(search_str, '[TGREF') + temp_list[i+1])
                            else:
                                if temp_list[i].count('[') >=2:
                                    for j in range(1,temp_list[i].count('[')):
                                        text_fresh = re.sub(r'\[', '',temp_list[i], count=1)
                                else:
                                    text_fresh = temp_list[i]

                                search_str1 = re.search(r'\[.*?$', text_fresh).group()
                                if re.search(r'.*?\]', temp_list[i+1]) is None:
                                    search_str3 = len(re.findall(r'refer\d', temp_list[i+1])) + 1
                                    temp_dict['Group_num'] = search_str1.count(',') + search_str3
                                    temp_part.append(temp_list[i].replace(search_str1, '[TGREF') + re.sub(r'refer\d+\,|refer\d', ']', temp_list[i+1]))
                                else:
                                    search_str2 = re.search(r'.*?\]', temp_list[i+1]).group()
                                    temp_dict['Group_num'] = search_str1.count(',') + search_str2.count(',') + 1
                                    temp_part.append(temp_list[i].replace(search_str1, '[TGREF') + temp_list[i+1].replace(search_str2, ']'))

                            temp_sent = temp_list[0:i] + temp_part  + temp_list[i+2:]
                            temp_dict['Sent_content'] = seq.join(temp_sent)
                            if len(temp_dict) == 14:
                                citance_dict_new.append(temp_dict)
                        except:
                            pass

    return citance_dict_new


def frequency_stat(bib_list, wos):
    """
    :param bib_list:
    :return each bibx and its frequency:
    """
    bib_freq = []
    bib_all = [i['Bib_id'] for i in bib_list]
    for bib_unique in set(bib_all):
        freq = bib_all.count(bib_unique)
        bib_dict = {'Bib_id': bib_unique,
                    'Frequency': freq,
                    'UT': wos,
                    'UT_bib': wos + '_' + str(bib_unique)}
        bib_freq.append(bib_dict)
    return bib_freq





sql_citance = """Insert into PLOS_191204_citance2
                 values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

sql_frequency = """Insert into PLOS_191204_frequency2
                   values(%s,%s,%s,%s,%s)"""

sql_location = """Insert into PLOS_191204_location2
                  values(%s,%s,%s,%s,%s)"""


def main():
    uid_citance = 2718730
    uid_bib = 1597083
    uid_loca = 134188
    error_paper = 0
    error_order = []
    for i in range(1, 3000489):         # 这里的第二位数字记得加1
        print('The result of article', i, '-------------------------')
        s_num = str(i).zfill(7)
        xml_path = '/Users/chenliyue/PycharmProjects/allofplos_xml/journal.pbio.' + s_num + '.xml'
        try:
            root_dot = read_xml(xml_path)
        except:
            print('No.', i, ' paper is not exist!!!!!')
            error_paper = error_paper + 1
            error_order.append(i)
            continue
        if root_dot.find('subj-group', attrs={"subj-group-type": "heading"}).text.strip() != 'Research Article':
            print('This is a ', root_dot.find('subj-group', attrs={"subj-group-type": "heading"}).text.strip())
            error_paper = error_paper + 1
            error_order.append(i)
            continue

        cursor = connection.cursor()
        try:
            if root_dot.find('subj-group', attrs={"subj-group-type": "heading"}).text.strip() == 'Research Article':
                dict_list, sent_num, paper_doi, refer_list = parse_section_citance(root_dot)

                sql_wos = """select UT from PLOS_191204_Paper
                             where doi = '%s'"""

                cursor.execute(sql_wos%paper_doi)
                try:

                    wos = cursor.fetchall()[0]['UT'].replace("WOS:", "")
                except:
                    wos = s_num
            if dict_list == []:
                print('Text body can not read normally!')
                error_paper = error_paper + 1
                error_order.append(i)
                continue

            citance_dict_list = parse_citance_feature(dict_list, sent_num, paper_doi, wos)
            citance_dict_full = preprocess_citance(citance_dict_list, refer_list)
            bib_frequency = sorted((frequency_stat(citance_dict_full, wos)),key = lambda e:(e.__getitem__('Bib_id')))
            cursor.close()
            for citance in citance_dict_full:
                if len(citance) != 14:
                    print(citance)

        except:
            connection.rollback()
            cursor.close()
            dict_list, sent_num, paper_doi, refer_list = parse_section_citance(root_dot)
            print(dict_list)
            print("Unexpected error:", sys.exc_info()[0])
            print('Citance import failed!')
            continue


        try:
            uid_temp1 = uid_citance
            uid_temp2 = uid_bib
            uid_temp3 = uid_loca
            cursor = connection.cursor()
            for citance in citance_dict_full:
                try:
                    cursor.execute(sql_citance, (uid_citance, citance['Bib_title'], citance['Bib_year'], citance['Sec_name'], citance['Sec_nm'], citance['Sec_num'], citance['Bib_id'], citance['Sent_id'], citance['Sent_content'],
                                     citance['Text_per'], citance['Target_UT'], citance['UT_bib'], citance['Group_num'], citance['Bib_all']))
                    uid_citance = uid_citance + 1
                except:
                    print('There is a coding problem.')
                    continue
            connection.commit()
            cursor.close()
            print('Citance import success for article', i)
        except:
            connection.rollback()
            print("Unexpected error:", sys.exc_info()[0])
            print('Citance import failed!')
            print('Uid of citance is: ', uid_temp1)
            print('Uid of bib is: ', uid_temp2)
            print('Uid of location is: ', uid_temp3)
            continue

        try:
            cursor = connection.cursor()
            for k, location in enumerate(dict_list):
                cursor.execute(sql_location, (uid_loca, wos, location['Sec_name'], location['Sec_nm'], str(k+1)))
                uid_loca = uid_loca + 1
            connection.commit()
            cursor.close()
            print('Bib location import success for article', i)
        except:
            connection.rollback()
            print("Unexpected error:", sys.exc_info()[0])
            print('Bib location import failed!')
            print('Uid of citance is: ', uid_temp1)
            print('Uid of bib is: ', uid_temp2)
            print('Uid of location is: ', uid_temp3)
            continue

        try:
            cursor = connection.cursor()
            for bib in bib_frequency:
                cursor.execute(sql_frequency, (uid_bib, bib['Bib_id'], bib['Frequency'], bib['UT'], bib['UT_bib']))
                uid_bib = uid_bib + 1
            connection.commit()
            cursor.close()
            print('Bib frequency import success for article', i)
        except:
            connection.rollback()
            print("Unexpected error:", sys.exc_info()[0])
            print('Bib frequency import failed!')
            print('Uid of citance is: ', uid_temp1)
            print('Uid of bib is: ', uid_temp2)
            print('Uid of location is: ', uid_temp3)
            raise

    print(error_paper)
    print(error_order)


# if __name__ == '__main__':
#     main()



