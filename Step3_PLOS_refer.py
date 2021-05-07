#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# created by Chen on 6 Jan, 2020____Getting the detail of references from XML files

from bs4 import BeautifulSoup
import re
import nltk.data
import pymysql.cursors
import sys

tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')

# Connect to the database
connection = pymysql.connect(host='localhost',
                             port=3306,
                             user='root',
                             password='xxxx',
                             db='Self_citation_PLOS',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)


# Detailed information: title, first author, corresponding author, publised year, source, volume, etc.
def refer_detail(soup):

    article_back = soup.find('back')
    doi = soup.find('article-id', attrs={"pub-id-type": "doi"}).text.strip()
    refer_list = []

    for i, refer in enumerate(article_back.find_all('ref')):
        refer_temp = {}
        refer_temp['citing_doi'] = doi
        refer_temp['plos_id'] = refer['id']
        if refer.find('label'):
            refer_temp['rid'] = refer.find('label').text
        else:
            refer_temp['rid'] = str(i + 1)
        element = refer.find(re.compile(r'element-citation|mixed-citation|nlm-citation'))
        try:
            refer_temp['pub_type'] = element['publication-type']
        except:
            refer_temp['pub_type'] = 'unknown'

        if element.find('person-group', attrs={"person-group-type": "author"}) is not None:
            if element.find('person-group', attrs={"person-group-type": "author"}).find('name') is not None:
                au_group = element.find('person-group', attrs={"person-group-type": "author"}).find('name')
                refer_temp['1st_author'] = au_group.get_text(' ', strip=True)
            else:
                au_group = element.find('person-group', attrs={"person-group-type": "author"}).find('collab')
                refer_temp['1st_author'] = au_group.get_text(strip=True)
        elif element.find('name') is not None and element.find('person-group', attrs={"person-group-type": "editor"}) is None:
            refer_temp['1st_author'] = element.find('name').get_text(' ', strip=True)
        elif element.find('collab') is not None:
            refer_temp['1st_author'] = element.find('collab').text
        else:
            refer_temp['1st_author'] = ''
        try:
            title = element.find('article-title').get_text(strip=True)
            if re.search(r'\.$', title):
                refer_temp['title'] = re.sub(r'\.$', '', title)
            else:
                refer_temp['title'] = title
        except:
            refer_temp['title'] = ''
        try:
            refer_temp['py'] = int(element.year.text)
        except:
            refer_temp['py'] = 0
        try:
            refer_temp['source'] = element.source.get_text(strip=True)
        except:
            refer_temp['source'] = ''
        try:
            refer_temp['vol'] = element.volume.text
        except:
            refer_temp['vol'] = ''
        try:
            refer_temp['fpage'] = element.fpage.text
        except:
            refer_temp['fpage'] = ''
        try:
            refer_temp['publisher'] = element.find('publisher-name').get_text(strip=True)
        except:
            refer_temp['publisher'] = ''

        refer_list.append(refer_temp)


    return refer_list


sql_detail = """Insert into PLOS_191204_referdetails
                 values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""

def main():
    uid_refer = 11689714
    unmatch = 0
    error_paper = 0
    error_order = []

    for i in range(1, 7780):         # 这里的第二位数字记得加1   3000490
        print('The result of article', i, '-------------------------')
        s_num = str(i).zfill(7)
        xml_path = '/Users/chenliyue/PycharmProjects/allofplos_xml/journal.pntd.' + s_num + '.xml'
        try:
            root_dot = BeautifulSoup(open(xml_path), "xml")
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

        if root_dot.find('subj-group', attrs={"subj-group-type": "heading"}).text.strip() == 'Research Article':
            refer_list = refer_detail(root_dot)
            other_num = 0
            for refer in refer_list:
                # print(refer)
                if refer['source'] == '' and refer['vol'] == '' and refer['fpage'] == '':
                    other_num = other_num + 1
            ratio = round(other_num/len(refer_list)*100, 3)
            if ratio >=50:
                unmatch = unmatch + 1

        try:
            uid_temp1 = uid_refer
            cursor = connection.cursor()
            for refer in refer_list:
                try:
                    cursor.execute(sql_detail, (uid_refer, refer['citing_doi'], refer['plos_id'], refer['pub_type'], refer['rid'], refer['1st_author'],
                                                refer['py'], refer['title'], refer['source'],refer['vol'], refer['fpage'], refer['publisher']))
                    uid_refer = uid_refer + 1
                except:
                    print(refer)
                    print("Unexpected error:", sys.exc_info()[0])
                    print('There is a inserting problem ~~~~~~~~~~~~~~~~~~~~~')
                    continue
            connection.commit()
            cursor.close()
            print('Citance import success for article', i)
        except:
            connection.rollback()
            print("Unexpected error:", sys.exc_info()[0])
            print('Citance import failed!')
            print('Uid of citance is: ', uid_temp1)
            continue


    print(error_order)
    print('The number of error paper is ', error_paper)
    print('The number of undermatch paper is ', unmatch)

if __name__ == '__main__':
    main()
