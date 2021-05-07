# github-upload
==============================

Introduction

    The scripts in this project are used for text data preprocessing in the study of national self-reference. 
    Here we summarize the main purpose of each file.

1. Step2_PLOS_Parse.py
    This script mainly works on the parsing of XML file, and its function includes:
    * reading XML files;
    * normalizing section names;
    * processing text in each section;
    * extracting citances;
    * extracting several features of each citance;
    * counting the mentioned frequency of each reference in full-text of citing paper, etc.

2. Step3_PLOS_refer.py
    This script can get the detailed information of each reference from XML files, and the detailed information 
    include title, first author, corresponding author, publised year, source, volume, etc.
3. Step4_PLOS_preprocess.py
    This script finish the preprocessing of several tables stored in MySQL DB, and the process includes:
    * marking  papers used in Table 'PLOS_191204_paper', MySQL（matched = 1）;
    * querying the number of papers contatining four main sections;
    * normalizing the 'country' column in Table 'PLOS_191204_paper', MySQL.

Attention:
    Here we didn't upload the raw data and text data of PLOS papers. If you need it, we can share it by email.
