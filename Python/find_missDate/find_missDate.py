#!/usr/bin/python
#coding = UFT-8

# 输入：
# 输出：
# 功能：

import os
import datetime
import fnmatch

# 输入文件路径，文件类型，输出
def get_fileName(path,filetype):
    name =[]
    for root, dirs,files in os.walk(path):
        for i in files:
            if fnmatch.fnmatch(i,"P_GJGD_SZT_*"):
                if filetype in i:
                    name.append(i.replace(filetype,''))
    #print(name)
    return name

def get_dateName(start,end):
    #加入 datetime.timedelta(days=-1) 可以把日期提前一天
    name = []
    date_start = datetime.datetime.strptime(start,'%Y%m%d')+ datetime.timedelta(days=-1)
    date_end = datetime.datetime.strptime(end,'%Y%m%d')+ datetime.timedelta(days=-1)
    while date_start < date_end:
        date_start+=datetime.timedelta(days=1)
        # print(date_start)   #2016-06-03 00:00:00
        # 格式化输出时间格式
        date_name = date_start.strftime('%Y%m%d')
        name.append(date_name)
    # print(name)
    return name

def compare_name(name1,name2):
    # 输出 name2 中有而 name1 中没有的
    diff_name = list(set(name2).difference(set(name1)))
    return diff_name

#比较两个 txt 文件中内容并输出不同
def compare_file(path1,path2):
    # file1 填 fileName_path , file2 填 dateName_path
    name1 = []
    name2 = []
    f1 = open(path1, 'r')
    f2 = open(path2, 'r')

    #按行读入 txt ,并保存为 list 类型
    for line in f1.readlines():
        name1.append(list(map(int,line.split(','))))
        print(name1)
        f1.close()

    for line in f2.readlines():
        name2.append(list(map(int,line.split(','))))
        print(name2)
        f2.close()

    # 输出 name2 中有而 name1 中没有的
    diff_name=list(set(name2).difference(set(name1)))
    print(diff_name)

def rid_fileName(file_name):
    list_fileName = []
    for index in range(len(file_name)):
        str_fileNames = ''.join(file_name[index - 1])
        substr_fileName = str_fileNames.replace('P_GJGD_SZT_', '')
        list_fileName.append(substr_fileName)
    return list_fileName

def test():
    # path1 需要获取文件名的目录
    #path1 = '/parastor/backup/data/SZT/t30data'
    path1= 'C://Users//Administrator//Desktop'
    file_name=get_fileName(path1, '')
    rid_fName = rid_fileName(file_name)
    date_name = get_dateName('20160801', '20170801')

    diff_name = compare_name(rid_fName,date_name)
    print(diff_name)

    str_diffName = '\n'.join(diff_name)
    #diff_path = '/home/wuying/projects'
    diff_path = "C://Users//Administrator//Desktop//diff.txt"
    f = open(diff_path, 'w')
    f.write(str_diffName)
    f.close()

test()