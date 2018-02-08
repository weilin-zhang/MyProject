# !/usr/bin/python
# coding = UFT-8

# 输入： 文件路径
# 输出： [[6640996, 18308347], ['allodclear.txt', 'allpath.txt']]   文件大小（KB），文件名
# 功能： 获取子目录下的所有文件大小与文件名


import os
from os.path import join, getsize

def getdirsize(dir):
    size = []
    fileName = []
    for root, dirs, files in os.walk(dir):
        size1 = [getsize(join(root, name)) for name in files]
        for i in files:
            fileName.append(i)

        str_fileName = ','.join(str(x) for x in size1)
        size.append(size1)

    return size,fileName


 # 获取文件大小
 # os.path.getsize('/parastor/backup/data/SZT/t30data/*')
print(getdirsize('E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421'))