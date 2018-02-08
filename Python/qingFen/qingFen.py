
# 输入：1268036000-1268036000|1268036000[0.00000000][0.00000000]-1268036000[0.00000000][0.00000000](268,0)|1.00000000    O-D|路径(线路id，0)|概率
# 输出：268036000，268036000，268，1.00000000   O,D,线路id,概率
# 功能：按行处理每条记录，按照特定标识符处理文本


filepath = "E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\工作日-平峰-20161102-2016-11-02\\ValidStationPath.txt"
f = open(filepath,"r",encoding='utf-8')
lines = f.readlines()
result1 = []
result2 = []
result3 = []
result4 = []
for i in range(len(lines)):
    record_each = lines[i].split('|')  # 取第i条记录并根据'|' 分开

    # 提取每条记录的 OD
    list_OD = record_each[0]
    OD = list_OD.split('-')
    str_OD = ''.join(OD)
    str_O = str_OD[1:7]
    str_D = str_OD[11:17]

    # 提取出每条线路的线路
    list_path = record_each[1].split('(')
    line_len = len(list_path)
    if line_len ==2:
        line = list_path[1].split(',')
        line1 = line[0]

        # 提取出每条记录的概率
        list_prob = record_each[2]
        if float(list_prob) != 0:  # 将概率为0 的剔除
            all = "%s,%s,%s,%s" % (str_O, str_D, line1, list_prob)  # 将字符串按照给定格式连接
            result1.append(all)
    elif line_len ==3:
        str = list_path[1].split(',')
        line = str[0]
        str1 = list_path[2].split(',')
        line1 = str1[0]

        # 提取出每条记录的概率
        list_prob = record_each[2]
        if float(list_prob) != 0:  # 将概率为0 的剔除
            all = "%s,%s,%s,%s,%s" % (str_O, str_D,line, line1, list_prob)  # 将字符串按照给定格式连接
            result2.append(all)
    elif line_len ==4:
        str = list_path[1].split(',')
        line = str[0]
        str1 = list_path[2].split(',')
        line1 = str1[0]
        str2 = list_path[3].split(',')
        line2 = str2[0]

        # 提取出每条记录的概率
        list_prob = record_each[2]
        if float(list_prob) != 0:  # 将概率为0 的剔除
            all = "%s,%s,%s,%s,%s,%s" % (str_O, str_D, line, line1, line2,list_prob)  # 将字符串按照给定格式连接
            result3.append(all)

    elif line_len == 5:
        str = list_path[1].split(',')
        line = str[0]
        str1 = list_path[2].split(',')
        line1 = str1[0]
        str2 = list_path[3].split(',')
        line2 = str2[0]
        str3 = list_path[4].split(',')
        line3 = str3[0]

        # 提取出每条记录的概率
        list_prob = record_each[2]
        if float(list_prob) != 0:  # 将概率为0 的剔除
            all = "%s,%s,%s,%s,%s,%s,%s" % (str_O, str_D, line, line1, line2,line3, list_prob)  # 将字符串按照给定格式连接
            result4.append(all)

str_result = ''.join(result1)
path = "C:\\Users\\will\\Desktop\\workday_ping_1.txt"
f = open(path, 'w')
f.write(str_result)
f.close()

str_result1 = ''.join(result2)
path1 = "C:\\Users\\will\\Desktop\\workday_ping_2.txt"
f1 = open(path1, 'w')
f1.write(str_result1)
f1.close()

str_result2 = ''.join(result3)
path2 = "C:\\Users\\will\\Desktop\\workday_ping_3.txt"
f2 = open(path2, 'w')
f2.write(str_result2)
f2.close()

str_result3 = ''.join(result4)
path3 = "C:\\Users\\will\\Desktop\\workday_ping_4.txt"
f3 = open(path3, 'w')
f3.write(str_result3)
f3.close()