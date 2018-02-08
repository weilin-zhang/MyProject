
# 输入：地铁一号线，地铁五号线，地铁四号线，8581.981942
#      地铁二号线	，地铁一号线，地铁五号线，2655.690937    : 表示按照该条路径线路换乘的客流量
# 输出：按照要求输出两条线路换乘的客运总量   地铁一号线，地铁五号线，11237.67288
# 功能：计算各线路换乘客流量

# zao  wan  ping  3  4
path1 = "ping"
filepath = "E:\\trafficDataAnalysis\\清分比例_8条线修改BUG加深圳北调整（早高峰三分之一）20170421\\workday\\" + \
           path1+"\\result_3\\part-00000"
f = open(filepath,"r",encoding='utf-8')
lines = f.readlines()

sum1 = 0
for i in range(len(lines)):
    record_each = lines[i].split(',')
    for j in range(len(record_each)-1):
        if record_each[j] =="地铁十一号线" and record_each[j+1] =="地铁五号线":
            sum1 = sum1+float(record_each[len(record_each)-1])

print(sum1)




