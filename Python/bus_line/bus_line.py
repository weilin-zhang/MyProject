#!/usr/bin/python
#coding = UFT-8

# 输入： 1:1:22.531714,114.118601;22.53182,114.119487;22.531304,114.11954;22.531375,114.119598;  （线路：方向：经度，维度；）
# 输出： 1_1,8.014442632415367
# 功能： 计算相邻经纬度的距离之和

from math import*


def geodistance(lat1,lng1,lat2,lng2):
    lng1, lat1, lng2, lat2 = map(radians, [lng1, lat1, lng2, lat2])  #radians:返回一个角度的弧度值，弧度才比较好做后续的运算
    dlon=lng2-lng1
    dlat=lat2-lat1
    a=sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    dis=2*asin(sqrt(a))*6371*1000
    return dis


def Distance1(Lat_A,Lng_A,Lat_B,Lng_B): #第一种计算方法
    ra=6378.140 #赤道半径
    rb=6356.755 #极半径 （km）
    flatten=(ra-rb)/ra  #地球偏率
    rad_lat_A=radians(Lat_A)
    rad_lng_A=radians(Lng_A)
    rad_lat_B=radians(Lat_B)
    rad_lng_B=radians(Lng_B)
    pA=atan(rb/ra*tan(rad_lat_A))
    pB=atan(rb/ra*tan(rad_lat_B))
    xx=acos(sin(pA)*sin(pB)+cos(pA)*cos(pB)*cos(rad_lng_A-rad_lng_B))
    c1=(sin(xx)-xx)*(sin(pA)+sin(pB))**2/cos(xx/2)**2
    c2=(sin(xx)+xx)*(sin(pA)-sin(pB))**2/sin(xx/2)**2
    dr=flatten/8*(c1-c2)
    distance=ra*(xx+dr)
    return distance

def Distance2(lat1,lng1,lat2,lng2):# 第二种计算方法
    radlat1=radians(lat1)
    radlat2=radians(lat2)
    a=radlat1-radlat2
    b=radians(lng1)-radians(lng2)
    s=2*asin(sqrt(pow(sin(a/2),2)+cos(radlat1)*cos(radlat2)*pow(sin(b/2),2)))
    earth_radius=6378.137
    s=s*earth_radius
    if s<0:
        return -s
    else:
        return s


filepath = "E://BusOD//lineCheckPointInfo_20160125.txt"
f = open(filepath,"r",encoding='utf-8')
lines = f.readlines()
for i in range(len(lines)):
    record_each = lines[i].split(':')  # 取第i条记录
    str1 = record_each[2]  # 取出每条线路的经纬度集合
    lines2 = str1.split(';')
    sum1 = 0
    for i in range(len(lines2) - 2):
        line3 = lines2[i].split(',')  #第i个经纬度
        line4 = lines2[i + 1].split(',')  #第i+1个经纬度
        lat = line3[0]  #取出第i个纬度
        lon = line3[1]
        lat1 = line4[0]
        lon1 = line4[1]
        dis = Distance2(float(lat), float(lon), float(lat1), float(lon1))  #要将str 转换成 float # 单位是米
        # print(dis*1000)
        if dis < 1000:
            sum1 = dis + sum1
    print(record_each[0]+'_'+record_each[1]+','+str(sum1))









