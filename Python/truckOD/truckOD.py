# -*- coding: utf-8 -*-
import geopandas as gp
from matplotlib import pyplot as plt
shp='E:\\trafficDataAnalysis\\Guotu\\行政区划2017\\街道2017.shp'
china_geod =gp.GeoDataFrame.from_file(shp)
china_geod.head()
china_geod.plot()
plt.show()


# path1 = "E:\\trafficDataAnalysis\\truckOD\\20170511\\part-r-00000"
# f = open(path1)
# s = f.read()
# print(s)
