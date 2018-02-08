import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt

from scipy import linalg, optimize
from numpy import poly1d

# 数组连接
#     将一维数组连接
a = np.r_[3,[0]*5,-1:1:6j]
#     将至少二维的数组连接
b = np.c_[np.array([1,2,3]), np.array([4,5,6])]
b1 = np.c_[np.array([[1,2,3]]), 0, 0, np.array([[4,5,6]])]
    # mgrid 也是将数组连接
c = np.mgrid[0:5,0:5]

#多项式
p = poly1d([3,4,5])

#矢量化函数
def addsubtract(a,b):
    if a > b:
      return a
    else:
        return

vec_addsubtract = np.vectorize(addsubtract)  # 将对数值的处理转化成对数组的处理
result = vec_addsubtract([0,3,6,9],[1,3,5,7])
print(result)