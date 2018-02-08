from numpy import *
import numpy as np
import pylab
import matplotlib.pyplot as plt
import scipy.stats as st

# a = arange(15)
# b = array([[1,2],[3,4]],dtype=complex)
# x = linspace( 0, 2, 5 )
# f = sin(x)
# c = ones((3,3),dtype=int)
# d = random.random((2,3))
# a[:9:3] = -1000
# print(d)

# c = array( [ [[  0,  1,  2],[ 10, 12, 13]],[[100,101,102],[110,112,113]] ] )
# cc = c.ravel()   # flatten the array
# cc.shape = (6,2)
# cc.resize((2,6)) #两行功能相同
# cc1 = cc.transpose()  #行列颠倒
#
# a = ([1,2],[3,4])
# b = ([1,2],[3,4])
# t1 = vstack((a,b))
# t2 = hstack((a, b))
# print(t2)

# a = arange(12).reshape(4,-1)
# b = a.copy()
# b[0,0] = 9999
# a = arange(12)**2
#
#
# mu, sigma = 2, 0.5
# s = np.random.normal(mu,sigma,10000)
# pylab.hist(s, bins=50, normed=1)       # matplotlib version (plot)
# pylab.show()
# aa = abs(mu < np.mean(s)) < .01
# bb = abs(sigma-np.std(s, ddof=1)) < .01
# s_fit = np.linspace(s.min(), s.max())
# plt.plot(s_fit, st.norm(mu, sigma).pdf(s_fit), lw=2, c='r')
# plt.show()


# a = range(10000000)
# b = range(10000000)
# c = []
# for i in range(len(a)):
#     c.append(a[i] + b[i])
# print(c)

#速度明显快很多，这就是 numpy 的优势
a = np.arange(10000000)
b = np.arange(10000000)
c = a + b
print(c)