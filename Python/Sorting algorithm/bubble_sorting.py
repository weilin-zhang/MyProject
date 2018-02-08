
# Refer to:https://www.cnblogs.com/eniac12/p/5329396.html
# 先解释一下排序算法的稳定性：表示如果两个相等的数在排序之后的先后位置与排序前的位置相同，即表明排序算法是稳定的

numbers = [6,5,3,1,8,7,2,4]
n = len(numbers)

# 交换同一数组中两个元素
def swap(arr,i,j):
    z = arr[i]
    arr[i] = arr[j]
    arr[j] = z

# 冒泡排序：依次比较相邻两个元素，如果顺序错误则调换过来，直到没有元素需要再调换
for i in range(n):
    for j in range(n-i-1):
        if numbers[j] > numbers[j + 1]:
          swap(numbers,j,j+1)
print(numbers)

# 冒泡排序改进：鸡尾酒排序
# 即先从低到高选出最大的，再从高到低选出最小的，重复此操作
# 好处在于访问序列的次数减少
left = 0
right = n-1
while(left < right):
    for i in range(left,right):
        if numbers[i] > numbers[i + 1]:
            swap(numbers, i, i + 1)
    right -= 1
    for j in range(right,left,-1):
        if numbers[j-1] > numbers[j]:
            swap(numbers, j-1, j)
    left += 1

print(numbers)





