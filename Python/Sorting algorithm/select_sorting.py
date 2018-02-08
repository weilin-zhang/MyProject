
# Refer to:https://www.cnblogs.com/eniac12/p/5329396.html


numbers = [6,5,3,1,8,7,2,4]
n = len(numbers)

def swap(arr,i,j):
    z = arr[i]
    arr[i] = arr[j]
    arr[j] = z

# 选择排序，每遍历一遍就记住最小（值）的位置，然后再放到排头
for i in range(n):
    min = i        # 只需记住下标i，i为已排序的末尾
    for j in range(i+1,n):
        if numbers[j] < numbers[min]:
            min = j               # 找到未排序中的最小值
    if min!= i:
        swap(numbers,min,i)

print(numbers)