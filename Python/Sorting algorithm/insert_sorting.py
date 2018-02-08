
# Refer to:https://www.cnblogs.com/eniac12/p/5329396.html


numbers = [6,5,3,1,8,7,2,4]
n = len(numbers)

def swap(arr,i,j):
    z = arr[i]
    arr[i] = arr[j]
    arr[j] = z

# 插入排序
# 将未排序与已排序元素逐个比较，插入新元素后需要反复将已排序的元素逐步向后挪
# 第一个元素默认排好序，第二个之后依次与排好序的元素比较

# for i in range(n):
#     get = numbers[i]  # 每次从未排序的数中挑一张出来
#     j = i -1  # 已经排好序的牌
#     while(j >= 0 and numbers[j] > get): # 注意 &表示按位与，区别于and
#         numbers[j+1] =numbers[j]
#         j -= 1
#     numbers[j+1] = get
# print(numbers)

# 改进：二分插入排序
# 如果比较操作比插入操作的代价大的话，考虑用二分查找法来减少操作的次数
# for i in range(n):
#     get = numbers[i]
#     left = 0    # 定义二分法的头部
#     right = i-1 # 尾部
#     while(left <= right):  # while 这一步直接找到插入元素的left 和right
#         mid = int((left+right)/2)
#         if (numbers[mid] > get):
#             right = mid -1
#         else:left = mid+1
#     for j in range(i-1,left-1,-1):  # left-1 是因为range()函数取左不取右的原因
#         numbers[j+1]=numbers[j]  # 每次换一个元素
#     numbers[left] = get
# print(numbers)



# 更高效：希尔排序 shell sort
# 在和以排序的元素进行比较时，引入步长加快比较速度
# 看不懂....？？？
h = 0
while(h<=n):
    h = 3*h + 1
while(h >= 1):  # 步长至少大于等于1
    for i in range(h,n):
        j = i - h
        get =numbers[i]
        while (j >= 0 and numbers[j] > get):
            numbers[j+h] = numbers[j]
            j = j-h
        numbers[j+h] = get
    h = int((h-1)/3)  # int()向下取整
print(numbers)

