

num1 = range(1, 30, 2)
sum1 = 0
for i in num1:
    add1 = 1/(i*(30-i))
    sum1 = add1+sum1

num2 = range(1,28,2)
sum2 = 0
for j in num2:
    add2 = 1/(j*(28-j))
    sum2 = add2+sum2

result = sum1-14/15*sum2
print(result)