#Using Math func in JSON collection fields yielding positive results

select sign(t.address.latitude),
       trunc(cos(t.numbers.num2),7),
       trunc(acos(t.age),7),
       floor(t.numbers.num1),
       floor(t.numbers.num4),
       ceil(t.numbers.num3),
       trunc(tan(t.numbers.pi1),7),
       trunc(sqrt(t.address.pincode),7),
       trunc(degrees(t.numbers.pi3),7),
       trunc(abs(t.address.longitude),7)
from jsonCollection_test t where id =1