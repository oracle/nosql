#ceil of constants
select ceil(7.8)=8.0, ceil(-7.8)=-7, ceil(0)=0, ceil(0.0)=0.0, ceil(9999999999.99)=10000000000.0, ceil(null) IS NULL, ceil(1234567890.1234567890)=1234567891.0, ceil(-0.2) from math_test where id=1

