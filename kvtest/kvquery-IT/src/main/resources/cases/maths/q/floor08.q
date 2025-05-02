#floor of constants
select floor(7.8)=7.0, floor(-7.8)=-8, floor(0)=0, floor(0.0)=0.0, floor(9999999999.99)=9999999999.0, floor(null) IS NULL, floor(1234567890.1234567890)=1234567890.0, floor(-0.2) from math_test where id=1

