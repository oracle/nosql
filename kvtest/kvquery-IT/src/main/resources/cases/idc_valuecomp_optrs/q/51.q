# Test Description: Integer column operand compared with float numeric literal using < operator.

select id1,int,flt from Foo where int < 2.53
order by id1