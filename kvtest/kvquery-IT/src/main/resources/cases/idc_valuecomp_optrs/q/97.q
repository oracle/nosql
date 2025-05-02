# Test Description: Double column operand compared with integer literal using < operator.

select id1, dbl from Foo where dbl < 2147483647
order by id1
