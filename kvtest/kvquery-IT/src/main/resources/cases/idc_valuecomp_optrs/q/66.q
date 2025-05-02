# Test Description: Long column operand compared with integer numeric literal using >= operator.

select id1, int from Foo where lng >= 2147483647
order by id1