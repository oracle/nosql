# Test Description: Float column operand compared with integer literal using <= operator.

select id1, int, flt from Foo where flt <= 2147483647
order by id1