# Test Description: Double column operand compared with integer literal using > operator.

select id1, dbl from Foo where dbl > -2147483648
order by id1