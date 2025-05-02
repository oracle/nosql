# Test Description: Double column operand compared with long numeric literal using > operator.

select id1, dbl from Foo where dbl > -9223372036854775808
order by id1