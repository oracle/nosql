# Test Description: Integer column operand compared with double numeric literal using > operator.

select id1, int, dbl from Foo where int > 4.9E-324
order by id1