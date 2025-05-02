# Test Description: Integer column operand compared with float numeric literal using > operator.

select id1, int from Foo where int > 1.4E-45
order by id1