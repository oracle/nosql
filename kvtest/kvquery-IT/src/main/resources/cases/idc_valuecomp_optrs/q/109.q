# Test Description: Double column operand compared with float numeric literal using < operator.

select id1, dbl from Foo where dbl < 3.4028235E38
order by id1