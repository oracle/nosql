# Test Description: Long column operand compared with float numeric literal using >= operator.

select id1, lng from Foo where lng >= 1.4E-45
order by id1