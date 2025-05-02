# Test Description: Long column operand compared with float literal using < operator.

select id1, lng from Foo where lng < 0.0
order by id1