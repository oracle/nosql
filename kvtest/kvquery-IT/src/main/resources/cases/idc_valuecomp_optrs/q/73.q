# Test Description: Long column operand compared with double literal using < operator.

select id1 from Foo where lng < 4.9E-324
order by id1