# Test Description: Float column operand compared with double literal using >= operator.

select id1, flt from Foo where flt >= 4.9E-324
order by id1