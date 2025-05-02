# Test Description: Float column operand compared with double literal using <= operator.

select id1, flt from Foo where flt < 1.7976931348623157E308
order by id1