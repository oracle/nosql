# Test Description: Float column operand compared with long literal using != operator.

select id1, lng, flt from Foo where flt != 9223372036854775807
order by id1