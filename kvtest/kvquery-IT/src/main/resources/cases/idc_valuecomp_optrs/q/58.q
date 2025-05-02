# Test Description: Integer column operand compared with double numeric literal using <= operator.

select id1, int, dbl from Foo where int <= 1.79769313486231
order by id1