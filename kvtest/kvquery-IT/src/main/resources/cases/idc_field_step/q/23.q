# Test Description: Test to verify a path expressions that does not start with a table column or table alias using var_ref.

declare $mapval string;
select children.$mapval
from Complex