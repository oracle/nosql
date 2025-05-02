# Test Description: Select value of non-existing key from a map using var_ref

declare $empval string;
select $C.children.$empval
from Complex $C