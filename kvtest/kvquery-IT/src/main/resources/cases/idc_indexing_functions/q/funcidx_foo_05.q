declare $arr1 array(string); // [  "  1abcdefghijk2lmnopqrstuvwxyz3 ", " NXXXME_12345  ", "  name_123 ", " XXXBCDEFGHIJKLMNOPQRSTUVWXYZ  ", "XXXXXXXXXBCDEFGHIJKLMNOPQRSTUVWXYZzz", "XXXXXXname_123z" ]

select /*+ FORCE_INDEX(foo idx_replace_name) */
      id,
      name,
      replace(name, 'A', 'XXX') as replaced_name
from foo f
where replace(f.name, 'A', 'XXX') in $arr1[]
