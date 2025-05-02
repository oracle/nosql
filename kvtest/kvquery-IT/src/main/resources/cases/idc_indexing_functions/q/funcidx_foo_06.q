declare $arr2 array(string); // [ " 3zyxwvutsrqponml2kjihgfedcba1  ", "  54321_EMAN ", " 321_eman  ", "  ZYXWVUTSRQPONMLKJIHGFEDCBA ", "zzZYXWVUTSRQPONMLKJIHGFEDCBAAA", "z321_emanAA" ] 

select /*+ FORCE_INDEX(foo idx_reverse_name) */ id, name, reverse(name) as reversed_name
from foo f
where reverse(f.name) in $arr2[]
