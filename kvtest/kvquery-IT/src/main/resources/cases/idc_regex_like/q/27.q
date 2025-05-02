#TestDescription: Input string is a literal of Atomic String type.

select id from playerinfo p where regex_like(p.name,"v.*","i") and id=100 and id1=1