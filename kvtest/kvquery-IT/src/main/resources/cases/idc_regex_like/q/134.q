#pattern or flags are not valid values or datatypes 

select regex_like(p.name,p.profile,p.info.id) from playerinfo p