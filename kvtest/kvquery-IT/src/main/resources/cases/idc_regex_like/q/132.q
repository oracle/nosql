#pattern, flag is sql null.
#return SQL NULL
select regex_like("test",p.name,p.name) from playerinfo p where id=100 and id1=2