#Expression has map constructor and is null in projection
select id,{"Friends":[s.children.keys($value="Relative")]} is null from sn s 