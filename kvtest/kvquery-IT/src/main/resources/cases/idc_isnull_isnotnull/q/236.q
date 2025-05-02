#Expression has map constructor and is not null in projection
select id,{"Friends":[s.children.keys($value="Relative")]} is not null from sn s 