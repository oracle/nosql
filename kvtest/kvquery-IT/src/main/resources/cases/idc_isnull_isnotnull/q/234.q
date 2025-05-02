#Expression has map constructor and is not null in predicate
select id from sn s where {"Friends":[s.children.keys($value="Relative")]} is not null