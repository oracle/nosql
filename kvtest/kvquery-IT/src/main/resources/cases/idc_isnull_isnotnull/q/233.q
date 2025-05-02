#Expression has map constructor and is null in predicate
select id from sn s where {"Friends":[s.children.keys($value="Relative")]} is null