#Expression has variable referenc with is not null in predicate
declare $v1 integer;
select id, age,$v1 from sn where id = $v1 is not null
