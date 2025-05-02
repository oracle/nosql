#Expression has variable reference with is null in predicate
declare $v1 integer;
select id, age from sn where id = $v1 is null
