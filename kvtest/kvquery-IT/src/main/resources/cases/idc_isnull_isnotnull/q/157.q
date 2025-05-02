#Expression is array slice step with L only in predicate and is null in predicate
select id,s.children.Fred.Relative.Uncle from sn s where s.children.Fred.Relative.Uncle[1:] is null