#Expression is array filter step with $pos implicit variable with is null in predicate
select id from sn s where s.children.Fred.Relative.Uncle[$pos >0] is  null