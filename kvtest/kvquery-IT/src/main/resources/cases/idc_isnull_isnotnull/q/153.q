#Expression is array slice step (with L and H) with L>H in predicate with is null in predicate
select id,s.children.Fred.Relative.Uncle from sn s where s.children.Fred.Relative.Uncle[1:0] is null