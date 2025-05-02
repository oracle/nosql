#Expression has logical and arithmetic operator with is null in predicate
select id, s.children.values().Relative,s.map.IDP from sn s where age >20 is null or s.map.IDP[].DL<="P"