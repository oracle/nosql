#asin with n that does not resolve to a number
select asin(t.doc.str), asin(t.doc.bool),asin(t.doc.obj) from functional_test t where t.id=7