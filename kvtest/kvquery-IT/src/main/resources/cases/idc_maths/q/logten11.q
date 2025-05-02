#log10 with n that does not resolve to a number
select log10(t.doc.str), log10(t.doc.bool),log10(t.doc.obj) from functional_test t where t.id=7