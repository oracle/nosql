# array filter + array slice step expression + sequence comparison 
select a.ida1, d.idd1, d.d4[0:2] as d4, f.idf1, f.idf2, f.idf3 from nested tables (A a descendants (A.B.D d, A.F f)) where d.d4[] =any 'abc'
