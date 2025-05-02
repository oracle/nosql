# combination of types for code coverage

select id1, int from Foo where (lng + int) > 14 or (flt + int) > 14 or
 (lng - int) > 14 or (flt - lng) > 14 or (lng * int) > 14 or (flt * int) >
 14 or (lng / (1+int)) > 14 or (flt / (1+int)) > 14