#ln of nested complex types map in arrays and arrays in map and in json
select t.nestedNumMapInArray, ln(t.nestedNumMapInArray) as lnnestedNumMapInArray,
       t.nestedDouMapInArray, ln(t.nestedDouMapInArray) as lnnestedDouMapInArray,
       t.nestedNumArrayInMap, ln(t.nestedNumArrayInMap) as lnnestedNumArrayInMap,
       t.nestedDouArrayInMap, ln(t.nestedDouArrayInMap) as lnnestedDouArrayInMap,
       t.doc.nestedNumMapInArray as docnestedNumMapInArray, ln(t.doc.nestedNumMapInArray) as lndocnestedNumMapInArray,
       t.doc.nestedDouMapInArray as docnestedDouMapInArray, ln(t.doc.nestedDouMapInArray) as lndocnestedDouMapInArray,
       t.doc.nestedNumArrayInMap as docnestedNumArrayInMap, ln(t.doc.nestedNumArrayInMap) as lndocnestedNumArrayInMap,
       t.doc.nestedDouArrayInMap as docnestedDouArrayInMap, ln(t.doc.nestedDouArrayInMap) as lndocnestedDouArrayInMap
 from functional_test t where id=1