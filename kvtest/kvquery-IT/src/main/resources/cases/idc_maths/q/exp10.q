#exp of nested complex types map in arrays and arrays in map and in json
select t.nestedNumMapInArray, exp(t.nestedNumMapInArray) as expnestedNumMapInArray,
       t.nestedDouMapInArray, exp(t.nestedDouMapInArray) as expnestedDouMapInArray,
       t.nestedNumArrayInMap, exp(t.nestedNumArrayInMap) as expnestedNumArrayInMap,
       t.nestedDouArrayInMap, exp(t.nestedDouArrayInMap) as expnestedDouArrayInMap,
       t.doc.nestedNumMapInArray as docnestedNumMapInArray, exp(t.doc.nestedNumMapInArray) as expdocnestedNumMapInArray,
       t.doc.nestedDouMapInArray as docnestedDouMapInArray, exp(t.doc.nestedDouMapInArray) as expdocnestedDouMapInArray,
       t.doc.nestedNumArrayInMap as docnestedNumArrayInMap, exp(t.doc.nestedNumArrayInMap) as expdocnestedNumArrayInMap,
       t.doc.nestedDouArrayInMap as docnestedDouArrayInMap, exp(t.doc.nestedDouArrayInMap) as expdocnestedDouArrayInMap
 from functional_test t where id=1