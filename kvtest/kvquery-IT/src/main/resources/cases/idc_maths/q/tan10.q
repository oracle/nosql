#tan of nested complex types map in arrays and arrays in map and in json
select t.nestedNumMapInArray, tan(t.nestedNumMapInArray) as tannestedNumMapInArray,
       t.nestedDouMapInArray, tan(t.nestedDouMapInArray) as tannestedDouMapInArray,
       t.nestedNumArrayInMap, tan(t.nestedNumArrayInMap) as tannestedNumArrayInMap,
       t.nestedDouArrayInMap, tan(t.nestedDouArrayInMap) as tannestedDouArrayInMap,
       t.doc.nestedNumMapInArray as docnestedNumMapInArray, tan(t.doc.nestedNumMapInArray) as tandocnestedNumMapInArray,
       t.doc.nestedDouMapInArray as docnestedDouMapInArray, tan(t.doc.nestedDouMapInArray) as tandocnestedDouMapInArray,
       t.doc.nestedNumArrayInMap as docnestedNumArrayInMap, tan(t.doc.nestedNumArrayInMap) as tandocnestedNumArrayInMap,
       t.doc.nestedDouArrayInMap as docnestedDouArrayInMap, tan(t.doc.nestedDouArrayInMap) as tandocnestedDouArrayInMap
 from functional_test t where id=1