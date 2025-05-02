#sqrt of nested complex types map in arrays and arrays in map and in json
select t.nestedNumMapInArray, sqrt(t.nestedNumMapInArray) as sqrtnestedNumMapInArray,
       t.nestedDouMapInArray, sqrt(t.nestedDouMapInArray) as sqrtnestedDouMapInArray,
       t.nestedNumArrayInMap, sqrt(t.nestedNumArrayInMap) as sqrtnestedNumArrayInMap,
       t.nestedDouArrayInMap, sqrt(t.nestedDouArrayInMap) as sqrtnestedDouArrayInMap,
       t.doc.nestedNumMapInArray as docnestedNumMapInArray, sqrt(t.doc.nestedNumMapInArray) as sqrtdocnestedNumMapInArray,
       t.doc.nestedDouMapInArray as docnestedDouMapInArray, sqrt(t.doc.nestedDouMapInArray) as sqrtdocnestedDouMapInArray,
       t.doc.nestedNumArrayInMap as docnestedNumArrayInMap, sqrt(t.doc.nestedNumArrayInMap) as sqrtdocnestedNumArrayInMap,
       t.doc.nestedDouArrayInMap as docnestedDouArrayInMap, sqrt(t.doc.nestedDouArrayInMap) as sqrtdocnestedDouArrayInMap
 from functional_test t where id=1