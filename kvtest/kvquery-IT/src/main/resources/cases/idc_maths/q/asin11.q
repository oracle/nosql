#asin of nested complex types map in arrays and arrays in map and in json
select t.nestedNumMapInArray, asin(t.nestedNumMapInArray) as asinnestedNumMapInArray,
       t.nestedDouMapInArray, asin(t.nestedDouMapInArray) as asinnestedDouMapInArray,
       t.nestedNumArrayInMap, asin(t.nestedNumArrayInMap) as asinnestedNumArrayInMap,
       t.nestedDouArrayInMap, asin(t.nestedDouArrayInMap) as asinnestedDouArrayInMap,
       t.doc.nestedNumMapInArray as docnestedNumMapInArray, asin(t.doc.nestedNumMapInArray) as asindocnestedNumMapInArray,
       t.doc.nestedDouMapInArray as docnestedDouMapInArray, asin(t.doc.nestedDouMapInArray) as asindocnestedDouMapInArray,
       t.doc.nestedNumArrayInMap as docnestedNumArrayInMap, asin(t.doc.nestedNumArrayInMap) as asindocnestedNumArrayInMap,
       t.doc.nestedDouArrayInMap as docnestedDouArrayInMap, asin(t.doc.nestedDouArrayInMap) as asindocnestedDouArrayInMap
 from functional_test t where id=1