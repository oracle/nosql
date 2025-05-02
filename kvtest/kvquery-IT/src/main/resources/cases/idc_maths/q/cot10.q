#cot of nested complex types map in arrays and arrays in map and in json
select t.nestedNumMapInArray, cot(t.nestedNumMapInArray) as cotnestedNumMapInArray,
       t.nestedDouMapInArray, cot(t.nestedDouMapInArray) as cotnestedDouMapInArray,
       t.nestedNumArrayInMap, cot(t.nestedNumArrayInMap) as cotnestedNumArrayInMap,
       t.nestedDouArrayInMap, cot(t.nestedDouArrayInMap) as cotnestedDouArrayInMap,
       t.doc.nestedNumMapInArray as docnestedNumMapInArray, cot(t.doc.nestedNumMapInArray) as cotdocnestedNumMapInArray,
       t.doc.nestedDouMapInArray as docnestedDouMapInArray, cot(t.doc.nestedDouMapInArray) as cotdocnestedDouMapInArray,
       t.doc.nestedNumArrayInMap as docnestedNumArrayInMap, cot(t.doc.nestedNumArrayInMap) as cotdocnestedNumArrayInMap,
       t.doc.nestedDouArrayInMap as docnestedDouArrayInMap, cot(t.doc.nestedDouArrayInMap) as cotdocnestedDouArrayInMap
 from functional_test t where id=1