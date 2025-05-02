#sin of nested complex types map in arrays and arrays in map and in json
select t.nestedNumMapInArray, sin(t.nestedNumMapInArray) as sinnestedNumMapInArray,
       t.nestedDouMapInArray, sin(t.nestedDouMapInArray) as sinnestedDouMapInArray,
       t.nestedNumArrayInMap, sin(t.nestedNumArrayInMap) as sinnestedNumArrayInMap,
       t.nestedDouArrayInMap, sin(t.nestedDouArrayInMap) as sinnestedDouArrayInMap,
       t.doc.nestedNumMapInArray as docnestedNumMapInArray, sin(t.doc.nestedNumMapInArray) as sindocnestedNumMapInArray,
       t.doc.nestedDouMapInArray as docnestedDouMapInArray, sin(t.doc.nestedDouMapInArray) as sindocnestedDouMapInArray,
       t.doc.nestedNumArrayInMap as docnestedNumArrayInMap, sin(t.doc.nestedNumArrayInMap) as sindocnestedNumArrayInMap,
       t.doc.nestedDouArrayInMap as docnestedDouArrayInMap, sin(t.doc.nestedDouArrayInMap) as sindocnestedDouArrayInMap
 from functional_test t where id=1