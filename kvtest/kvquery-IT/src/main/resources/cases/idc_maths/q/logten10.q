#log10 of nested complex types map in arrays and arrays in map and in json
select t.nestedNumMapInArray, log10(t.nestedNumMapInArray) as log10nestedNumMapInArray,
       t.nestedDouMapInArray, log10(t.nestedDouMapInArray) as log10nestedDouMapInArray,
       t.nestedNumArrayInMap, log10(t.nestedNumArrayInMap) as log10nestedNumArrayInMap,
       t.nestedDouArrayInMap, log10(t.nestedDouArrayInMap) as log10nestedDouArrayInMap,
       t.doc.nestedNumMapInArray as docnestedNumMapInArray, log10(t.doc.nestedNumMapInArray) as log10docnestedNumMapInArray,
       t.doc.nestedDouMapInArray as docnestedDouMapInArray, log10(t.doc.nestedDouMapInArray) as log10docnestedDouMapInArray,
       t.doc.nestedNumArrayInMap as docnestedNumArrayInMap, log10(t.doc.nestedNumArrayInMap) as log10docnestedNumArrayInMap,
       t.doc.nestedDouArrayInMap as docnestedDouArrayInMap, log10(t.doc.nestedDouArrayInMap) as log10docnestedDouArrayInMap
 from functional_test t where id=1