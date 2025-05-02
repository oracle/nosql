#cot of maps and nested map and in json values
select t.numMap, cot(t.numMap) as cotnumMap,
       t.numNestedMap, cot(t.numNestedMap) as cotnumNestedMap,
       t.douMap, cot(t.douMap) as cotdouMap,
       t.douNestedMap, cot(t.douNestedMap) as cotdouNestedMap,
       t.doc.numMap as docnumMap, cot(t.doc.numMap) as cotdocnumMap,
       t.doc.numNestedMap as docnumNestedMap, cot(t.doc.numNestedMap) as cotdocnumNestedMap,
       t.doc.douMap as docdouMap, cot(t.doc.douMap) as cotdocdouMap,
       t.doc.douNestedMap as docdouNestedMap, cot(t.doc.douNestedMap) as cotdocdouNestedMap
 from functional_test t where id=1