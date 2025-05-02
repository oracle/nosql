#cos of maps and nested map and in json values
select t.numMap, cos(t.numMap) as cosnumMap,
       t.numNestedMap, cos(t.numNestedMap) as cosnumNestedMap,
       t.douMap, cos(t.douMap) as cosdouMap,
       t.douNestedMap, cos(t.douNestedMap) as cosdouNestedMap,
       t.doc.numMap as docnumMap, cos(t.doc.numMap) as cosdocnumMap,
       t.doc.numNestedMap as docnumNestedMap, cos(t.doc.numNestedMap) as cosdocnumNestedMap,
       t.doc.douMap as docdouMap, cos(t.doc.douMap) as cosdocdouMap,
       t.doc.douNestedMap as docdouNestedMap, cos(t.doc.douNestedMap) as cosdocdouNestedMap
 from functional_test t where id=1