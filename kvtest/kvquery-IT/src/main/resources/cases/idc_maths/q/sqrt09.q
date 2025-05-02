#sqrt of maps and nested map and in json values
select t.numMap, sqrt(t.numMap) as sqrtnumMap,
       t.numNestedMap, sqrt(t.numNestedMap) as sqrtnumNestedMap,
       t.douMap, sqrt(t.douMap) as sqrtdouMap,
       t.douNestedMap, sqrt(t.douNestedMap) as sqrtdouNestedMap,
       t.doc.numMap as docnumMap, sqrt(t.doc.numMap) as sqrtdocnumMap,
       t.doc.numNestedMap as docnumNestedMap, sqrt(t.doc.numNestedMap) as sqrtdocnumNestedMap,
       t.doc.douMap as docdouMap, sqrt(t.doc.douMap) as sqrtdocdouMap,
       t.doc.douNestedMap as docdouNestedMap, sqrt(t.doc.douNestedMap) as sqrtdocdouNestedMap
 from functional_test t where id=1