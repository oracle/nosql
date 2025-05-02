#log10 of maps and nested map and in json values
select t.numMap, log10(t.numMap) as log10numMap,
       t.numNestedMap, log10(t.numNestedMap) as log10numNestedMap,
       t.douMap, log10(t.douMap) as log10douMap,
       t.douNestedMap, log10(t.douNestedMap) as log10douNestedMap,
       t.doc.numMap as docnumMap, log10(t.doc.numMap) as log10docnumMap,
       t.doc.numNestedMap as docnumNestedMap, log10(t.doc.numNestedMap) as log10docnumNestedMap,
       t.doc.douMap as docdouMap, log10(t.doc.douMap) as log10docdouMap,
       t.doc.douNestedMap as docdouNestedMap, log10(t.doc.douNestedMap) as log10docdouNestedMap
 from functional_test t where id=1