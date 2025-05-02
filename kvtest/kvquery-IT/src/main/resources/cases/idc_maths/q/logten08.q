#log10 of array types and array in json
select t.numArr, log10(t.numArr) as log10numArr,
       t.num2DArr, log10(t.num2DArr) as log10num2DArr,
       t.num3DArr, log10(t.num3DArr) as log10num3DArr,
       t.douArr, log10(t.numArr) as log10douArr,
       t.dou2DArr, log10(t.num2DArr) as log10dou2DArr,
       t.dou3DArr, log10(t.num3DArr) as log10dou3DArr,
                     t.doc.numArr as docnumArr, log10(t.doc.numArr) as log10docnumArr,
                     t.doc.num2DArr as docnum2DArr, log10(t.doc.num2DArr) as log10docnum2DArr,
                     t.doc.num3DArr as docnum3DArr, log10(t.doc.num3DArr) as log10docnum3DArr,
                     t.doc.douArr as docdouArr, log10(t.doc.numArr) as log10docdouArr,
                     t.doc.dou2DArr as docdou2DArr, log10(t.doc.num2DArr) as log10docdou2DArr,
                     t.doc.dou3DArr as  docdou3DArr, log10(t.doc.num3DArr) as log10docdou3DArr
 from functional_test t where id=1