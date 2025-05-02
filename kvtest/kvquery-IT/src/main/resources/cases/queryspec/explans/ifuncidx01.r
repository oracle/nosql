compiled-query-plan
{
"query file" : "queryspec/q/ifuncidx01.q",
"plan" : 
{
  "iterator kind" : "INSERT_ROW",
  "row to insert (potentially partial)" : 
{
  "id" : 10,
  "firstName" : "Tom",
  "lastName" : "Waits",
  "otherNames" : null,
  "age" : 66,
  "income" : 1000,
  "address" : null,
  "expenses" : null,
  "connections" : null
},
  "value iterators" : [

  ],
  "TTL iterator" :
  {
    "iterator kind" : "CONST",
    "value" : 6
  }
}
}
