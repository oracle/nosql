compiled-query-plan

{
"query file" : "insert/q/ins14.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "INSERT_ROW",
    "row to insert (potentially partial)" : 
{
  "id" : 5,
  "id1" : "EMPTY",
  "name" : "john"
},
    "value iterators" : [

    ]
  },
  "FROM variable" : "$$tIdentity",
  "SELECT expressions" : [
    {
      "field name" : "id",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "id",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$tIdentity"
        }
      }
    },
    {
      "field name" : "name",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "name",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$tIdentity"
        }
      }
    }
  ]
}
}