compiled-query-plan

{
"query file" : "delete/q/del04.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "DELETE_ROW",
    "input iterator" :
    {
      "iterator kind" : "SEQ_CONCAT",
      "input iterators" : [

      ]
    }
  },
  "FROM variable" : "$delcount-0",
  "GROUP BY" : "No grouping expressions",
  "SELECT expressions" : [
    {
      "field name" : "numRowsDeleted",
      "field expression" : 
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "numRowsDeleted",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$delcount-0"
          }
        }
      }
    }
  ]
}
}