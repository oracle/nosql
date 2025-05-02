compiled-query-plan

{
"query file" : "insert/q/ups05r.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "UPSERT_ROW",
    "row to insert (potentially partial)" : 
{
  "str" : "gtm",
  "id1" : 3,
  "id2" : 100,
  "info" : {
    "a" : 10,
    "b" : 30
  },
  "rec1" : null
},
    "value iterators" : [

    ],
    "TTL iterator" :
    {
      "iterator kind" : "SEQ_CONCAT",
      "input iterators" : [

      ]
    }
  },
  "FROM variable" : "$f",
  "SELECT expressions" : [
    {
      "field name" : "row",
      "field expression" : 
      {
        "iterator kind" : "VAR_REF",
        "variable" : "$f"
      }
    },
    {
      "field name" : "ttl",
      "field expression" : 
      {
        "iterator kind" : "FUNC_REMAINING_DAYS",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$f"
        }
      }
    }
  ]
}
}