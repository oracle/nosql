compiled-query-plan

{
"query file" : "insert/q/ups04r.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "UPSERT_ROW",
    "row to insert (potentially partial)" : 
{
  "str" : "gtm",
  "id1" : 1,
  "id2" : -204,
  "info" : {
    "a" : 10,
    "b" : 30
  },
  "rec1" : {
    "long" : 120,
    "rec2" : {
      "str" : "dfg",
      "num" : 2.3E+3,
      "arr" : [10, 4, 6]
    },
    "recinfo" : {},
    "map" : {
      "bar" : "dff",
      "foo" : "xyz"
    }
  }
},
    "value iterators" : [

    ],
    "TTL iterator" :
    {
      "iterator kind" : "CONST",
      "value" : 7
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