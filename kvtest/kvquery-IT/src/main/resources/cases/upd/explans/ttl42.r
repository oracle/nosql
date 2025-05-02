compiled-query-plan

{
"query file" : "upd/q/ttl42.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "UPDATE_ROW",
      "indexes to update" : [  ],
      "update clauses" : [

      ],
      "update TTL" : true,
      "TimeUnit" : "DAYS",
      "TTL iterator" :
      {
        "iterator kind" : "CONST",
        "value" : -1
      },
      "isCompletePrimaryKey" : true,
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "NoTTL",
          "row variable" : "$j",
          "index used" : "primary index",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {"id":40},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$j",
        "SELECT expressions" : [
          {
            "field name" : "j",
            "field expression" : 
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$j"
            }
          }
        ]
      }
    },
    "FROM variable" : "$j",
    "SELECT expressions" : [
      {
        "field name" : "Expires",
        "field expression" : 
        {
          "iterator kind" : "FUNC_REMAINING_DAYS",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$j"
          }
        }
      }
    ]
  }
}
}
