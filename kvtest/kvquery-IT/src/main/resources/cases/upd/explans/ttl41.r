compiled-query-plan

{
"query file" : "upd/q/ttl41.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "UPDATE_ROW",
    "indexes to update" : [  ],
    "update clauses" : [

    ],
    "update TTL" : true,
    "TimeUnit" : "HOURS",
    "TTL iterator" :
    {
      "iterator kind" : "CASE",
      "clauses" : [
        {
          "when iterator" :
          {
            "iterator kind" : "LESS_THAN",
            "left operand" :
            {
              "iterator kind" : "FUNC_REMAINING_HOURS",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 0
            }
          },
          "then iterator" :
          {
            "iterator kind" : "CONST",
            "value" : 2
          }
        },
        {
          "else iterator" :
          {
            "iterator kind" : "CONST",
            "value" : 10
          }
        }
      ]
    },
    "isCompletePrimaryKey" : true,
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "NoTTL",
        "row variable" : "$f",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"id":41},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$f",
      "SELECT expressions" : [
        {
          "field name" : "f",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$f"
          }
        }
      ]
    }
  }
}
}
