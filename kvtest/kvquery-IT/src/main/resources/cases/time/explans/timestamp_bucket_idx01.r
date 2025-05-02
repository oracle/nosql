compiled-query-plan

{
"query file" : "time/q/timestamp_bucket_idx01.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "order by fields at positions" : [ 0 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "roundtest",
        "row variable" : "$$roundtest",
        "index used" : "idx_bucket_t3",
        "covering index" : true,
        "index row variable" : "$$roundtest_idx",
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$roundtest_idx",
      "GROUP BY" : "Grouping by the first expression in the SELECT list",
      "SELECT expressions" : [
        {
          "field name" : "t3_5mins",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "timestamp_bucket#t3@,'5 minutes'",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$roundtest_idx"
            }
          }
        },
        {
          "field name" : "count",
          "field expression" : 
          {
            "iterator kind" : "FUNC_COUNT_STAR"
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-1",
  "GROUP BY" : "Grouping by the first expression in the SELECT list",
  "SELECT expressions" : [
    {
      "field name" : "t3_5mins",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "t3_5mins",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-1"
        }
      }
    },
    {
      "field name" : "count",
      "field expression" : 
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "count",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      }
    }
  ]
}
}