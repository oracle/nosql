compiled-query-plan

{
"query file" : "schemaless/q/q08.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 1 ],
  "input iterator" :
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
          "target table" : "Viewers",
          "row variable" : "$$v",
          "index used" : "idx_country_showid",
          "covering index" : true,
          "index row variable" : "$$v_idx",
          "index scans" : [
            {
              "equality conditions" : {"country":"USA"},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$v_idx",
        "GROUP BY" : "Grouping by the first expression in the SELECT list",
        "SELECT expressions" : [
          {
            "field name" : "showId",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "shows[].showId",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$v_idx"
              }
            }
          },
          {
            "field name" : "cnt",
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
        "field name" : "showId",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "showId",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      },
      {
        "field name" : "cnt",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "cnt",
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
}