compiled-query-plan

{
"query file" : "queryspec/q/unnest_opt03.q",
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
          "index used" : "idx5_country_showid",
          "covering index" : false,
          "index row variable" : "$$v_idx",
          "index scans" : [
            {
              "equality conditions" : {"info.country":"USA"},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$v",
        "GROUP BY" : "Grouping by the first expression in the SELECT list",
        "SELECT expressions" : [
          {
            "field name" : "showId",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "info.shows[].showId",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$v_idx"
              }
            }
          },
          {
            "field name" : "total_time",
            "field expression" : 
            {
              "iterator kind" : "FUNC_SUM",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "minWatched",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "episodes",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "seasons",
                    "input iterator" :
                    {
                      "iterator kind" : "ARRAY_FILTER",
                      "predicate iterator" :
                      {
                        "iterator kind" : "EQUAL",
                        "left operand" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "showId",
                          "input iterator" :
                          {
                            "iterator kind" : "VAR_REF",
                            "variable" : "$element"
                          }
                        },
                        "right operand" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "info.shows[].showId",
                          "input iterator" :
                          {
                            "iterator kind" : "VAR_REF",
                            "variable" : "$$v_idx"
                          }
                        }
                      },
                      "input iterator" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "shows",
                        "input iterator" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "info",
                          "input iterator" :
                          {
                            "iterator kind" : "VAR_REF",
                            "variable" : "$$v"
                          }
                        }
                      }
                    }
                  }
                }
              }
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
        "field name" : "total_time",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "total_time",
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