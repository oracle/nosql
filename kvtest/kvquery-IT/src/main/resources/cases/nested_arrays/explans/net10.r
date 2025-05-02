compiled-query-plan

{
"query file" : "nested_arrays/q/net10.q",
"plan" : 
{
  "iterator kind" : "SORT",
  "order by fields at positions" : [ 2 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "RECEIVE",
      "distribution kind" : "ALL_SHARDS",
      "order by fields at positions" : [ 0, 1 ],
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "netflix",
          "row variable" : "$$n",
          "index used" : "idx_showid_seasonNum",
          "covering index" : false,
          "index row variable" : "$$n_idx",
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : {}
            }
          ],
          "position in join" : 0
        },
        "FROM variable" : "$$n",
        "GROUP BY" : "Grouping by the first 2 expressions in the SELECT list",
        "SELECT expressions" : [
          {
            "field name" : "showId",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "value.contentStreamed[].showId",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$n_idx"
              }
            }
          },
          {
            "field name" : "seasonNum",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "value.contentStreamed[].seriesInfo[].seasonNum",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$n_idx"
              }
            }
          },
          {
            "field name" : "length",
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
                    "iterator kind" : "ARRAY_FILTER",
                    "predicate iterator" :
                    {
                      "iterator kind" : "EQUAL",
                      "left operand" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "seasonNum",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$element"
                        }
                      },
                      "right operand" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "value.contentStreamed[].seriesInfo[].seasonNum",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$$n_idx"
                        }
                      }
                    },
                    "input iterator" :
                    {
                      "iterator kind" : "FIELD_STEP",
                      "field name" : "seriesInfo",
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
                            "field name" : "value.contentStreamed[].showId",
                            "input iterator" :
                            {
                              "iterator kind" : "VAR_REF",
                              "variable" : "$$n_idx"
                            }
                          }
                        },
                        "input iterator" :
                        {
                          "iterator kind" : "FIELD_STEP",
                          "field name" : "contentStreamed",
                          "input iterator" :
                          {
                            "iterator kind" : "FIELD_STEP",
                            "field name" : "value",
                            "input iterator" :
                            {
                              "iterator kind" : "VAR_REF",
                              "variable" : "$$n"
                            }
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
    "GROUP BY" : "Grouping by the first 2 expressions in the SELECT list",
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
        "field name" : "seasonNum",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "seasonNum",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$from-1"
          }
        }
      },
      {
        "field name" : "length",
        "field expression" : 
        {
          "iterator kind" : "FUNC_SUM",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "length",
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