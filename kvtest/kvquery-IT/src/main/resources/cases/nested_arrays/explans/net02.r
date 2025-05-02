compiled-query-plan

{
"query file" : "nested_arrays/q/net02.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "netflix",
        "row variable" : "$$f",
        "index used" : "idx_showid",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {"value.contentStreamed[].showId":15},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$f",
      "WHERE" : 
      {
        "iterator kind" : "AND",
        "input iterators" : [
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FUNC_SIZE",
              "input iterator" :
              {
                "iterator kind" : "PROMOTE",
                "target type" : "Any",
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
                        "iterator kind" : "CONST",
                        "value" : 15
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
                          "variable" : "$$f"
                        }
                      }
                    }
                  }
                }
              }
            },
            "right operand" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "numSeasons",
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
                    "iterator kind" : "CONST",
                    "value" : 15
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
                      "variable" : "$$f"
                    }
                  }
                }
              }
            }
          },
          {
            "iterator kind" : "OP_NOT",
            "input iterator" :
            {
              "iterator kind" : "ANY_EQUAL",
              "left operand" :
              {
                "iterator kind" : "SEQ_MAP",
                "mapper iterator" :
                {
                  "iterator kind" : "EQUAL",
                  "left operand" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "numEpisodes",
                    "input iterator" :
                    {
                      "iterator kind" : "VAR_REF",
                      "variable" : "$sq1"
                    }
                  },
                  "right operand" :
                  {
                    "iterator kind" : "FUNC_SIZE",
                    "input iterator" :
                    {
                      "iterator kind" : "PROMOTE",
                      "target type" : "Any",
                      "input iterator" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "episodes",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$sq1"
                        }
                      }
                    }
                  }
                },
                "input iterator" :
                {
                  "iterator kind" : "ARRAY_FILTER",
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
                          "iterator kind" : "CONST",
                          "value" : 15
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
                            "variable" : "$$f"
                          }
                        }
                      }
                    }
                  }
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : false
              }
            }
          },
          {
            "iterator kind" : "EQUAL",
            "left operand" :
            {
              "iterator kind" : "FN_SEQ_SUM",
              "input iterator" :
              {
                "iterator kind" : "SEQ_MAP",
                "mapper iterator" :
                {
                  "iterator kind" : "ADD_SUBTRACT",
                  "operations and operands" : [
                    {
                      "operation" : "+",
                      "operand" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "lengthMin",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$sq1"
                        }
                      }
                    },
                    {
                      "operation" : "-",
                      "operand" :
                      {
                        "iterator kind" : "FIELD_STEP",
                        "field name" : "minWatched",
                        "input iterator" :
                        {
                          "iterator kind" : "VAR_REF",
                          "variable" : "$sq1"
                        }
                      }
                    }
                  ]
                },
                "input iterator" :
                {
                  "iterator kind" : "ARRAY_FILTER",
                  "input iterator" :
                  {
                    "iterator kind" : "FIELD_STEP",
                    "field name" : "episodes",
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
                            "iterator kind" : "CONST",
                            "value" : 15
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
                              "variable" : "$$f"
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 0
            }
          }
        ]
      },
      "GROUP BY" : "No grouping expressions",
      "SELECT expressions" : [
        {
          "field name" : "Column_1",
          "field expression" : 
          {
            "iterator kind" : "FUNC_COUNT_STAR"
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-1",
  "GROUP BY" : "No grouping expressions",
  "SELECT expressions" : [
    {
      "field name" : "Column_1",
      "field expression" : 
      {
        "iterator kind" : "FUNC_SUM",
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "Column_1",
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