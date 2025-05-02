compiled-query-plan

{
"query file" : "idc_schemaless/q/xdel02.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "DELETE_ROW",
      "positions of primary key columns in input row" : [ 8, 9, 10 ],
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "jsoncol",
          "row variable" : "$f",
          "index used" : "idx_name",
          "covering index" : false,
          "index row variable" : "$f_idx",
          "index scans" : [
            {
              "equality conditions" : {},
              "range conditions" : {}
            }
          ],
          "index filtering predicate" :
          {
            "iterator kind" : "GREATER_THAN",
            "left operand" :
            {
              "iterator kind" : "FUNC_INDEX_STORAGE_SIZE",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f_idx"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 100
            }
          },
          "position in join" : 0
        },
        "FROM variable" : "$f",
        "SELECT expressions" : [
          {
            "field name" : "id",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "id",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            }
          },
          {
            "field name" : "row_size",
            "field expression" : 
            {
              "iterator kind" : "FUNC_ROW_STORAGE_SIZE",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            }
          },
          {
            "field name" : "isize_sca",
            "field expression" : 
            {
              "iterator kind" : "FUNC_INDEX_STORAGE_SIZE",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            }
          },
          {
            "field name" : "isize_cp",
            "field expression" : 
            {
              "iterator kind" : "FUNC_INDEX_STORAGE_SIZE",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            }
          },
          {
            "field name" : "part",
            "field expression" : 
            {
              "iterator kind" : "FUNC_PARTITION",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            }
          },
          {
            "field name" : "shard",
            "field expression" : 
            {
              "iterator kind" : "FUNC_SHARD",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            }
          },
          {
            "field name" : "expiration",
            "field expression" : 
            {
              "iterator kind" : "FUNC_REMAINING_DAYS",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            }
          },
          {
            "field name" : "mod_time",
            "field expression" : 
            {
              "iterator kind" : "AND",
              "input iterators" : [
                {
                  "iterator kind" : "GREATER_OR_EQUAL",
                  "left operand" :
                  {
                    "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
                    "input iterator" :
                    {
                      "iterator kind" : "FUNC_MOD_TIME",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$f"
                      }
                    }
                  },
                  "right operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 2020
                  }
                },
                {
                  "iterator kind" : "GREATER_OR_EQUAL",
                  "left operand" :
                  {
                    "iterator kind" : "FUNC_EXTRACT_FROM_TIMESTAMP",
                    "input iterator" :
                    {
                      "iterator kind" : "FUNC_MOD_TIME",
                      "input iterator" :
                      {
                        "iterator kind" : "VAR_REF",
                        "variable" : "$f"
                      }
                    }
                  },
                  "right operand" :
                  {
                    "iterator kind" : "CONST",
                    "value" : 9
                  }
                }
              ]
            }
          },
          {
            "field name" : "majorKey1_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "majorKey1",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            }
          },
          {
            "field name" : "majorKey2_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "majorKey2",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            }
          },
          {
            "field name" : "minorKey_gen",
            "field expression" : 
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "minorKey",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            }
          }
        ]
      }
    },
    "FROM variable" : "$f",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$f"
          }
        }
      },
      {
        "field name" : "row_size",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "row_size",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$f"
          }
        }
      },
      {
        "field name" : "isize_sca",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "isize_sca",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$f"
          }
        }
      },
      {
        "field name" : "isize_cp",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "isize_cp",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$f"
          }
        }
      },
      {
        "field name" : "part",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "part",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$f"
          }
        }
      },
      {
        "field name" : "shard",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "shard",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$f"
          }
        }
      },
      {
        "field name" : "expiration",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "expiration",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$f"
          }
        }
      },
      {
        "field name" : "mod_time",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "mod_time",
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
}