compiled-query-plan
{
"query file" : "rowprops/q/xxdel02.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "input iterator" :
  {
    "iterator kind" : "DELETE_ROW",
    "positions of primary key columns in input row" : [ 0 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Foo",
        "row variable" : "$f",
        "index used" : "idx_state_city_age",
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
            "value" : 40
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
            "iterator kind" : "AND",
            "input iterators" : [
              {
                "iterator kind" : "GREATER_OR_EQUAL",
                "left operand" :
                {
                  "iterator kind" : "FUNC_ROW_STORAGE_SIZE",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$f"
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 165
                }
              },
              {
                "iterator kind" : "LESS_OR_EQUAL",
                "left operand" :
                {
                  "iterator kind" : "FUNC_ROW_STORAGE_SIZE",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$f"
                  }
                },
                "right operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 206
                }
              }
            ]
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
          "field name" : "creation_time",
          "field expression" : 
          {
            "iterator kind" : "GREATER_OR_EQUAL",
            "left operand" :
            {
              "iterator kind" : "FUNC_CREATION_TIME",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : "2020-09-01T00:00:00.000Z"
            }
          }
        },
        {
          "field name" : "creation_ms",
          "field expression" : 
          {
            "iterator kind" : "GREATER_THAN",
            "left operand" :
            {
              "iterator kind" : "FUNC_CREATION_TIME_MILLIS",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : 1700000000
            }
          }
        },
        {
          "field name" : "mod_time",
          "field expression" : 
          {
            "iterator kind" : "GREATER_OR_EQUAL",
            "left operand" :
            {
              "iterator kind" : "FUNC_MOD_TIME",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            },
            "right operand" :
            {
              "iterator kind" : "CONST",
              "value" : "2020-09-01T00:00:00.000Z"
            }
          }
        }
      ]
    }
  }
}
}
