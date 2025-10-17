compiled-query-plan

{
"query file" : "rowprops/q/jc_xxdel02.q",
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
        "target table" : "Boo",
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
                  "value" : 319
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
                  "value" : 457
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
          "field name" : "mod_time",
          "field expression" : 
          {
            "iterator kind" : "GREATER_THAN",
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
              "iterator kind" : "FUNC_CURRENT_TIME"
            }
          }
        }
      ]
    }
  }
}
}
