compiled-query-plan
{
"query file" : "rowprops/q/xupd01.q",
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
      "indexes to update" : [ "idx_city_phones", "idx_state_city_age" ],
      "update clauses" : [
        {
          "iterator kind" : "SET",
          "clone new values" : false,
          "theIsMRCounterDec" : false,
          "theJsonMRCounterColPos" : -1,
          "theIsJsonMRCounterUpdate" : false,
          "target iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "age",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$f"
            }
          },
          "new value iterator" :
          {
            "iterator kind" : "ADD_SUBTRACT",
            "operations and operands" : [
              {
                "operation" : "+",
                "operand" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$"
                }
              },
              {
                "operation" : "+",
                "operand" :
                {
                  "iterator kind" : "CONST",
                  "value" : 3
                }
              }
            ]
          }
        },
        {
          "iterator kind" : "ADD",
          "clone new values" : false,
          "theIsMRCounterDec" : false,
          "theJsonMRCounterColPos" : -1,
          "theIsJsonMRCounterUpdate" : false,
          "target iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "phones",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "address",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$f"
              }
            }
          },
          "new value iterator" :
          {
            "iterator kind" : "SEQ_CONCAT",
            "input iterators" : [
              {
                "iterator kind" : "MAP_CONSTRUCTOR",
                "input iterators" : [
                  {
                    "iterator kind" : "CONST",
                    "value" : "work"
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : 3445
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : "home"
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : 1231423
                  }
                ]
              },
              {
                "iterator kind" : "MAP_CONSTRUCTOR",
                "input iterators" : [
                  {
                    "iterator kind" : "CONST",
                    "value" : "work"
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : 3446
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : "home"
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : 1231423
                  }
                ]
              },
              {
                "iterator kind" : "MAP_CONSTRUCTOR",
                "input iterators" : [
                  {
                    "iterator kind" : "CONST",
                    "value" : "work"
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : 3447
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : "home"
                  },
                  {
                    "iterator kind" : "CONST",
                    "value" : 1231423
                  }
                ]
              }
            ]
          }
        }
      ],
      "update TTL" : false,
      "isCompletePrimaryKey" : true,
      "input iterator" :
      {
        "iterator kind" : "SELECT",
        "FROM" :
        {
          "iterator kind" : "TABLE",
          "target table" : "Foo",
          "row variable" : "$f",
          "index used" : "primary index",
          "covering index" : false,
          "index scans" : [
            {
              "equality conditions" : {"id":3},
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
    },
    "FROM variable" : "$f",
    "SELECT expressions" : [
      {
        "field name" : "age",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "age",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$f"
          }
        }
      },
      {
        "field name" : "address",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "address",
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
          "iterator kind" : "LESS_OR_EQUAL",
          "left operand" :
          {
            "iterator kind" : "ABS",
            "input iterators" : [
              {
                "iterator kind" : "ADD_SUBTRACT",
                "operations and operands" : [
                  {
                    "operation" : "+",
                    "operand" :
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
                    "operation" : "-",
                    "operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : 172
                    }
                  }
                ]
              }
            ]
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 1
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
