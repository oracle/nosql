compiled-query-plan
{
"query file" : "rowprops/q/xins02.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "INSERT_ROW",
    "row to insert (potentially partial)" : 
{
  "id" : 100,
  "firstName" : "first100",
  "lastName" : "last100",
  "age" : 33,
  "ptr" : "lastName",
  "address" : {
    "city" : "San Fransisco",
    "state" : "CA",
    "phones" : [{
      "work" : 504,
      "home" : 50
    }, {
      "work" : 518,
      "home" : 51
    }, {
      "work" : 528,
      "home" : 52
    }, {
      "work" : 538,
      "home" : 53
    }, {
      "work" : 548,
      "home" : 54
    }],
    "ptr" : "city"
  },
  "children" : {}
},
    "value iterators" : [

    ]
  },
  "FROM variable" : "$f",
  "SELECT expressions" : [
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
                    "value" : 166
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
