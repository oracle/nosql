compiled-query-plan
{
"query file" : "rowprops/q/jc_xins02.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "INSERT_ROW",
    "row to insert (potentially partial)" : 
{
  "id" : 100,
  "address" : {
    "city" : "San Fransisco",
    "phones" : [{
      "home" : 50,
      "work" : 504
    }, {
      "home" : 51,
      "work" : 518
    }, {
      "home" : 52,
      "work" : 528
    }, {
      "home" : 53,
      "work" : 538
    }, {
      "home" : 54,
      "work" : 548
    }],
    "state" : "CA"
  },
  "age" : 33,
  "firstName" : "first100",
  "lastName" : "last100"
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
                    "value" : 320
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
