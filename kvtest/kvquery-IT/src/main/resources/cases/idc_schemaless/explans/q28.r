compiled-query-plan

{
"query file" : "idc_schemaless/q/q28.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_PARTITIONS",
    "order by fields at positions" : [ 1 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "jsoncol",
        "row variable" : "$$jsoncol",
        "index used" : "primary index",
        "covering index" : false,
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$$jsoncol",
      "SELECT expressions" : [
        {
          "field name" : "jsoncol",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$jsoncol"
          }
        },
        {
          "field name" : "sort_gen",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "majorKey1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$jsoncol"
            }
          }
        }
      ],
      "LIMIT" :
      {
        "iterator kind" : "ADD_SUBTRACT",
        "operations and operands" : [
          {
            "operation" : "+",
            "operand" :
            {
              "iterator kind" : "CONST",
              "value" : 1
            }
          },
          {
            "operation" : "+",
            "operand" :
            {
              "iterator kind" : "CONST",
              "value" : 2
            }
          }
        ]
      }
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "jsoncol",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "jsoncol",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    }
  ],
  "OFFSET" :
  {
    "iterator kind" : "CONST",
    "value" : 1
  },
  "LIMIT" :
  {
    "iterator kind" : "CONST",
    "value" : 2
  }
}
}