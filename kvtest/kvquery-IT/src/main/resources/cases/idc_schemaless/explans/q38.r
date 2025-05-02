compiled-query-plan

{
"query file" : "idc_schemaless/q/q38.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "SINGLE_PARTITION",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "jsoncol",
      "row variable" : "$f",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {"majorKey1":"hello","majorKey2":"helloo","minorKey":"hellooo"},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$f",
    "SELECT expressions" : [
      {
        "field name" : "Column_1",
        "field expression" : 
        {
          "iterator kind" : "MULTIPLY_DIVIDE",
          "operations and operands" : [
            {
              "operation" : "*",
              "operand" :
              {
                "iterator kind" : "FUNC_EXPIRATION_TIME_MILLIS",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$f"
                }
              }
            },
            {
              "operation" : "/",
              "operand" :
              {
                "iterator kind" : "MULTIPLY_DIVIDE",
                "operations and operands" : [
                  {
                    "operation" : "*",
                    "operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : 1000
                    }
                  },
                  {
                    "operation" : "*",
                    "operand" :
                    {
                      "iterator kind" : "CONST",
                      "value" : 3600
                    }
                  }
                ]
              }
            }
          ]
        }
      }
    ]
  }
}
}