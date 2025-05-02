compiled-query-plan

{
"query file" : "isnull/q/n13.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "distinct by fields at positions" : [ 0 ],
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Foo",
      "row variable" : "$$f",
      "index used" : "idx_phones",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"address.phones[].work":null},
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
          "iterator kind" : "LESS_OR_EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "work",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_SLICE",
              "low bound" : 1,
              "high bound" : 1,
              "input iterator" :
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
                    "variable" : "$$f"
                  }
                }
              }
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 518
          }
        },
        {
          "iterator kind" : "OP_IS_NULL",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "work",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_SLICE",
              "low bound" : 0,
              "high bound" : 0,
              "input iterator" :
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
                    "variable" : "$$f"
                  }
                }
              }
            }
          }
        }
      ]
    },
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
            "variable" : "$$f"
          }
        }
      }
    ]
  }
}
}