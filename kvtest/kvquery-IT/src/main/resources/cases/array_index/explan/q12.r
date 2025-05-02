compiled-query-plan

{
"query file" : "array_index/q/q12.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_SHARDS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Foo",
      "row variable" : "$$t",
      "index used" : "idx_a_c_f",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"rec.a":10,"rec.c[].ca":3},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$t",
    "WHERE" : 
    {
      "iterator kind" : "OR",
      "input iterators" : [
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "f",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "rec",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$t"
              }
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 4.5
          }
        },
        {
          "iterator kind" : "GREATER_THAN",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "d2",
            "input iterator" :
            {
              "iterator kind" : "ARRAY_SLICE",
              "low bound" : 0,
              "high bound" : 0,
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "d",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "rec",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
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
            "variable" : "$$t"
          }
        }
      },
      {
        "field name" : "Column_2",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : false,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "d2",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "d",
                "input iterator" :
                {
                  "iterator kind" : "FIELD_STEP",
                  "field name" : "rec",
                  "input iterator" :
                  {
                    "iterator kind" : "VAR_REF",
                    "variable" : "$$t"
                  }
                }
              }
            }
          ]
        }
      }
    ]
  }
}
}