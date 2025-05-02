compiled-query-plan

{
"query file" : "gb/q/singleshard03.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "SINGLE_PARTITION",
    "distinct by fields at positions" : [ 0, 2, 3 ],
    "input iterator" :
    {
      "iterator kind" : "SELECT",
      "FROM" :
      {
        "iterator kind" : "TABLE",
        "target table" : "Bar",
        "row variable" : "$$f",
        "index used" : "idx_year_price",
        "covering index" : true,
        "index row variable" : "$$f_idx",
        "index scans" : [
          {
            "equality conditions" : {},
            "range conditions" : {}
          }
        ],
        "index filtering predicate" :
        {
          "iterator kind" : "AND",
          "input iterators" : [
            {
              "iterator kind" : "EQUAL",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "#id1",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f_idx"
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : 0
              }
            },
            {
              "iterator kind" : "ANY_GREATER_THAN",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "xact.items[].qty",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$f_idx"
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : 2
              }
            }
          ]
        },
        "position in join" : 0
      },
      "FROM variable" : "$$f_idx",
      "SELECT expressions" : [
        {
          "field name" : "id1",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#id1",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f_idx"
            }
          }
        },
        {
          "field name" : "year",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "xact.year",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f_idx"
            }
          }
        },
        {
          "field name" : "id2_gen",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#id2",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f_idx"
            }
          }
        },
        {
          "field name" : "id3_gen",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "#id3",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f_idx"
            }
          }
        }
      ]
    }
  },
  "FROM variable" : "$from-0",
  "SELECT expressions" : [
    {
      "field name" : "id1",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "id1",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "year",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "year",
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
    "value" : 2
  },
  "LIMIT" :
  {
    "iterator kind" : "CONST",
    "value" : 6
  }
}
}