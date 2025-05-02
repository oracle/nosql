compiled-query-plan

{
"query file" : "unnest_opt/q/arr_partial01.q",
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
      "row variable" : "$t",
      "index used" : "idx_state_city_age",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"address.state":"MA"},
          "range conditions" : {}
        }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$t",
    "FROM" :
    {
      "iterator kind" : "ARRAY_SLICE",
      "low bound" : 2,
      "high bound" : 5,
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
            "variable" : "$t"
          }
        }
      }
    },
    "FROM variable" : "$phone",
    "WHERE" : 
    {
      "iterator kind" : "AND",
      "input iterators" : [
        {
          "iterator kind" : "LESS_OR_EQUAL",
          "left operand" :
          {
            "iterator kind" : "CONST",
            "value" : 500
          },
          "right operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "areacode",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$phone"
            }
          }
        },
        {
          "iterator kind" : "LESS_THAN",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "areacode",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$phone"
            }
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 600
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
            "variable" : "$t"
          }
        }
      },
      {
        "field name" : "number",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "number",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$phone"
          }
        }
      }
    ]
  }
}
}