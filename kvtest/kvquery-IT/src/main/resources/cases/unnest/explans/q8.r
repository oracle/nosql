compiled-query-plan

{
"query file" : "unnest/q/q8.q",
"plan" : 
{
  "iterator kind" : "SELECT",
  "FROM" :
  {
    "iterator kind" : "RECEIVE",
    "distribution kind" : "ALL_SHARDS",
    "order by fields at positions" : [ 2 ],
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
            "equality conditions" : {},
            "range conditions" : {}
          }
        ],
        "position in join" : 0
      },
      "FROM variable" : "$t",
      "FROM" :
      {
        "iterator kind" : "ARRAY_FILTER",
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
      "FROM" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "address",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$t"
        }
      },
      "FROM variable" : "$addr",
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
          "field name" : "phone",
          "field expression" : 
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$phone"
          }
        },
        {
          "field name" : "sort_gen",
          "field expression" : 
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "state",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$addr"
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
              "value" : 3
            }
          }
        ]
      }
    }
  },
  "FROM variable" : "$from-0",
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
          "variable" : "$from-0"
        }
      }
    },
    {
      "field name" : "phone",
      "field expression" : 
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "phone",
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
    "value" : 3
  }
}
}