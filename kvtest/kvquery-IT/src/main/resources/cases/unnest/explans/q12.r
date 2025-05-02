compiled-query-plan

{
"query file" : "unnest/q/q12.q",
"plan" : 
{
  "iterator kind" : "RECEIVE",
  "distribution kind" : "ALL_PARTITIONS",
  "input iterator" :
  {
    "iterator kind" : "SELECT",
    "FROM" :
    {
      "iterator kind" : "TABLE",
      "target table" : "Foo",
      "row variable" : "$t",
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
      "field name" : "Anna",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "children",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$t"
        }
      }
    },
    "FROM variable" : "$anna",
    "WHERE" : 
    {
      "iterator kind" : "LESS_THAN",
      "left operand" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "age",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$anna"
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : 20
      }
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
      }
    ]
  }
}
}