compiled-query-plan

{
"query file" : "unnest_json/q/arrmap06.q",
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
            "iterator kind" : "FIELD_STEP",
            "field name" : "info",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$t"
            }
          }
        }
      }
    },
    "FROM variable" : "$phone",
    "FROM" :
    {
      "iterator kind" : "FIELD_STEP",
      "field name" : "children",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "info",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$t"
        }
      }
    },
    "FROM variable" : "$children",
    "WHERE" : 
    {
      "iterator kind" : "ANY_EQUAL",
      "left operand" :
      {
        "iterator kind" : "KEYS",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$children"
        }
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : "Anna"
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
      },
      {
        "field name" : "Mark",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "Mark",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$children"
              }
            }
          ]
        }
      }
    ]
  }
}
}