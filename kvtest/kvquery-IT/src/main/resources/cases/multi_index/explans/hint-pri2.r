compiled-query-plan

{
"query file" : "multi_index/q/hint-pri2.q",
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
      "row variable" : "$$t",
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
    "FROM variable" : "$$t",
    "WHERE" : 
    {
      "iterator kind" : "AND",
      "input iterators" : [
        {
          "iterator kind" : "EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "a",
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
            "value" : 10
          }
        },
        {
          "iterator kind" : "ANY_EQUAL",
          "left operand" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "ca",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "c",
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
          },
          "right operand" :
          {
            "iterator kind" : "CONST",
            "value" : 3
          }
        }
      ]
    },
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
            "variable" : "$$t"
          }
        }
      }
    ]
  }
}
}