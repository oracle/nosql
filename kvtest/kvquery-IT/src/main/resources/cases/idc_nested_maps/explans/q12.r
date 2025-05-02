compiled-query-plan

{
"query file" : "idc_nested_maps/q/q12.q",
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
      "target table" : "nestedTable",
      "row variable" : "$nt",
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
    "FROM variable" : "$nt",
    "WHERE" : 
    {
      "iterator kind" : "OP_EXISTS",
      "input iterator" :
      {
        "iterator kind" : "VALUES",
        "predicate iterator" :
        {
          "iterator kind" : "AND",
          "input iterators" : [
            {
              "iterator kind" : "EQUAL",
              "left operand" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$key"
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : "phone7"
              }
            },
            {
              "iterator kind" : "ANY_LESS_THAN",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "number",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$value"
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : 50
              }
            },
            {
              "iterator kind" : "ANY_EQUAL",
              "left operand" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "kind",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$value"
                }
              },
              "right operand" :
              {
                "iterator kind" : "CONST",
                "value" : "home"
              }
            }
          ]
        },
        "input iterator" :
        {
          "iterator kind" : "VALUES",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "phones",
            "input iterator" :
            {
              "iterator kind" : "VALUES",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "addresses",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$nt"
                }
              }
            }
          }
        }
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
            "variable" : "$nt"
          }
        }
      }
    ]
  }
}
}