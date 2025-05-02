compiled-query-plan

{
"query file" : "queryspec/q/funcidx03.q",
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
      "target table" : "Users",
      "row variable" : "$$u",
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
    "FROM variable" : "$$u",
    "WHERE" : 
    {
      "iterator kind" : "EQUAL",
      "left operand" :
      {
        "iterator kind" : "FN_SUBSTRING",
        "input iterators" : [
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "first",
            "input iterator" :
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "otherNames",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$$u"
              }
            }
          },
          {
            "iterator kind" : "CONST",
            "value" : 1
          },
          {
            "iterator kind" : "CONST",
            "value" : 2
          }
        ]
      },
      "right operand" :
      {
        "iterator kind" : "CONST",
        "value" : "re"
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
            "variable" : "$$u"
          }
        }
      },
      {
        "field name" : "first",
        "field expression" : 
        {
          "iterator kind" : "ARRAY_CONSTRUCTOR",
          "conditional" : true,
          "input iterators" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "first",
              "input iterator" :
              {
                "iterator kind" : "FIELD_STEP",
                "field name" : "otherNames",
                "input iterator" :
                {
                  "iterator kind" : "VAR_REF",
                  "variable" : "$$u"
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