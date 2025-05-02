compiled-query-plan

{
"query file" : "in_expr/q/q335.q",
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
      "target table" : "foo",
      "row variable" : "$$f",
      "index used" : "idx_phones",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {"info.phones[].num":0,"info.phones[].kind":""},
          "range conditions" : {}
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "ARRAY_FILTER",
          "input iterator" :
          {
            "iterator kind" : "EXTERNAL_VAR_REF",
            "variable" : "$arr28"
          }
        }
      ],
      "map of key bind expressions" : [
        [ 0, 0 ]
      ],
      "bind info for in3 operator" : [
        {
          "theNumComps" : 2,
          "thePushedComps" : [ 0, 1 ],
          "theIndexFieldPositions" : [ 0, 1 ]
         }
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$f",
    "WHERE" : 
    {
      "iterator kind" : "OP_EXISTS",
      "input iterator" :
      {
        "iterator kind" : "ARRAY_FILTER",
        "predicate iterator" :
        {
          "iterator kind" : "IN",
          "left-hand-side expressions" : [
            {
              "iterator kind" : "FIELD_STEP",
              "field name" : "num",
              "input iterator" :
              {
                "iterator kind" : "VAR_REF",
                "variable" : "$element"
              }
            }
          ],
          "right-hand-side expressions" : [
            {
              "iterator kind" : "ARRAY_FILTER",
              "input iterator" :
              {
                "iterator kind" : "EXTERNAL_VAR_REF",
                "variable" : "$arr25"
              }
            }
          ]
        },
        "input iterator" :
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "phones",
          "input iterator" :
          {
            "iterator kind" : "FIELD_STEP",
            "field name" : "info",
            "input iterator" :
            {
              "iterator kind" : "VAR_REF",
              "variable" : "$$f"
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
            "variable" : "$$f"
          }
        }
      }
    ]
  }
}
}