compiled-query-plan

{
"query file" : "joins/q/unnest03.q",
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
      "target table" : "P",
      "row variable" : "$$p",
      "index used" : "primary index",
      "covering index" : false,
      "index scans" : [
        {
          "equality conditions" : {},
          "range conditions" : {}
        }
      ],
      "descendant tables" : [
        { "table" : "P.C", "row variable" : "$$c", "covering primary index" : false }
      ],
      "position in join" : 0
    },
    "FROM variables" : ["$$p", "$$c"],
    "FROM" :
    {
      "iterator kind" : "ARRAY_FILTER",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "arr",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$p"
        }
      }
    },
    "FROM variable" : "$pa",
    "FROM" :
    {
      "iterator kind" : "ARRAY_FILTER",
      "input iterator" :
      {
        "iterator kind" : "FIELD_STEP",
        "field name" : "arr",
        "input iterator" :
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$$c"
        }
      }
    },
    "FROM variable" : "$ca",
    "SELECT expressions" : [
      {
        "field name" : "idp",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idp",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$p"
          }
        }
      },
      {
        "field name" : "idc",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "idc",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$c"
          }
        }
      },
      {
        "field name" : "a1",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "a1",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$c"
          }
        }
      },
      {
        "field name" : "pa",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$pa"
        }
      },
      {
        "field name" : "ca",
        "field expression" : 
        {
          "iterator kind" : "VAR_REF",
          "variable" : "$ca"
        }
      }
    ]
  }
}
}