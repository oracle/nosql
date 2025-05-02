compiled-query-plan

{
"query file" : "in_expr/q/ext_q01.q",
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
      "target table" : "foo",
      "row variable" : "$$f",
      "index used" : "idx_bar1234",
      "covering index" : true,
      "index row variable" : "$$f_idx",
      "index scans" : [
        {
          "equality conditions" : {"info.bar1":0},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":0},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":"EMPTY"},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":0},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":null},
          "range conditions" : {}
        },
        {
          "equality conditions" : {"info.bar1":0},
          "range conditions" : {}
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$k1"
        },
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$k2"
        }
      ],
      "map of key bind expressions" : [
        [ 0 ],
        [ 1 ],
        null,
        [ 1 ],
        null,
        [ 0 ]
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$f_idx",
    "SELECT expressions" : [
      {
        "field name" : "id",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#id",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f_idx"
          }
        }
      }
    ]
  }
}
}