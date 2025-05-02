compiled-query-plan

{
"query file" : "idc_indexing_functions/q/funcidx_foo_11.q",
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
      "index used" : "idx_rtrim_name",
      "covering index" : true,
      "index row variable" : "$$f_idx",
      "index scans" : [
        {
          "equality conditions" : {"rtrim#name":""},
          "range conditions" : {}
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "ARRAY_FILTER",
          "input iterator" :
          {
            "iterator kind" : "EXTERNAL_VAR_REF",
            "variable" : "$arr7"
          }
        }
      ],
      "map of key bind expressions" : [
        [ 0 ]
      ],
      "bind info for in3 operator" : [
        {
          "theNumComps" : 1,
          "thePushedComps" : [ 0 ],
          "theIndexFieldPositions" : [ 0 ]
         }
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
      },
      {
        "field name" : "name",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "#name",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$f_idx"
          }
        }
      },
      {
        "field name" : "rtrim_name",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "rtrim#name",
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