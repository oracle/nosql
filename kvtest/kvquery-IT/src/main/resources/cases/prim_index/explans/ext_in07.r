compiled-query-plan

{
"query file" : "prim_index/q/ext_in07.q",
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
      "row variable" : "$$foo",
      "index used" : "primary index",
      "covering index" : true,
      "index scans" : [
        {
          "equality conditions" : {"id1":0,"id2":0.0},
          "range conditions" : { "id3" : { "start value" : "tok1", "start inclusive" : false } }
        },
        {
          "equality conditions" : {"id1":4,"id2":42.0},
          "range conditions" : { "id3" : { "start value" : "tok1", "start inclusive" : false } }
        },
        {
          "equality conditions" : {"id1":1,"id2":12.0},
          "range conditions" : { "id3" : { "start value" : "tok1", "start inclusive" : false } }
        }
      ],
      "key bind expressions" : [
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$x1"
        },
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$y2"
        },
        {
          "iterator kind" : "EXTERNAL_VAR_REF",
          "variable" : "$z1"
        }
      ],
      "map of key bind expressions" : [
        [ 0, 1, 2, -1 ],
        [ -1, -1, 2, -1 ],
        [ -1, -1, 2, -1 ]
      ],
      "position in join" : 0
    },
    "FROM variable" : "$$foo",
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
            "variable" : "$$foo"
          }
        }
      },
      {
        "field name" : "id2",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id2",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$foo"
          }
        }
      },
      {
        "field name" : "id3",
        "field expression" : 
        {
          "iterator kind" : "FIELD_STEP",
          "field name" : "id3",
          "input iterator" :
          {
            "iterator kind" : "VAR_REF",
            "variable" : "$$foo"
          }
        }
      }
    ]
  }
}
}