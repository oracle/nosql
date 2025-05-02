#
# Result files have 3 parts: 1) optional comment lines,
#                            2) result-type line and
#                            3) result-content
#
# 1) Comment lines start with \s*#
#
# 2) Result-type line is mandatory and can be one of:
#   Compile-exception, Runtime-exception, Unordered-result or Ordered-result
#
# 3) Result-content:
# For Compile-exception or Runtime-exception the rest of the file may contain
# an exception message.
# For Unordered-result or Ordered-result the rest of the file contains
# un/ordered result records. One record per line (for now). Each line is
# trimmed of spaces.
#
#
Unordered-result
{"lastName":"last7","age":17}
{"lastName":"last8","age":18}
{"lastName":"last1","age":11}
{"lastName":"last3","age":13}
{"lastName":"last2","age":12}
{"lastName":"last6","age":16}
{"lastName":"last4","age":14}
{"lastName":"last5","age":15}
{"lastName":"last0","age":10}
{"lastName":"last9","age":19}