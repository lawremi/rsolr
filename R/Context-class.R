### =========================================================================
### Context objects
### -------------------------------------------------------------------------
###
### Where Expressions are evaluated 
###

setClassUnion("Context", "environment")

setClassUnion("ContextORNULL", c("Context", "NULL"))
