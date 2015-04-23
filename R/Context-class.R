### =========================================================================
### Context objects
### -------------------------------------------------------------------------
###
### Where Expressions are evaluated 
###

setClass("Context")

setClass("RContext", contains=c("Context", "environment"))

setClassUnion("ContextORNULL", c("Context", "NULL"))
