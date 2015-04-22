### =========================================================================
### Promise objects
### -------------------------------------------------------------------------
###
### A Promise has an expression, and a context in which to evaluate it.
###

setClass("Context")

setClass("Promise",
         representation(expr="Expression",
                        context="Context"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

expr <- function(x) x@expr
context <- function(x) x@context

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utilities
###

setGeneric("fulfill", function(x, ...) standardGeneric("fulfill"))

setMethod("fulfill", "Promise", function(x) eval(expr(x), context(x)))
