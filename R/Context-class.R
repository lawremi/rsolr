### =========================================================================
### Context objects
### -------------------------------------------------------------------------
###
### Where Expressions are evaluated 
###

setClassUnion("Context", "environment")

setClass("DelegateContext",
         representation(frame="Context",
                        parent="Context"),
         contains="Context")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

frame <- function(x) x@frame
parent <- function(x) x@parent

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

DelegateContext <- function(frame, parent) {
    new("DelegateContext", frame=frame, parent=parent)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Evaluation
###

setMethod("eval", c("ANY", "DelegateContext"),
          function (expr, envir, enclos) {
              if (!missing(enclos)) {
                  warning("'enclos' is ignored")
              }
              eval(expr, frame(context), parent(context))
          })
