### =========================================================================
### Context objects
### -------------------------------------------------------------------------
###
### Where Expressions are evaluated 
###

setClassUnion("Context", "environment")

setClass("DelegateContext",
         representation(frame="Context",
                        parent="Context"))

setIs("DelegateContext", "Context")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

frame <- function(x) x@frame
`frame<-` <- function(x, value) {
    x@frame <- value
    x
}

parent <- function(x) x@parent
`parent<-` <- function(x, value) {
    x@parent <- value
    x
}

setGeneric("symbolFactory", function(x, ...) standardGeneric("symbolFactory"))
setMethod("symbolFactory", "DelegateContext",
          function(x) symbolFactory(frame(x)))

setGeneric("symbolFactory<-", function(x, ..., value)
    standardGeneric("symbolFactory<-"))
setReplaceMethod("symbolFactory", "DelegateContext", function(x, value) {
                     symbolFactory(frame(x)) <- value
                     x
                 })

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
              if (is.environment(frame(envir))) {
                  frame(envir) <- as.list(frame(envir), all.names=TRUE)
              }
              eval(expr, frame(envir), parent(envir))
          })
