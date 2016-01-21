### =========================================================================
### Promise objects
### -------------------------------------------------------------------------
###
### A Promise has an expression, and a context in which to evaluate it.
###

setClass("Promise")

setClass("SimplePromise",
         representation(expr="Expression",
                        context="Context"),
         contains="Promise")

setClass("RPromise",
         representation(expr="language",
                        context="environment"),
         contains="SimplePromise")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

expr <- function(x) x@expr
`expr<-` <- function(x, value) {
    x@expr <- value
    x
}
context <- function(x) x@context
`context<-` <- function(x, value) {
    x@context <- value
    x
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Construction
###

RPromise <- function(expr, context) {
    new("RPromise", expr=expr, context=context)
}

setGeneric("Promise", function(expr, context, ...) standardGeneric("Promise"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Fulfillment
###

setGeneric("fulfill", function(x, ...) standardGeneric("fulfill"))
setMethod("fulfill", "Promise", function(x) {
              eval(expr(x), undefer(context(x)))
          })
setMethod("fulfill", "ANY", function(x) x)


setMethod("as.logical", "Promise", function(x) as.logical(fulfill(x)))
setMethod("as.integer", "Promise", function(x) as.integer(fulfill(x)))
setMethod("as.numeric", "Promise", function(x) as.numeric(fulfill(x)))
setMethod("as.character", "Promise", function(x) as.character(fulfill(x)))
setMethod("as.factor", "Promise", function(x) as.factor(fulfill(x)))
setMethod("as.vector", "Promise",
          function(x, mode = "any") as.vector(fulfill(x), mode=mode))
as.Date.Promise <- function(x, ...) as.Date(fulfill(x))
as.POSIXct.Promise <- function(x, ...) as.POSIXct(fulfill(x))
as.POSIXlt.Promise <- function(x, ...) as.POSIXlt(fulfill(x))
as.data.frame.Promise <- function(x, row.names = NULL, optional = FALSE, ...) {
    as.data.frame(fulfill(x), row.names=row.names, optional=optional, ...)
}

as.list.Promise <- function(x, ...) as.list(fulfill(x))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Default methods that just fulfill
###

setReplaceMethod("[", "Promise", function (x, i, j, ..., value) {
                     if (!missing(i) && is(i, "Promise"))
                         i <- fulfill(i)
                     if (!missing(j) && is(j, "Promise"))
                         j <- fulfill(j)
                     if (is(value, "Promise"))
                         value <- fulfill(value)
                     x <- fulfill(x)
                     callGeneric()
                 })

setMethod("cbind2", c("ANY", "Promise"), function(x, y) {
              cbind(x, fulfill(y))
          })

setMethod("cbind2", c("Promise", "ANY"), function(x, y) {
              cbind(fulfill(x), y)
          })

setMethod("cbind2", c("Promise", "Promise"), function(x, y) {
              cbind(fulfill(x), fulfill(y))
          })

setMethod("rbind2", c("ANY", "Promise"), function(x, y) {
              rbind(x, fulfill(y))
          })

setMethod("rbind2", c("Promise", "ANY"), function(x, y) {
              rbind(fulfill(x), y)
          })

setMethod("rbind2", c("Promise", "Promise"), function(x, y) {
              rbind(fulfill(x), fulfill(y))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

## makes Promises mostly invisible to the user
setMethod("show", "Promise", function(object) {
              show(fulfill(object))
          })
