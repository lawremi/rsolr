### =========================================================================
### Promise objects
### -------------------------------------------------------------------------
###
### A Promise has an expression, and a context in which to evaluate it.
###

### Question: How far can we defer during translation? The most
###           immediate would be to build the expression as it is
###           evaluated, forcing anything that can not be incorporated
###           directly. Deferring further, we could build a tree of
###           Promise objects during evaluation. But we would need to
###           capture R code that otherwise would have forced, i.e.,
###           we have RPromises as well as
###           SolrPromises. Unfortunately, there are too many things
###           we would not be able to catch. In particular, it would
###           be odd for us to override base coercions like
###           as.integer() and friends. Those should coerce
###           immediately.
###
###           However, we could *partially* defer the evaluation. When
###           the user explicitly coerces, we must force. But
###           otherwise, we can defer until very late, late enough so
###           that many operations, including aggregations in queries,
###           are deferred until fulfillment of the SolrQuery. That
###           opens the door to meta programming. Although, we should
###           probably leave optimization to Solr.


setClass("Promise")

setClass("SimplePromise",
         representation(expr="Expression",
                        context="ContextORNULL"),
         contains="Promise")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

expr <- function(x) x@expr
context <- function(x) x@context

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Construction
###

setGeneric("Promise", function(expr, context) standardGeneric("Promise"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Fulfillment
###

setGeneric("fulfill", function(x, ...) standardGeneric("fulfill"))
setMethod("fulfill", "Promise", function(x) {
              if (!fulfillable(x)) {
                  stop("promise '", expr(x), "' cannot be fulfilled")
              }
              eval(expr(x), context(x))
          })

setGeneric("fulfillable", function(x, ...) standardGeneric("fulfillable"))
setMethod("fulfillable", "ANY", function(x) TRUE)
setMethod("fulfillable", "Promise", function(x) !is.null(context(x)))

setMethod("as.logical", "Promise", function(x) as.logical(fulfill(x)))
setMethod("as.integer", "Promise", function(x) as.integer(fulfill(x)))
setMethod("as.numeric", "Promise", function(x) as.numeric(fulfill(x)))
setMethod("as.character", "Promise", function(x) as.character(fulfill(x)))
setMethod("as.factor", "Promise", function(x) as.factor(fulfill(x)))
setMethod("as.vector", "Promise",
          function(x, mode = "any") as.vector(fulfill(x), mode=mode))
as.Date.Promise <- function(x) as.Date(fulfill(x))
as.POSIXct.Promise <- function(x) as.POSIXct(fulfill(x))
as.POSIXlt.Promise <- function(x) as.POSIXlt(fulfill(x))
as.data.frame.Promise <- function(x) as.data.frame(fulfill(x))
as.list.Promise <- function(x) as.list(fulfill(x))

setReplaceMethod("[<-", "Promise", function (x, i, j, ..., value) {
                     if (!missing(i) && is(i, "Promise"))
                         i <- fulfill(i)
                     if (!missing(j) && is(j, "Promise"))
                         j <- fulfill(j)
                     if (is(value, "Promise"))
                         value <- fulfill(value)
                     x <- fulfill(x)
                     callGeneric()
                 })

## makes Promises mostly invisible to the user
setMethod("show", "Promise", function(object) {
              show(fulfill(object))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utilities
###

replaceError <- function(from, value) {
    stop("promise is read only")
}
