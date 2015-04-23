### =========================================================================
### Promise objects
### -------------------------------------------------------------------------
###
### A Promise has an expression, and a context in which to evaluate it.
###

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
### Delegate methods
###
### The expression tree consists of Promise objects, as well as other,
### non-lazy objects. We need to forward any function call on a
### Promise to the corresponding method on the Expression, and then
### cast the result back to a Promise. Since there can be any
### combination of Promises in the signature, we must cover all
### possible signatures. We also need to ensure that every promise
### comes from the same context. Otherwise, it is not possible to
### determine a single context for the result.
###
### Some helper functions make this easier.
###

resolveContext <- function(...) {
    args <- list(...)
    sameContext <- vapply(args[-1L],
                          function(x, first) identical(context(x), first),
                          context(args[[1L]]), FUN.VALUE=logical(1L))
    if (!all(sameContext)) {
        stop("promises mixed from different contexts")
    }
    context(args[[1L]])
}

setPromiseMethods <- function(generic, gensig, ...) {
    generic <- getGeneric(generic, mustFind=TRUE)
    if (missing(gensig)) {
        gensig <- generic@signature
    } else if (any(!(gensig %in% generic@signature))) {
        stop("invalid signature for generic")
    }
    factors <- rep(list(c("ANY", "Promise")), length(gensig))
    sigmatrix <- as.matrix(do.call(expand.grid, factors))
    sigs <- split(sigmatrix, row(sigmatrix))
    for (sig in sigs) {
        if (!isSealedMethod(generic@generic, sig)) {
            setPromiseMethod(generic, setNames(sig, gensig))
        }
    }
}

setPromiseMethod <- function(generic, sig, ...) {
    promises <- lapply(names(sig)[sig == "Promise"], as.name)
    resolveContext <- as.call(c(quote(resolveContext), promises))
    assignments <- lapply(promises, function(p) {
                              call("<-", p, call("expr", p))
                          })
    genericCall <- call("Promise", call("callGeneric"), resolveContext)
    body <- as.call(c(quote(`{`), assignments, list(genericCall)))
    FUN <- as.function(c(formals(generic), list(body)),
                       environment(sys.function()))
    setMethod(generic, sig, FUN, ...)
}

setPromiseMethods("Logic")
setPromiseMethods("!")
setPromiseMethods("is.na")
setPromiseMethods("Comparison")
setPromiseMethods("%in%")
setPromiseMethods("Arith")
setPromiseMethod("-", c("Promise", "missing")) # unary minus
setPromiseMethods("Math")
setPromiseMethods("round")
setPromiseMethods("ifelse")
setPromiseMethods("pmax2")
setPromiseMethods("pmin2")
setPromiseMethods("[", c("x", "i", "j")) # no 'drop'
