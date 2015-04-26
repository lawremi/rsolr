### =========================================================================
### SolrPromise objects
### -------------------------------------------------------------------------
###
### Reference to data stored in a Solr core (modified by some query)
###
###
### An interesting test case for extensibility would be an external
### package that adds spatial query support for Ranges objects:
### pos %over% IRanges(1,10) => pos:[1 TO 10]
### pos %over% GRanges("1", IRanges(1,10)) => pos:[1,1 TO 10,1]
### pos %over% circle(cx,cy,d) => {!frange l=0 u=d}geodist(pos, cx, cy)
###
### Other things to add:
### Transform:
###  cut() using solr::if(lucene::range, 1, if (lucene::range, 2, ...))
### Filter/transform:
###  grepl(), which would just be an alias for "=="
### Aggregation:
###  anyNA(), because faceting counts missing values
### Output restriction:
###  head()/tail()
### On promises:
###  unique(), from facets
###  intersect(), unique(x[x %in% table])
###  range(), from min+max
###  length(), via nrow,SolrFrame()
###

### All translations follow this basic strategy:

### 1. Coerce Promise arguments to the target Promise subclass. It is
###    important that we coerce at the Promise level, so that we can
###    defer when necessary by emebdding the Promise in the target
###    Expression type.
### 2. Extract the Expressions from the Promises and combine with any
###    literals to form a compound Expression.
### 3. Ensure that all Promises have the same Context.
### 4. Create a new Promise with the compound Expression and shared Context.

### FIXME: Gracefully handle the case of non-scalar arguments
###        (arguments that cannot be encoded in a query). This would
###        require defining an R-level Promise, because we will need
###        to download the data and perform the work in R. While we
###        want things to "just work", this is low priority, because
###        the user is better off just coercing to data.frame.

### FIXME: Gracefully handle the case of conflicting contexts.  This
###        would be relatively easy to implement by wrapping the
###        foreign Promises in the target Expression type, so that
###        they are eventually forced. Note that they must each
###        evaluate to a scalar (aggregate), otherwise, the FIXME
###        above comes into play. This seems strange enough that it is
###        a low priority.

### FIXME: Gracefully handle the case of missing Solr implementation,
###        i.e., when there is no hope of encoding an operation in a
###        Solr language. Taken to the extreme, this would mean
###        methods for all possible generics, and most of the time,
###        the result would be useless due to the scalar
###        limitation. We already capture pretty much all base R
###        aggregation functions, so this is a low priority.

setClass("SolrPromise",
         representation(expr="SolrExpression",
                        context="SolrFrameORNULL"),
         contains="SimplePromise")

setClassUnion("SolrLuceneExpressionOrSymbol",
              c("SolrLuceneExpression", "SolrSymbol"))

setClass("SolrLucenePromise",
         representation(expr="SolrLuceneExpressionOrSymbol"),
         contains="SolrPromise")

### NOTE: if we do this, we should probably have SolrLucenePromise
### inherit directly from Promise.
setIs("SolrLucenePromise", "logical")

setClass("SolrFunctionPromise",
         representation(expr="SolrFunctionExpression"),
         contains="SolrPromise")

### Not quite true, only function CALLS return numeric. For arbitrary
### symbols, we would need one subclass for each type.
##setIs("SolrFunctionPromise", "numeric")

setClass("SolrAggregatePromise",
         representation(expr="SolrAggregateExpression"),
         contains="SolrPromise")

setIs("SolrAggregatePromise", "numeric")

setClass("SolrSymbolPromise",
         representation(expr="SolrSymbol"),
         contains="SolrFunctionPromise")

setClass("SolrLuceneSymbolPromise",
         contains=c("SolrLucenePromise", "SolrSymbolPromise"))

setClass("PredicatedSolrSymbolPromise",
         representation(expr="PredicatedSolrSymbol"),
         contains="SolrPromise")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

newSolrPromise <- function(class, expr, context) {
    new(class, expr=SolrSymbol(expr), context=solr(context))
}

setMethod("Promise", c("name", "SolrLuceneExpression", "RSolrContext"),
          function(expr, target, context) {
              newSolrPromise("SolrLuceneSymbolPromise", expr, context)
          })

setMethod("Promise", c("name", "SolrFunctionExpression", "RSolrContext"),
          function(expr, target, context) {
              newSolrPromise("SolrSymbolPromise", expr, context)
          })

setMethod("Promise", c("name", "SolrAggregateExpression", "RSolrContext"),
          function(expr, target, context) {
              newSolrPromise("SolrSymbolPromise", expr, context)
          })

SolrLucenePromise <- function(expr, context) {
    new("SolrLucenePromise", expr=as(expr, "SolrLuceneExpression"),
        context=context)
}

SolrFunctionPromise <- function(expr, context) {
    new("SolrFunctionPromise", expr=as(expr, "SolrFunctionExpression"),
        context=context)
}

SolrAggregatePromise <- function(expr, context) {
    new("SolrAggregatePromise", expr=as(expr, "SolrAggregateExpression"),
        context=context)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Fulfillment
###

### TODO

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Lucene translation
###

setMethod("%in%", c("SolrSymbolPromise", "PredicatedSolrSymbolPromise"),
          function(x, table) {
              ctx <- resolveContext(x, table)
              expr <- JoinQParserExpression(query=expr(table)@predicate,
                                            from=expr(table)@subject,
                                            to=expr(x))
              SolrLucenePromise(expr, ctx)
          })

setMethod("%in%", c("SolrSymbolPromise", "SolrSymbolPromise"),
          function(x, table) {
              x %in% table[]
          })

setMethod("%in%", c("SolrSymbolPromise", "ANY"),
          function(x, table) {
              term <- SolrLuceneTerm(NULL, table)
              expr <- LuceneQParserExpression(term, df=expr(x))
              SolrLucenePromise(expr, context(x))
          })

### We are greedy here, which is probably OK, since most logical
### expressions are conveniently expressed with Lucene syntax...
setMethods("Logic",
           list(c("SolrPromise", "SolrLucenePromise"),
                c("SolrLucenePromise", "SolrPromise")),
           function(e1, e2) {
               e1 <- as(e1, "SolrLucenePromise")
               e2 <- as(e2, "SolrLucenePromise")
               ctx <- resolveContext(e1, e2)
               expr <- if (.Generic == "&") {
                   SolrLuceneAND(expr(e1), expr(e2))
               } else {
                   SolrLuceneOR(expr(e1), expr(e2))
               }
               LuceneSolrPromise(expr, ctx)
           })

setMethod("!", "SolrPromise", function(x) {
              x <- as(x, "SolrLucenePromise")
              SolrLucenePromise(LuceneNOT(expr(x)), context(x))
          })

setMethod("is.na", "SolrLuceneSymbolPromise", function(x) {
              x != I("*:*")
          })

setMethods("Comparison",
           list(c("SolrSymbolPromise", "ANY"),
                c("ANY", "SolrSymbolPromise")),
           function(e1, e2) {
               luceneRelational(.Generic, e1, e2)
           })

setMethods("Comparison",
           list(c("SolrPromise", "numeric"),
                c("numeric", "SolrPromise")),
           function(e1, e2) {
               frangeRelational(.Generic, e1, e2)
           })

setMethod("[", "SolrSymbolPromise", function(x, i, j, ..., drop = TRUE) {
              if (!missing(j) || length(list(...)) > 0L || !missing(drop)) {
                  stop("'[' only accepts x[i] or x[] syntax")
              }
              if (missing(i)) {
                  i <- I("*:*")
                  ctx <- context(x)
              } else if (!is(i, "literal")) {
                  if (!is(i, "Promise")) {
                      stop("currently, 'i' must be a Promise")
                  }
                  ctx <- resolveContext(x, i)
                  i <- expr(i)
              }
              symbol <- PredicatedSolrSymbol(expr(x), i)
              PredicatedSolrSymbolPromise(symbol, ctx)
          })

setMethod("grepl", c("ANY", "SolrSymbolPromise"),
          function(pattern, x, ignore.case = FALSE, perl = FALSE,
                   fixed = FALSE, useBytes = FALSE) {
              if (!isSingleString(pattern)) {
                  stop("'pattern' must be a single, non-NA string")
              }
              if (!identical(ignore.case, FALSE)) {
                  stop("'ignore.case' must be FALSE")
              }
              if (!identical(perl, FALSE)) {
                  stop("'perl' must be FALSE")
              }
              if (!isTRUEorFALSE(fixed)) {
                  stop("'fixed' must be TRUE or FALSE")
              }
              if (!identical(useBytes, FALSE)) {
                  stop("'useBytes' must be FALSE")
              }
              if (!fixed) {
                  pattern <- I(paste0("/", pattern, "/"))
              }
              SolrLucenePromise(SolrLuceneTerm(expr(x), pattern))
          })

setMethod("grep", c("ANY", "SolrSymbolPromise"),
          function(pattern, x, ignore.case = FALSE, perl = FALSE,
                   value = FALSE, fixed = FALSE, useBytes = FALSE,
                   invert = FALSE) {
              if (!isTRUE(value)) {
                  stop("'value' must be TRUE")
              }
              if (!isTRUEorFALSE(invert)) {
                  stop("'invert' must be TRUE or FALSE")
              }
              i <- grepl(pattern, x, ignore.case, perl, fixed, useBytes)
              if (invert) {
                  i <- !i
              }
              x[i]
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Lucene coercion
###

setAs("SolrPromise", "SolrLucenePromise", function(from) {
          SolrLucenePromise(as(expr(from), "SolrLuceneExpression"),
                            context(from))
      })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Lucene utilities
###

frangeRelational <- function(fun, x, y) {
    promise <- if (is.numeric(x)) y else x
    query <- expr(as(promise, "SolrFunctionPromise"))
    num  <- if (is.numeric(x)) x else y
    expr <- FRangeQParserExpression(query,
                                    l    = if (fun %in% c(">", ">=", "==")) num,
                                    u    = if (fun %in% c("<", "<=", "==")) num,
                                    incl = fun %in% c(">=", "=="),
                                    incu = fun %in% c("<=", "=="))
    SolrLucenePromise(expr, context(promise))
}

reverseRelational <- function(call) {
    call$fun <- chartr("<>", "><", call$fun)
    call[c("x", "y")] <- call[c("y", "x")]
    call
}

### NOTE: '==' will *search* text fields, not look for an exact match.
###       String and other fields behave as expected.

luceneRelational <- function(fun, x, y) {
    if (fun == "!=") {
        return(!(x == y))
    }

    call <- list(fun=fun, x=x, y=y)
    if (is(y, "SolrSymbolPromise")) {
        call <- reverseRelational(call)
        return(do.call(luceneRelational, call))
    }
    
    expr <- switch(fun,
                   "==" = SolrLuceneTerm(expr(x), y),
                   ">"  = SolrLuceneRangeTerm(expr(x), y, I("*"), FALSE, TRUE),
                   ">=" = SolrLuceneRangeTerm(expr(x), y, I("*"), TRUE, TRUE),
                   "<"  = SolrLuceneRangeTerm(expr(x), I("*"), y, TRUE, FALSE),
                   "<=" = SolrLuceneRangeTerm(expr(x), I("*"), y, TRUE, TRUE))
    ctx <- resolveContext(x, y)
    LuceneSolrPromise(expr, ctx)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Function translation
###

setMethods("Logic",
           list(c("SolrFunctionPromise", "SolrFunctionPromise")
                c("ANY", "SolrPromise"),
                c("SolrPromise", "ANY")),
           function(e1, e2) {
               solrCall(if (.Generic == "&") "and" else "or", e1, e2)
           })

setMethod("!", "SolrFunctionPromise", function(x) {
              solrCall("not", x)
          })

setMethod("Comparison", c("SolrPromise", "SolrPromise"),
          function(e1, e2) {
              switch(.Generic,
### crazy construct enables field comparisons using Solr functions
                     ">" = ifelse(ceiling(e1 / e2) - 1L, TRUE, FALSE),
                     "<" = e2 > e1,
                     ">=" = !(e1 < e2),
                     "<=" = !(e1 > e2),
                     "==" = !(e1 != e2),
                     "!=" = ifelse(abs(e1 - e2), TRUE, FALSE)
                     )
          })

setMethods("Arith",
           list(c("numeric", "SolrPromise"),
                c("SolrPromise", "numeric"),
                c("SolrPromise", "SolrPromise")),
           function(e1, e2) {
               if (.Generic == "%/%") {
                   floor(e1 / e2)
               } else {
                   fun <- switch(.Generic,
                                 '+' = "sum",
                                 '-' = "sub",
                                 '*' = "product",
                                 '/' = "div",
                                 '%%' = "mod",
                                 '^' = "pow")
                   solrCall(fun, e1, e2)
               }
           })

setMethod("-", c("SolrPromise", "missing"), function(e1, e2) {
              e1 * -1L
          })

setMethod("Math", "SolrPromise", function(x) {
              fun <- switch(.Generic,
                            abs = "abs",
                            sqrt = "sqrt",
                            ceiling = "Math.ceil",
                            floor = "Math.floor",
                            log = "Math.ln",
                            log10 = "log",
                            acos = "Math.acos",
                            asin = "Math.asin",
                            atan = "Math.atan",
                            exp = "Math.exp",
                            cos = "Math.cos",
                            cosh = "Math.cosh",
                            sin = "Math.sin",
                            sinh = "Math.sinh",
                            tan = "Math.tan",
                            tanh = "Math.tanh")
              if (is.null(fun)) {
                  prom <- switch(.Generic, ## will not be as accurate as native
                                 sign = x / abs(x),
                                 trunc = floor(abs(x)) * sign(x),
                                 log1p = log(x + 1),
                                 acosh = log(x + sqrt(x^2 - 1)),
                                 asinh = log(x + sqrt(x^2 + 1)),
                                 atanh = 0.5 * log((1+x)/(1-x)),
                                 expm1 = exp(x) - 1,
                                 cospi = cos(x * Constant(x, "pi")),
                                 sinpi = sin(x * Constant(x, "pi")),
                                 tanpi = tan(x * Constant(x, "pi")))
                  if (is.null(prom)) {
                      ## cummax, cummin, cumprod, cumsum, log2, *gamma
                      x <- fulfill(x)
                      return(callGeneric())
                  }
              } else {
                  prom <- solrCall(fun, x)
              }
              prom
          })

setMethod("round", "SolrPromise", function(x, digits = 0L) {
              if (digits != 0L) {
                  stop("'digits' must be 0")
              }
              solrCall("Math.rint", x)
          })

setGeneric("rescale", function(x, ...) standardGeneric("rescale"))

setMethod("rescale", "SolrPromise", function(x, min, max) {
              if (!isSingleNumber(min)) {
                  stop("'min' must be a single, non-NA number")
              }
              if (!isSingleNumber(max)) {
                  stop("'max' must be a single, non-NA number")
              }
              solrCall("scale", x, min, max)
          })

setPromiseMethods("rescale")

## generic in S4Vectors
setMethod("ifelse",
          c("SolrPromise", "ANY", "ANY"),
          function(test, yes, no) {
              solrCall("if", test, yes, no)
          })

setGeneric("pmax2", function(x, y, na.rm=FALSE) standardGeneric("pmax2"),
           signature=c("x", "y"))

setMethod("pmax2", "ANY", function(x, y, na.rm=FALSE) {
              pmax(x, y, na.rm=na.rm)
          })

setMethods("pmax2",
           list(c("numeric", "SolrPromise"),
                c("SolrPromise", "numeric"),
                c("SolrPromise", "SolrPromise")),
           function(x, y, na.rm=FALSE) {
               if (!identical(na.rm, FALSE)) {
                   stop("'na.rm' must be FALSE")
               }
               solrCall("max", x, y)
           })

setGeneric("pmin2", function(x, y, na.rm=FALSE) standardGeneric("pmin2"),
           signature=c("x", "y"))

setMethod("pmin2", "ANY", function(x, y, na.rm=FALSE) {
              pmin(x, y, na.rm=na.rm)
          })

setMethods("pmin2",
           list(c("numeric", "SolrPromise"),
                c("SolrPromise", "numeric"),
                c("SolrPromise", "SolrPromise")),
           function(x, y, na.rm=FALSE) {
               if (!identical(na.rm, FALSE)) {
                   stop("'na.rm' must be FALSE")
               }
               solrCall("min", x, y)
           })

setMethod("is.na", "SolrFunctionSymbol", function(x) {
              !solrCall("exists", x)
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Function coercion
###

setAs("SolrPromise", "SolrFunctionPromise", function(from) {
          SolrFunctionPromise(as(expr(from), "SolrFunctionExpression"),
                              context(from))
      })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Function utilities
###

checkArgLengths <- function(args) {
    if (any(lengths(args) != 1L))
        stop("all arguments to a Solr function must have length 1")
}

solrCall <- function(fun, ...) {
    args <- lapply(list(...), function(x) {
                       if (is(x, "Promise"))
                           as(x, "SolrFunctionPromise")
                       else x
                   })
    checkArgLengths(args)
    expr <- SolrFunctionCall(fun, args)
    ctx <- resolveContext(...)
    SolrFunctionPromise(expr, ctx)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Aggregation
###
### Most of the implementation happens in SolrQuery and the result parsing,
### so this is more or less quoting the calls.
###

### We can translate the following functions:
###
### sum => sum [Summary], and any/all with some math
### avg => mean
### sumsq ~> var, sd
### min/max => min/max [Summary]
### unique => countUnique? nunique?
### percentile => quantile, median
### min+max => range [Summary, just add both stats]
### IQR, mad, etc
###
### FIXME: Unhandled: prod(), which is uncommon anyway
###

### The na.rm slot on SolrAggregateExpression is handled appropriately
### by the underlying classes (SolrQuery and the result parsing).

### The SolrAggregateExpression has two special abilities. First, it
### can pass along additional, supporting statistics to calculate. The
### other is a function that post-processes the result, usually to
### combine the result with the auxillary statistics. It is passed
### itself, its value, as well as the list of auxillary stats.

### By convention, the auxillary stats should be prefixed with
### ".". All stats with "." are removed from the result after
### processing.

### Simple example: any() sends sum() but then does > 0 on its value
### Complex example: weighted.mean sends sum(product(x, w)),
###                  augments it with sum(w), and later does val/sum(w).

setMethod("Summary", "SolrPromise",
          function (x, ..., na.rm = FALSE) {
              fun <- switch(.Generic,
                            max="max",
                            min="min",
                            range="max",
                            sum="sum",
                            any="sum",
                            all="sum")
              aux <- list()
              postprocess <- NULL
              if (.Generic == "range") {
                  aux <- list(min=min(x))
                  postprocess <- function(max, aux) max - aux$min
              } else if (.Generic == "any") {
                  postprocess <- function(sum, aux) sum > 0L
              } else if (.Generic == "all") {
                  postprocess <- function(sum, aux) sum == aux$count
              }
              prom <- solrAggregate(.Generic, x, na.rm, aux=aux,
                                    postprocess=postprocess)
              prom
          })

setMethod("mean", "SolrPromise", function(x, na.rm=FALSE) {
              solrAggregate("avg", x, na.rm)
          })

ppVar <- function(sumsq, aux) sumsq / (aux$count - 1L)

setMethod("var", "SolrPromise", function(x, na.rm=FALSE) {
              solrAggregate("sumsq", x, na.rm, postprocess=ppVar)
          })

setMethod("sd", "SolrPromise", function(x, na.rm=FALSE) {
              solrAggregate("sumsq", x, na.rm,
                            postprocess=function(sumsq, aux) {
                                sqrt(ppVar(sumsq, aux))
                            })
          })

lengthUnique <- function(x, na.rm=FALSE) {
    solrAggregate("unique", x, na.rm)
}

setGeneric("quantile",
           function (x, probs = seq(0, 1, 0.25), na.rm = FALSE,
                     names = TRUE, type = 7, ...) standardGeneric("quantile"),
           signature="x")

setMethod("quantile", "SolrPromise",
          function (x, probs = seq(0, 1, 0.25), na.rm = FALSE, names = TRUE,
                    type = 7) {
              if (!missing(type)) {
                  warning("'type' is ignored")
              }
              if (!isTRUEorFALSE(names)) {
                  stop("'names' must be TRUE or FALSE")
              }
              probs <- probs * 100
              solrAggregate("percentile", x, na.rm, as.list(probs),
                            postprocess = function(percentile, aux) {
                                m <- as.matrix(percentile)
                                if (isTRUE(names))
                                    colnames(m) <- paste0(probs, "%")
                                m
                            })
          })

setMethod("median", "SolrPromise", function(x, na.rm=FALSE) {
              solrAggregate("percentile", x, na.rm, list(50))
          })

setGeneric("weighted.mean", function(x, w, ...)
    standardGeneric("weighted.mean"))

setMethod("weighted.mean", c("SolrPromise", "SolrPromise"),
          function(x, w, na.rm=FALSE) {
              solrAggregate("sum", x*w, na.rm, aux=list(wsum=sum(w)),
                            postprocess=function(sum, aux) {
                                sum / aux$wsum
                            })
          })

setGeneric("IQR", function(x, na.rm=FALSE) standardGeneric("IQR"),
           signature="x")

setMethod("IQR", "SolrPromise",
          function(x, na.rm=FALSE) {
              solrAggregate("percentile", x, na.rm, list(25, 75),
                            postprocess=function(percentile, aux) {
                                percentile[,2] - percentile[,1]
                            })
          })

setGeneric("mad", function(x, center = median(x), constant = 1.4826,
                           na.rm = FALSE, low = FALSE, high = FALSE)
           standardGeneric("mad"), signature=c("x", "center"))

setMethod("mad", c("SolrPromise", "SolrAggregatePromise"),
          function (x, center = median(x), constant = 1.4826, na.rm = FALSE, 
                    low = FALSE, high = FALSE) {
              if (!identical(low, FALSE) || !identical(high, FALSE)) {
                  stop("'high' and 'low' must be FALSE")
              }
              ## FIXME: computing center requires an extra round trip...
              ## ... and breaks this for grouped data.
              med <- median(abs(x - center), na.rm=na.rm)
              med@postprocess <- function(median, aux) {
                  median * constant
              }
              med
          })

solrAggregate <- function(fun, x, na.rm, params = list(), aux = list(),
                          postprocess = NULL)
{
    if (!isTRUEorFALSE(na.rm)) {
        stop("'na.rm' must be TRUE or FALSE")
    }
    x <- as(x, "SolrFunctionPromise")
    expr <- SolrAggregateExpression(fun, expr(x), na.rm, params)
    SolrAggregatePromise(expr)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Aggregate coercion
###

setAs("SolrAggregatePromise", "SolrFunctionPromise", function(from) {
          SolrFunctionPromise(SolrFunctionExpression(from), context(from))
      })

setAs("SolrAggregatePromise", "SolrLucenePromise", function(from) {
          SolrLucenePromise(SolrLuceneTerm(NULL, from))
      })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### General utilities
###

resolveContext <- function(args) {
    isProm <- vapply(args, is, "Promise", FUN.VALUE=logical(1L))
    ctx <- Filter(Negate(is.null), lapply(args[isProm], context))
    if (any(!vapply(ctx[-1], identical, ctx[[1L]], FUN.VALUE=logical(1L)))) {
        stop("cannot combine promises from different contexts")
    }
    ctx
}

