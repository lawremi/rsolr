### =========================================================================
### SolrPromise objects
### -------------------------------------------------------------------------
###
### Reference to data stored in a Solr core (modified by some
### query). SolrPromises map R operations down to low-level operations
### on Solr Expressions. Thus, there is a separate type of Promise for
### each Solr language.
###
### An interesting test case for extensibility would be an external
### package that adds spatial query support for Ranges objects:
### pos %over% IRanges(1,10) => pos:[1 TO 10]
### pos %over% GRanges("1", IRanges(1,10)) => pos:[1,1 TO 10,1]
### pos %over% circle(cx,cy,d) => {!frange l=0 u=d}geodist(pos, cx, cy)
###
### Other things to add:
### Transform:
###  cut() using nested map() calls, or defer and wait for table()=>facets
### 
### rank(na.last='keep', ties.method='first') and xtfrm() using ord().
### - need to somehow convert NA/null to 0, -1 to NA, and add 1 (for rank).
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

### FIXME: Solr functions do not propagate missingness. In fact, they
###        are documented to always return 0 when missing. A possible
###        work-around is to wrap every call with:
##            if(exists(arg), fun(arg), query({!lucene v='-*:*'}))
###        but wow, that's crazy. Alternative: each call to a Solr
###        function is wrapped in an
##            if(and(exists(sym1), ...)), ., query({!lucene v='-*:*'}))
###        where condition is composed of the condition for each of
###        the call args, and there is a new exists() added for each
###        symbol arg. If exists() is called explicitly, it is given
###        that condition from the previous call (if there is
###        one). The call to exists() is *not* wrapped in the
###        if(). The implementation is simple: the expression keeps
###        track of symbols that are potentially NA, and each function
###        (except is.na) builds an expression with the unique set of
###        potential missing symbols. It clears the missing symbols
###        from each of its arguments, possibly via class
###        coercion. During coercion to character, the NA-protected
###        expression constructs the necessary calls to exists(). The
###        is.na() function calls exist() on each of the missing
###        symbols in its argument, and and()s the result. The
###        ifelse() function is another special case: we cannot
###        propagate potential missings up past the Solr if()
###        condition. Luckily, if() does propagate missings, so we
###        just do nothing. But missables should propagate past the
###        condition argument.

setClass("SolrPromise",
         representation(expr="SolrExpression",
                        context="Context"),
         contains="SimplePromise")

setIs("SolrLuceneExpressionOrSymbol", "SolrExpression")

setClass("SolrLucenePromise",
         representation(expr="SolrLuceneExpressionOrSymbol"),
         contains="SolrPromise")


setClass("SolrFunctionPromise",
         representation(expr="SolrFunctionExpression"),
         contains="SolrPromise")

setClass("SolrReducePromise", contains="SolrPromise")

setClass("SolrAggregatePromise",
         representation(expr="SolrAggregateCall"),
         contains="SolrReducePromise")

setClass("SolrSymbolPromise",
         representation(expr="SolrSymbol"),
         contains="SolrFunctionPromise")

setClass("SolrLuceneSymbolPromise",
         contains=c("SolrSymbolPromise", "SolrLucenePromise"))

setClass("PredicatedSolrSymbolPromise",
         representation(expr="PredicatedSolrSymbol"),
         contains="SolrPromise")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Relations to basic classes
###

### One could imagine defining abstract fundamental data types:
## setClassUnion("Logical", c("SolrLucenePromise", "logical"))
## setClassUnion("Numeric", c("SolrAggregatePromise", "numeric"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

### Translation proceeds by creating Promises in a special translation
### context. It evaluates the expression in that context to a Promise,
### and then extracts the Expression from the Promise.

computedFieldPromise <- function(solr, name) {
    fl <- params(query(solr))$fl[[name]]
    if (!is.null(fl)) {
        expr <- translate(fl, SolrFunctionExpression(), core(solr))
        SolrFunctionPromise(expr, solr)
    }
}

SolrSymbolPromise <- function(expr, context) {
    new("SolrSymbolPromise", expr=expr, context=context)
}

setMethod("Promise",
          c("SolrLuceneSymbol", "Solr"),
          function(expr, context) {
              promise <- callNextMethod()
              if (is(promise, "SolrSymbolPromise"))
                  as(promise, "SolrLuceneSymbolPromise", strict=FALSE)
              else promise
          })

setMethod("Promise",
          c("SolrSymbol", "Solr"),
          function(expr, context) {
              context <- as(context, "SolrFrame", strict=FALSE)
              prom <- computedFieldPromise(context, name(expr))
              if (is.null(prom)) {
                  prom <- SolrSymbolPromise(SolrSymbol(expr), context)
              }
              prom
          })

SolrLucenePromise <- function(expr, context) {
    new("SolrLucenePromise",
        expr=as(expr, "SolrLuceneExpressionOrSymbol", strict=FALSE),
        context=context)
}

SolrFunctionPromise <- function(expr, context) {
    new("SolrFunctionPromise",
        expr=as(expr, "SolrFunctionExpression", strict=FALSE),
        context=context)
}

SolrAggregatePromise <- function(expr, context) {
    new("SolrAggregatePromise",
        expr=as(expr, "SolrAggregateExpression", strict=FALSE),
        context=context)
}

PredicatedSolrSymbolPromise <- function(expr, context) {
    new("PredicatedSolrSymbolPromise",
        expr=as(expr, "PredicatedSolrSymbol", strict=FALSE),
        context=context)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Basic accessors
###

setMethod("length", "SolrPromise", function(x) nrow(context(x)))

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

setMethod("%in%", c("SolrSymbolPromise", "vector"),
          function(x, table) {
              expr <- SolrLuceneTerm(expr(x), table)
              SolrLucenePromise(expr, context(x))
          })

### We are greedy here, which is probably OK, since most logical
### expressions are conveniently expressed with Lucene syntax...
setMethods("Logic",
           list(c("SolrPromise", "SolrLucenePromise"),
                c("SolrLucenePromise", "SolrPromise"),
                c("SolrLucenePromise", "SolrLucenePromise"),
                c("SolrPromise", "SolrSymbolPromise"),
                c("SolrSymbolPromise", "SolrPromise"),
                c("SolrSymbolPromise", "SolrSymbolPromise")),
           function(e1, e2) {
               e1 <- as(e1, "SolrLucenePromise", strict=FALSE)
               e2 <- as(e2, "SolrLucenePromise", strict=FALSE)
               ctx <- resolveContext(e1, e2)
               expr <- if (.Generic == "&") {
                   SolrLuceneAND(expr(e1), expr(e2))
               } else {
                   SolrLuceneOR(expr(e1), expr(e2))
               }
               SolrLucenePromise(expr, ctx)
           })

setMethod("!", "SolrPromise", function(x) {
              x <- as(x, "SolrLucenePromise", strict=FALSE)
              SolrLucenePromise(SolrLuceneNOT(expr(x)), context(x))
          })

setMethod("!", "SolrLuceneSymbolPromise", function(x) {
              SolrLucenePromise(SolrLuceneTerm(expr(x), FALSE), context(x))
          })

setMethod("is.na", "SolrLuceneSymbolPromise", function(x) {
              x != I("*")
          })

setMethods("Compare",
           list(c("SolrSymbolPromise", "vector"),
                c("vector", "SolrSymbolPromise"),
                c("SolrSymbolPromise", "AsIs"),
                c("AsIs", "SolrSymbolPromise"),
                c("SolrSymbolPromise", "numeric"),
                c("numeric", "SolrSymbolPromise")),
           function(e1, e2) {
               luceneCompare(.Generic, e1, e2)
           })

setMethods("Compare",
           list(c("SolrPromise", "numeric"),
                c("numeric", "SolrPromise")),
           function(e1, e2) {
               frangeCompare(.Generic, e1, e2)
           })

setMethod("[", "SolrSymbolPromise", function(x, i, j, ..., drop = TRUE) {
              if (!missing(j) || length(list(...)) > 0L || !missing(drop)) {
                  stop("'[' only accepts x[i] or x[] syntax")
              }
              if (missing(i)) {
                  i <- TRUE
                  ctx <- context(x)
              } else {
                  if (!is(i, "Promise")) {
                      stop("currently, 'i' must be a Promise")
                  }
                  ctx <- resolveContext(x, i)
                  i <- expr(i)
              }
              symbol <- PredicatedSolrSymbol(expr(x), i)
              PredicatedSolrSymbolPromise(symbol, ctx)
          })

setMethod("[", "SolrPromise", function(x, i, j, ..., drop = TRUE) {
              if (!missing(j) || length(list(...)) > 0L || !missing(drop)) {
                  stop("'[' only accepts x[i] or x[] syntax")
              }
              if (missing(i)) {
                  return(x)
              }
              if (is(i, "Promise")) {
                  ctx <- resolveContext(x, i)
              } else {
                  ctx <- context(x)
              }
              context(x) <- ctx[i,]
              x
          })

setMethod("grepl", c("character", "SolrSymbolPromise"),
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
              SolrLucenePromise(SolrLuceneTerm(expr(x), pattern),
                                context(x))
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
          SolrLucenePromise(expr(from), context(from))
      })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Lucene utilities
###

frangeCompare <- function(fun, x, y) {
    promise <- if (is.numeric(x)) y else x
    num <- if (is.numeric(x)) x else y
    crossesZero <- # crossing zero will cover NA
        (substring(fun, 2, 2) == "=" && num == 0) ||
            (substring(fun, 1, 1) == "<" && num > 0) ||
                (substring(fun, 1, 1) == ">" && num < 0)
    if (crossesZero) {
        return(numericCompare(fun, x, y))
    }
    query <- expr(as(promise, "SolrFunctionPromise", strict=FALSE))
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

luceneCompare <- function(fun, x, y) {
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
    SolrLucenePromise(expr, ctx)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Function translation
###

setMethod("Logic",
          c("SolrFunctionPromise", "SolrFunctionPromise"),
           function(e1, e2) {
               solrCall(if (.Generic == "&") "and" else "or", e1, e2)
           })

## Probably rare to encounter a scalar logical value, but it can happen,
## and so we do the obvious simplification here...
setMethod("Logic", c("logical", "SolrPromise"),
          function(e1, e2) {
              callGeneric(e2, e1)
           })

setMethod("Logic", c("SolrPromise", "logical"),
          function(e1, e2) {
              if (length(e2) != 1L) {
                  stop("logical operand must be scalar")
              }
              if (.Generic == "&") if (isTRUE(e2)) e1 else FALSE
              else if (isTRUE(e2)) TRUE else e1
          })

setMethod("!", "SolrFunctionPromise", function(x) {
              solrCall("not", x)
          })

numericCompare <- function(.Generic, e1, e2) {
    `>` <- function(e1, e2) ifelse(ceiling(e1 / e2) - 1L, TRUE, FALSE)
    `==` <- function(e1, e2) !(e1 - e2)
    switch(.Generic,
           ">" = e1 > e2,
           "<" = e2 > e1,
           ">=" = !(e2 > e1),
           "<=" = !(e1 > e2),
           "==" = e1 == e2,
           "!=" = !(e1 == e2))
}

setMethod("Compare", c("SolrPromise", "SolrPromise"),
          function(e1, e2) {
              numericCompare(.Generic, e1, e2)
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
                            ceiling = "ceil",
                            floor = "floor",
                            log = "ln",
                            log10 = "log",
                            acos = "acos",
                            asin = "asin",
                            atan = "atan",
                            exp = "exp",
                            cos = "cos",
                            cosh = "cosh",
                            sin = "sin",
                            sinh = "sinh",
                            tan = "tan",
                            tanh = "tanh")
              if (is.null(fun)) {
                  pi <- SolrFunctionPromise(SolrFunctionCall("pi", list()),
                                            context(x))
                  prom <- switch(.Generic, ## will not be as accurate as native
                                 sign = x / abs(x),
                                 trunc = floor(abs(x)) * sign(x),
                                 log1p = log(x + 1),
                                 acosh = log(x + sqrt(x^2 - 1)),
                                 asinh = log(x + sqrt(x^2 + 1)),
                                 atanh = 0.5 * log((1+x)/(1-x)),
                                 expm1 = exp(x) - 1,
                                 cospi = cos(x * pi),
                                 sinpi = sin(x * pi),
                                 tanpi = tan(x * pi))
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
              if (!identical(digits, 0L)) {
                  stop("'digits' must be 0")
              }
              solrCall("rint", x)
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

## generic in S4Vectors
setMethod("ifelse",
          c("SolrPromise", "ANY", "ANY"),
          function(test, yes, no) {
              solrCall("if", test, yes, no)
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

setMethod("is.na", "SolrFunctionPromise", function(x) {
              !solrCall("exists", x)
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Function coercion
###

setAs("SolrPromise", "SolrFunctionPromise", function(from) {
          SolrFunctionPromise(as(expr(from), "SolrFunctionExpression",
                                 strict=FALSE),
                              context(from))
      })

setAs("PredicatedSolrSymbolPromise", "SolrFunctionPromise", function(from) {
          ctx <- subset(context(from), .(expr(from)@predicate))
          SolrSymbolPromise(expr(x)@subject, ctx)
      })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Function utilities
###

checkArgLengths <- function(args) {
    if (any(lengths(args) != 1L))
        stop("all arguments to a Solr function must have length 1")
}

solrCall <- function(fun, ...) {
    args <- list(...)
    promises <- vapply(args, is, "Promise", FUN.VALUE=logical(1L))
    args[promises] <- lapply(args[promises], function(p) {
                                 expr(as(p, "SolrFunctionPromise",
                                         strict=FALSE))
                             })
    checkArgLengths(args[!promises])
    expr <- SolrFunctionCall(fun, args)
    ctx <- resolveContext(...)
    SolrFunctionPromise(expr, ctx)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Aggregation
###

###
### FIXME: Unhandled: prod(), which is uncommon anyway
###

### The SolrAggregateExpression has two special abilities. First, it
### can pass along additional, supporting statistics to calculate. The
### other is a function that post-processes the result, usually to
### combine the result with the auxillary statistics. It is passed
### itself, its value, as well as the list of auxillary stats.

### By convention, the auxillary stats should be prefixed with
### ".". All stats with "." are removed from the result after
### processing. Adding the aux stats is tricky, since we defer
### translation. After translation, we need to recurse through the
### list, extracting the aux promises and setting them in the
### containing list element. Could be made easier by classing the list
### element that contains stats (the actual "facet" element), so we
### can recurse with rapply() to turn merge the aux.

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
### FIXME: na.rm=TRUE will give wrong answer, avg() counts NAs as 0.
### Could count the existing and scale by count/count(!na)
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
           function (x, ...) standardGeneric("quantile"))

setMethod("quantile", "SolrPromise",
          function (x, probs = seq(0, 1, 0.25), na.rm = FALSE, names = TRUE) {
              if (!isTRUEorFALSE(names)) {
                  stop("'names' must be TRUE or FALSE")
              }
              probs <- probs * 100
              solrAggregate("percentile", x, na.rm, as.list(probs),
                            postprocess = function(percentile, aux) {
                                if (isTRUE(names))
                                    colnames(percentile) <- paste0(probs, "%")
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
              solrAggregate("sum", x*w, na.rm,
                            aux=list(wsum=sum(w,na.rm=na.rm)),
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

setMethod("mad", c("SolrPromise", "ANY"),
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

setMethod("anyNA", "SolrPromise", function(x) {
              any(is.na(x), na.rm=TRUE)
          })

solrAggregate <- function(fun, x, na.rm, params = list(), aux = list(),
                          postprocess = NULL)
{
    if (!isTRUEorFALSE(na.rm)) {
        stop("'na.rm' must be TRUE or FALSE")
    }
    if (!na.rm) {
        aux <- c(aux, .anyNA=anyNA(x))
        postprocess2 <- postprocess
        postprocess <- function(val, aux) {
            postprocess2(ifelse(aux$.anyNA, NA, val), aux)
        }
    }
    x <- as(x, "SolrFunctionPromise", strict=FALSE)
    expr <- SolrAggregateExpression(fun, expr(x), params, aux, postprocess)
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
### Forcing reductions
###

window.SolrPromise <- function(x, start = 1L, end = NA_integer_, ...)
    window(x, start, end)
setMethod("window", "SolrPromise",
          function (x, start = 1L, end = NA_integer_, ...) {
              query(context(x)) <- window(query(context(x)), start, end, ...)
              fulfill(x)
          })

head.SolrPromise <- function(x, n = 6L, ...) head(x, n)
setMethod("head", "SolrPromise", function (x, n = 6L, ...) {
              ## backdoor hack to avoid immediate forcing of entire context
              query(context(x)) <- head(query(context(x)), n, ...)
              fulfill(x)
          })

tail.SolrPromise <- function(x, n = 6L, ...) tail(x, n)
setMethod("tail", "SolrPromise", function (x, n = 6L, ...) {
              query(context(x)) <- tail(query(context(x)), n, ...)
              fulfill(x)
          })

setMethod("unique", "SolrSymbolPromise", function (x, incomparables = FALSE) {
              unique(context(x)[expr(x)@name],
                     incomparables=incomparables)[[1L]]
          })

setMethod("intersect", c("SolrSymbolPromise", "SolrSymbolPromise"),
          function(x, y) {
              unique(x[x %in% y])
          })

setMethod("setdiff", c("SolrSymbolPromise", "SolrSymbolPromise"),
          function(x, y) {
              unique(x[!(x %in% y)])
          })

setMethod("union", c("SolrSymbolPromise", "SolrSymbolPromise"),
          function(x, y) {
### FIXME: two requests, could be one with some work
              unique(c(unique(x), unique(y)))
          })

setMethod("unique", "PredicatedSolrSymbolPromise",
          function (x, incomparables = FALSE) {
              ctx <- subset(context(x), .(expr(x)@predicate))
              unique(SolrSymbolPromise(expr(x)@subject, ctx),
                     incomparables=incomparables)
          })

setMethod("table", "SolrSymbolPromise",
          function (..., exclude = c(NA, NaN)) {
              ctx <- resolveContext(...)
              syms <- lapply(list(...), function(p) expr(p)@name)
              f <- as.formula(paste("~", paste(syms, collapse="+")))
              tab <- xtabs(by, ctx, exclude=exclude)
              if (!is.null(names(syms))) {
                  aliased <- names(syms) != ""
                  names(dimnames(tab))[aliased] <- names(syms)[aliased]
              }
              tab
          })

setGeneric("ftable", function (..., exclude = c(NA, NaN), row.vars = NULL,
                               col.vars = NULL) standardGeneric("ftable"),
           signature="...")

setMethod("ftable", "SolrSymbolPromise",
          function (..., exclude = c(NA, NaN), row.vars = NULL,
                    col.vars = NULL) {
              ftable(table(..., exclude=exclude), row.vars=row.vars,
                     col.vars=col.vars)
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### General utilities
###

resolveContext <- function(...) {
    args <- list(...)
    isProm <- vapply(args, is, "Promise", FUN.VALUE=logical(1L))
    ctx <- Filter(Negate(is.null), lapply(args[isProm], context))
    if (any(!vapply(ctx[-1], compatible, ctx[[1L]], FUN.VALUE=logical(1L)))) {
        stop("cannot combine promises from different contexts")
    }
    ctx[[1L]]
}

