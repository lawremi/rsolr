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
###  rank(na.last='keep', ties.method='first') and xtfrm() using ord().
###  - Need to somehow convert NA/null to 0, -1 to NA, and add 1 (for rank).
### Reduction:
###   cov() could do sum(x*y - mean(x)*mean(y)) / (n-1L)
###   cor() could do cov(x, y) / (sd(x)*sd(y))
###    - It would be nice if sd() worked first.
###      - This may now work as of Solr 6.6
###   split() would just split the underlying Solr, similar for unlist()
###    - Should support formulas
###   tapply() and by(), via split() and as.table()
###
### Stuff that would be tough to support:
### - diff(): no intrinsic order (sort not withstanding)
### - Matrix functions
### - String functions
### - Join operations (cbind, rbind, merge, append)

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
        expr <- translate(fl, SolrFunctionExpression(), solr)
        SolrFunctionPromise(expr, solr)
    }
}

SolrSymbolPromise <- function(expr, context) {
    missable(expr) <- name(expr) != "score" &&
        !required(fields(schema(core(context)))[name(expr)])
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
        expr=as(expr, "SolrAggregateCall", strict=FALSE),
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

setMethod("lengths", "SolrSymbolPromise", function(x, use.names = TRUE) {
              if (is.null(grouping(context(x)))) {
                  field <- fields(schema(context(x)), name(expr(x)))
                  returnedAsList <- multiValued(field)
                  if (!returnedAsList) {
                      return(rep(1L, length(context(x))))
                  }
              }
              callNextMethod()
          })

setMethod("lengths", "SolrPromise", function(x, use.names = TRUE) {
              if (!is.null(grouping(context(x)))) {
                  ndoc(x)
              } else {
                  lengths(as.list(x))
              }
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Basic coercions
###

setAs("SolrPromise", "Context", function(from) {
   transform(context(from), x = .(expr(from)))["x"]
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Low-level conveniences for dealing with missing values (*eager*)
###

setMethod("missables", "SolrPromise",
          function(x) {
              missables(expr(x))
          })

setMethod("dropMissables", "SolrPromise", function(x, which) {
              expr(x) <- dropMissables(expr(x), which)
              x
          })

setMethod("propagateNAs", "SolrPromise", function(x, na) {
              expr(x) <- propagateNAs(expr(x), na)
              x
          })

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
              predicate <- predicateExpression(query(context(table)),
                                               core(context(table)))
              if (!is.null(predicate)) {
                  symbol <- PredicatedSolrSymbol(expr(table), predicate)
                  table <- PredicatedSolrSymbolPromise(symbol, context(x))
              } else {
                  table <- table[]
              }
              x %in% table
          })

setMethod("%in%", c("SolrSymbolPromise", "vector"),
          function(x, table) {
              sym <- expr(x)
              missable(sym) <- FALSE
              expr <- SolrLuceneTerm(sym, table)
              SolrLucenePromise(expr, context(x))
          })

setMethods("Logic",
           list(c("SolrPromise", "SolrLucenePromise"),
                c("SolrLucenePromise", "SolrPromise"),
                c("SolrLucenePromise", "SolrLucenePromise"),
                c("SolrPromise", "SolrSymbolPromise"),
                c("SolrSymbolPromise", "SolrPromise"),
                c("SolrLucenePromise", "SolrSymbolPromise"),
                c("SolrSymbolPromise", "SolrLucenePromise"),
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
              initialize(x, expr=invert(expr(x)))
          })

setMethod("!", "SolrLuceneSymbolPromise", function(x) {
              SolrLucenePromise(SolrLuceneTerm(expr(x), FALSE), context(x))
          })

setMethod("complete.cases", "SolrLuceneSymbolPromise", function(...) {
              Reduce(`&`, lapply(list(...), `==`, I("*")))
          })

setMethod("is.na", "SolrLuceneSymbolPromise", function(x) {
              x != I("*")
          })


### Comparisons. These are tricky.

### We need to compare fields to a constant, computed values to constant,
### and fields to other fields.
###
### Field compared to constant:
###
### Lucene: field:[1 TO *}, field:1 (for equality)
### -- uses an index, so should be fast for restricted queries
### -- we cannot know when a query is restricted enough,
###    but we should use this when we can (symbols).
###
### Computed value to constant:
###
### FRange: {!frange l=1 incl=true}field
### -- more efficient bulk data access, so faster for large results?
### -- missing values will pass if range crosses zero
###    -- we protect by taking all missables from the function and
###       doing a "AND field:*" in the enclosing query.
### -- encoding != is tricky, approaches:
###    -- not(not(e1-e2)), no frange needed (but happens for queries)
###
### Field compared to field:
###
### This is as simple as expressing '>' as 'e1 - e2 > 0', and so
### becomes a comparison of a computed value to constant.

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

setReplaceMethod("[", c("SolrPromise", "SolrPromise"),
                 function (x, i, j, ..., value) {
                     if (!missing(j) || !missing(...)) {
                         stop("'[<-' does not accept 'j' and '...'")
                     }
                     if (is(value, "Promise") || length(value) == 1L) {
                         ifelse(i, value, x)
                     } else {
                         callNextMethod()
                     }
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
    if (fun == "!=") {
        return(numericCompare(fun, x, y))
    }
    if (is.numeric(x)) {
        call <- reverseRelational(list(fun=fun, x=x, y=y))
        return(do.call(frangeCompare, call))
    }
    promise <- x
    num <- y
    query <- expr(as(promise, "SolrFunctionPromise", strict=FALSE))
    expr <- FRangeQParserExpression(query,
                                    l    = if (fun %in% c(">", ">=", "==")) num,
                                    u    = if (fun %in% c("<", "<=", "==")) num,
                                    incl = fun %in% c(">=", "=="),
                                    incu = fun %in% c("<=", "=="))
    SolrLucenePromise(expr, context(promise))
}

reverseRelationalOp <- function(op) {
    c(">"="<=", ">="="<", "<"=">=", "<="=">", "=="="==")[op]
}

reverseRelational <- function(call) {
    call$fun <- reverseRelationalOp(call$fun)
    call[c("x", "y")] <- call[c("y", "x")]
    call
}

### NOTE: '==' will *search* text fields, not look for an exact match.
###       String and other fields behave as expected.

luceneCompareExpr <- function(fun, x, y) {
    switch(fun,
           "==" = SolrLuceneTerm(expr(x), y),
           ">"  = SolrLuceneRangeTerm(expr(x), y, I("*"), FALSE, TRUE),
           ">=" = SolrLuceneRangeTerm(expr(x), y, I("*"), TRUE, TRUE),
           "<"  = SolrLuceneRangeTerm(expr(x), I("*"), y, TRUE, FALSE),
           "<=" = SolrLuceneRangeTerm(expr(x), I("*"), y, TRUE, TRUE))
}

luceneCompare <- function(fun, x, y) {
    if (fun == "!=") {
        return(!(x == y))
    }

    if (is(y, "SolrSymbolPromise")) {
        call <- reverseRelational(list(fun=fun, x=x, y=y))
        return(do.call(luceneCompare, call))
    }

    if (isNA(y)) {
        expr <- solrQueryNA
    } else {
        expr <- luceneCompareExpr(fun, x, y)
    }
    
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
              if (is.na(e2)) {
                  if (.Generic == "&") ifelse(e1, solrNA, FALSE) 
                  else ifelse(e1, TRUE, solrNA)
              } else {
                  if (.Generic == "&") if (isTRUE(e2)) e1 else FALSE
                  else if (isTRUE(e2)) TRUE else e1
              }
          })

setMethod("!", "SolrFunctionPromise", function(x) {
              SolrFunctionPromise(invert(expr(x)), context(x))
          })

## TODO: at some point use gt(), lt(), etc functions new in Solr 6.2.0

numericCompare <- function(.Generic, e1, e2) {
    `>` <- function(e1, e2) (e2 - e1) < 0L
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
                c("SolrPromise", "SolrPromise"),
                c("logical", "SolrPromise"),
                c("SolrPromise", "logical")),
           function(e1, e2) {
               if (isNA(e1) || isNA(e2)) {
                   return(NA_real_)
               }
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

map <- function(x, min, max, target, value) {
    solrCall("map", x, min, max, target, value)
}

### FIXME: We are forcing on cummax, cummin, cumprod, cumsum, log2, *gamma.
###        The lack of 'gamma' means we also force on factorial().

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
                                 sign = map(x, 0, I("Infinity"), 1, -1),
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

setMethod("signif", "SolrPromise", function(x, digits = 6L) {
              round(x, digits - floor(log10(x)+1L))
          })

setMethod("round", "SolrPromise", function(x, digits = 0L) {
              solrCall("rint", x * 10^digits) / 10^digits
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
### FIXME: we could check for a constant TRUE/FALSE in 'test' and skip the check
              solrCall("if", test, yes, no)
          })

pairwiseNARm <- function(ans, x, y) {
    missables(expr(ans)) <- character()
    xm <- missables(x)
    ym <- missables(y)
    if (length(xm) > 0L && length(ym) > 0L) {
        ifelse(exists(x), ifelse(exists(y), ans, dropMissables(x, xm)),
               ifelse(exists(y), dropMissables(y, ym),
                      SolrFunctionPromise(solrNA, context(ans))))
    } else {
        if (length(xm) > 0L) {
            ifelse(exists(x), ans, y)
        } else {
            ifelse(exists(y), ans, x)
        }
    }
}

setMethods("pmax2",
           list(c("numeric", "SolrPromise"),
                c("SolrPromise", "numeric"),
                c("SolrPromise", "SolrPromise")),
           function(x, y, na.rm=FALSE) {
               if (!isTRUEorFALSE(na.rm)) {
                   stop("'na.rm' must be TRUE or FALSE")
               }
               if (na.rm) {
                   if (isNA(x)) return(y)
                   if (isNA(y)) return(x)
               } else {
                   if (isNA(x) || isNA(y)) return(NA_real_)
               }
               max <- solrCall("max", x, y)
               if (na.rm) {
                   max <- pairwiseNARm(max, x, y)
               }
               max
           })

setMethods("pmin2",
           list(c("numeric", "SolrPromise"),
                c("SolrPromise", "numeric"),
                c("SolrPromise", "SolrPromise")),
           function(x, y, na.rm=FALSE) {
               if (!isTRUEorFALSE(na.rm)) {
                   stop("'na.rm' must be TRUE or FALSE")
               }
               if (na.rm) {
                   if (isNA(x)) return(y)
                   if (isNA(y)) return(x)
               } else {
                   if (isNA(x) || isNA(y)) return(NA_real_)
               }
               min <- solrCall("min", x, y)
               if (na.rm) {
                   min <- pairwiseNARm(min, x, y)
               }
               min
           })

exists <- function(x) {
    missables <- missables(x)
    if (length(missables) == 0L) {
        expr <- TRUE
    } else {
        expr <- noneMissing(missables)
    }
    SolrFunctionPromise(expr, context(x))
}

setMethod("complete.cases", "SolrFunctionPromise", function(...) {
    Reduce(`&`, lapply(list(...), exists))
})

setMethod("is.na", "SolrFunctionPromise", function(x) {
              !exists(x)
          })

cut.SolrPromise <-
    function(x, breaks, include.lowest = FALSE, right = TRUE, ...)
        cut(x, breaks, include.lowest, right)

setMethod("cut", "SolrPromise",
          function(x, breaks, include.lowest = FALSE, right = TRUE) {
              gap <- unique(diff(breaks))
              if (length(gap) != 1L) {
                  stop("only uniform breaks are supported")
              }
              if (!isTRUEorFALSE(include.lowest)) {
                  stop("'include.lowest' must be TRUE or FALSE")
              }
              if (!isTRUEorFALSE(right)) {
                  stop("'right' must be TRUE or FALSE")
              }
              scaled <- (x - breaks[1L]) / gap
              if (right) {
                  bins <- ceiling(scaled)
                  if (include.lowest) {
                      bins <- ifelse(bins == 0L, 1L, bins)
                  }
              } else {
                  bins <- floor(scaled) + 1L
                  if (include.lowest) {
                      bins <- ifelse(bins == length(breaks) + 1L,
                                     length(breaks), bins)
                  }
              }
              lowest.fun <- if (include.lowest || !right) `<` else `<=`
              highest.fun <- if (include.lowest || right) `>` else `>=`
              na <- SolrFunctionPromise(solrNA, context(x))
              ifelse(lowest.fun(x, breaks[1L]), na,
                     ifelse(highest.fun(x, tail(breaks, 1L)), na, bins))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Function coercion
###

setAs("SolrPromise", "SolrFunctionPromise", function(from) {
          SolrFunctionPromise(as(expr(from), "SolrFunctionExpression",
                                 strict=FALSE),
                              context(from))
      })

setMethod("fulfill", "PredicatedSolrSymbolPromise", function(x) {
              fulfill(as(x, "SolrFunctionPromise"))
          })

setAs("PredicatedSolrSymbolPromise", "SolrFunctionPromise", function(from) {
          ctx <- subset(context(from), .(expr(from)@predicate))
          SolrSymbolPromise(expr(from)@subject, ctx)
      })

as.data.frame.SolrPromise <- function(x, row.names = NULL, optional = FALSE,
                                      ...,
                                      nm = paste(deparse(substitute(x),
                                          width.cutoff = 500L),
                                          collapse = " "))
{
    as.data.frame(x, row.names=row.names, optional=optional, ..., nm=nm)
}

## mostly to ensure that multi-valued fields are not mistreated
setMethod("as.data.frame", "SolrPromise",
          function(x, row.names = NULL, optional = FALSE, ...,
                   nm = paste(deparse(substitute(x), width.cutoff = 500L),
                       collapse = " "))
              {
                  as.data.frame.vector(fulfill(x), row.names=row.names,
                                       optional=optional, nm=nm, ...)
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
    ctx <- resolveContext(...)
    expr <- SolrFunctionCall(fun, args)
    SolrFunctionPromise(expr, ctx)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Aggregation
###

### The SolrAggregateCall has two special abilities. First, it
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

setGeneric("hiddenName", function(x) standardGeneric("hiddenName"))

setMethod("hiddenName", "ANY", function(x) {
              paste0(".", as.character(x))
          })

### FIXME: forcing hidden names to be distinct like this prevents
### errors, but can give Solr redundant work, at least when range()
### and weighted.mean() are involved. In theory, this would be fixable
### by replacing these names with non-recursive ones when serializing
### the query (and then matching back). Just not worth the effort now.

setMethod("hiddenName", "ParentSolrAggregateCall", function(x) {
              paste0(callNextMethod(), hiddenName(child(x)))
          })

setMethod("Summary", "SolrPromise",
          function (x, ..., na.rm = FALSE) {
              if (.Generic == "prod") {
                  stop("'prod(x)' not yet supported")
              }
              if (!isTRUEorFALSE(na.rm)) {
                  stop("'na.rm' must be TRUE or FALSE")
              }
              fun <- switch(.Generic,
                            max="max",
                            min="min",
                            range="max",
                            sum="sum",
                            any="sum",
                            all="sum")
              child <- NULL
              postprocess <- NULL
              if (.Generic == "range") {
                  child <- min(x, na.rm=TRUE)
                  postprocess_range <- function(max, min) {
                      min[is.na(max)] <- NA
                      cbind(min, max)
                  }
                  postprocess <- postprocess_range
              } else if (.Generic == "any") {
                  if (!na.rm) {
                      na.rm <- TRUE
                      child <- anyNA(x)
                      postprocess_any_na <- function(sum, anyNA) {
                          ifelse(sum > 0L, TRUE, ifelse(anyNA, NA, FALSE))
                      }
                      postprocess <- postprocess_any_na
                  } else {
                      postprocess_any <- function(sum, count) sum > 0
                      postprocess <- postprocess_any
                  }
              } else if (.Generic == "all") {
                  .na.rm <- na.rm
                  na.rm <- TRUE
                  child <- if (length(missables(x)) > 0L) sum(exists(x))
                  postprocess_all <- function(sum, countExists) {
                      count <- attr(countExists, "child")
                      ifelse(sum < countExists, FALSE,
                             if(.na.rm) TRUE
                             else ifelse(count > countExists, NA, TRUE))
                  }
                  postprocess <- postprocess_all
              }
              prom <- solrAggregate(fun, x, na.rm, child=child,
                                    postprocess=postprocess)
              prom
          })

setMethod("mean", "SolrPromise", function(x, trim = 0, na.rm=FALSE) {
              if (!missing(trim)) {
                  warning("'trim' is not yet? supported")
              }
              if (!isTRUEorFALSE(na.rm)) {
                  stop("'na.rm' must be TRUE or FALSE")
              }
              if (na.rm) {
### FIXME: can exists() be made smarter?
                  child <- if (length(missables(x)) > 0L) sum(exists(x))
                  solrAggregate("sum", x, na.rm=TRUE, child=child,
                                postprocess=function(sum, count) {
                                    sum / count
                                })
              } else {
                  solrAggregate("avg", x, na.rm=FALSE)
              }
          })

ppVar <- function(sumsq, count) sumsq / (count - 1L)

setMethod("var", "SolrPromise", function(x, na.rm=FALSE) {
### FIXME: Solr calculates mean internally, cannot handle na.rm=TRUE properly
              if (!identical(na.rm, FALSE)) {
                  stop("'na.rm' must be FALSE")
              }
              if (version(core(context(x))) < "6.6.0")
                  stop("'var' requires Solr version >= 6.6.0")
              solrAggregate("variance", x, na.rm=FALSE)
          })

setMethod("sd", "SolrPromise", function(x, na.rm=FALSE) {
              if (!identical(na.rm, FALSE)) {
                  stop("'na.rm' must be FALSE")
              }
              if (version(core(context(x))) < "6.6.0")
                  stop("'sd' requires Solr version >= 6.6.0")
              solrAggregate("stdev", x, na.rm=FALSE)
          })

setGeneric("nunique", function(x, ...) standardGeneric("nunique"))
setMethod("nunique", "ANY", function(x, na.rm = FALSE) {
              if (na.rm) {
                  length(setdiff(x, NA))
              } else {
                  length(unique(x))
              }
          })
setMethod("nunique", "factor", function(x, na.rm = FALSE) {
              nlevels(x) + if (na.rm) 0L else anyNA(x)
          })

## for lists (multivalued fields) this is effectively called on unlist(x)
setMethod("nunique", "SolrPromise",
          function(x, na.rm=FALSE, approximate=FALSE) {
              if (!isTRUEorFALSE(na.rm)) {
                  stop("'na.rm' must be TRUE or FALSE")
              }
              if (!isTRUEorFALSE(approximate)) {
                  stop("'approximate' must be TRUE or FALSE")
              }
              postprocess <- function(len, anyNA) {
                  if (!na.rm) {
                      len <- len + anyNA
                  }
                  as.integer(len)
              }
              if (!na.rm) {
                  child <- anyNA(x)
              } else {
                  child <- NULL
              }
              solrAggregate(if (approximate) "hll" else "unique", x, na.rm=TRUE,
                            child=child, postprocess=postprocess)
          })

quantile.SolrPromise <- function(x, probs = seq(0, 1, 0.25), na.rm = FALSE, ...)
{
              probs <- probs * 100
              solrAggregate("percentile", x, na.rm, as.list(probs),
                            postprocess = function(percentile, count) {
                                percentile <- as.matrix(percentile)
                                colnames(percentile) <- paste0(probs, "%")
                                percentile
                            })
}

median.SolrPromise <- function(x, na.rm=FALSE, ...) median(x, na.rm=na.rm)

setMethod("median", "SolrPromise", function(x, na.rm=FALSE) {
              solrAggregate("percentile", x, na.rm, list(50))
          })

weighted.mean.SolrPromise <- function(x, w, na.rm=FALSE, ...)
    weighted.mean(x, w, na.rm=na.rm)

setMethod("weighted.mean", c("SolrPromise", "SolrPromise"),
          function(x, w, na.rm=FALSE) {
              if (!isTRUEorFALSE(na.rm)) {
                  stop("'na.rm' must be TRUE or FALSE")
              }
              if (na.rm && length(missables(x)) > 0L) {
                  w <- w * exists(x)
                  x <- propagateNAs(x)
              }
              wsum <- sum(w)
              if (na.rm) {
                  w <- propagateNAs(w)
              }
              solrAggregate("sum", x*w, na.rm,
                            child=wsum,
                            postprocess=function(sum, wsum) {
                                sum /  wsum
                            })
          })

setMethod("IQR", "SolrPromise",
          function(x, na.rm=FALSE, type = 7) {
              if (!missing(type)) {
                  warning("argument 'type' is ignored")
              }
              solrAggregate("percentile", x, na.rm, list(25, 75),
                            postprocess=function(percentile, count) {
                                percentile[,2] - percentile[,1]
                            })
          })

setMethod("mad", "SolrPromise",
          function (x, center = median(x, na.rm=na.rm), constant = 1.4826,
                    na.rm = FALSE, low = FALSE, high = FALSE) {
              if (!identical(low, FALSE) || !identical(high, FALSE)) {
                  stop("'high' and 'low' must be FALSE")
              }
              ## FIXME: computing center requires an extra round trip...
              ## ... and breaks this for grouped data.
              med <- median(abs(x - as.numeric(center)), na.rm=na.rm)
              if (is.vector(med) && is.na(med)) {
                  stop("median evaluated to NA, try na.rm=TRUE (for now)")
              }
              expr(med)@postprocess <- function(median, count) {
                  median * constant
              }
              med
          })

setMethod("anyNA", "SolrPromise", function(x) {
              any(is.na(x), na.rm=TRUE)
          })

setMethod("ndoc", "SolrPromise", function(x) {
### In a package full of hacks, this is one of the best
              call <- SolrAggregateCall("sum", 1L,
                                        postprocess=function(stat, count)
                                            as.integer(stat))
              SolrAggregatePromise(call, context(x))
          })

solrAggregate <- function(fun, x, na.rm, params = list(), child = NULL,
                          postprocess = NULL)
{
    if (!isTRUEorFALSE(na.rm)) {
        stop("'na.rm' must be TRUE or FALSE")
    }
    if (!is.null(child)) {
        child <- expr(child)
    }
    x <- as(x, "SolrFunctionPromise", strict=FALSE)
    missables <- missables(x)
    if (!na.rm && length(missables) > 0L) {
        child <- `child<-`(expr(anyNA(x)), child)
        postprocess2 <- postprocess
        postprocess <- function(val, anyNA) {
            if (is.matrix(val)) {
                val[anyNA,] <- NA
            } else {
                val[anyNA] <- NA
            }
            if (!is.null(postprocess2)) {
                val <- postprocess2(val, attr(anyNA, "child"))
            }
            val
        }
        x <- dropMissables(x, missables)
    }
    expr <- SolrAggregateCall(fun, expr(x), params, child, postprocess)
    SolrAggregatePromise(expr, context(x))
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

setGeneric("windows", function(x, ...) standardGeneric("windows"))

setMethod("windows", "SolrPromise",
          function(x, start = 1L, end = .Machine$integer.max) {
              if (is.null(grouping(context(x)))) {
                  stop("context(x) is not grouped")
              }
              ctx <- as(x, "Context")
              windows(ctx, start=start, end=end)$x
          })

unique.SolrPromise <- function(x, incomparables = FALSE, ...) {
    unique(x, incomparables=incomparables)
}

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

summary.SolrPromise <- function(object, ...) {
    summary(object, ...)
}

setMethod("summary", "SolrPromise", function(object, maxsum = 100L, ...) {
    as.table(summary(as(object, "Context"), maxsum=maxsum, ...), drop=TRUE)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Lazy evaluation of aggregate manipulation
###

aggPostCall <- function(name, agg, ..., first = TRUE) {
    pp <- expr(agg)@postprocess
    if (is.null(pp)) {
        pp <- function(x, len) x 
    }
    if (first) {
        args <- list(body(pp), ...)
    } else {
        args <- list(..., body(pp))
    }
    body(pp) <- as.call(c(as.name(name), args))
    expr(agg)@postprocess <- pp
    agg
}

setMethod("Logic", c("logical", "SolrAggregatePromise"), function(e1, e2) {
              aggPostCall(.Generic, e2, e1)
          })

setMethod("Logic", c("SolrAggregatePromise", "logical"), function(e1, e2) {
              aggPostCall(.Generic, e1, e2)
          })

setMethod("Logic", c("SolrAggregatePromise", "SolrAggregatePromise"),
          function(e1, e2) {
              e1 <- as.logical(e1)
              callGeneric()
          })

setMethod("!", "SolrAggregatePromise", function(x) {
              aggPostCall("!", x)
          })

setMethod("is.na", "SolrAggregatePromise", function(x) {
              aggPostCall("is.na", x)
          })

setMethod("Compare", c("numeric", "SolrAggregatePromise"), function(e1, e2) {
              aggPostCall(.Generic, e2, e1, first=FALSE)
          })

setMethod("Compare", c("SolrAggregatePromise", "numeric"), function(e1, e2) {
              aggPostCall(.Generic, e1, e2)
          })

setMethod("Compare", c("SolrAggregatePromise", "SolrAggregatePromise"),
          function(e1, e2) {
              e1 <- as.numeric(e1)
              callGeneric()
          })

setMethod("Arith", c("numeric", "SolrAggregatePromise"), function(e1, e2) {
              aggPostCall(.Generic, e2, e1, first=FALSE)
          })

setMethod("Arith", c("SolrAggregatePromise", "numeric"), function(e1, e2) {
              aggPostCall(.Generic, e1, e2)
          })

setMethod("Arith", c("SolrAggregatePromise", "SolrAggregatePromise"),
          function(e1, e2) {
              e1 <- as.numeric(e1)
              callGeneric()
          })

setMethod("Math", "SolrAggregatePromise", function(x) {
              aggPostCall(.Generic, x)
          })

setMethod("round", "SolrAggregatePromise", function(x, digits = 0L) {
              aggPostCall("round", x, digits)
          })

setMethod("ifelse",
          c("SolrAggregatePromise", "ANY", "ANY"),
          function(test, yes, no) {
              aggPostCall("ifelse", test, yes, no)
          })

setMethod("pmax2", c("numeric", "SolrAggregatePromise"),
          function(x, y, na.rm=FALSE) {
              aggPostCall("pmax2", y, x, na.rm)
          })

setMethod("pmax2", c("SolrAggregatePromise", "numeric"),
          function(x, y, na.rm=FALSE) {
              aggPostCall("pmax2", x, y, na.rm)
          })

setMethod("pmax2", c("SolrAggregatePromise", "SolrAggregatePromise"),
          function(x, y, na.rm=FALSE) {
              x <- as.numeric(x)
              callGeneric()
          })

setMethod("pmin2", c("numeric", "SolrAggregatePromise"),
          function(x, y, na.rm=FALSE) {
              aggPostCall("pmax2", y, x, na.rm)
          })

setMethod("pmin2", c("SolrAggregatePromise", "numeric"),
          function(x, y, na.rm=FALSE) {
              aggPostCall("pmax2", x, y, na.rm)
          })

setMethod("pmin2", c("SolrAggregatePromise", "SolrAggregatePromise"),
          function(x, y, na.rm=FALSE) {
              x <- as.numeric(x)
              callGeneric()
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

