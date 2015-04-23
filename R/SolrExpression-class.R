### =========================================================================
### SolrExpression objects
### -------------------------------------------------------------------------
###
### Implements the Expression translation framework for Solr queries
### and function calls. This happens through S4 dispatch on all
### translatable functions. Note that if we did this for too many
### functions, even our implementation would become lazy. Thus, we
### reserve all low-level R object manipulation functions, including
### things like class(), as(), slot access, etc. Not that it would be
### possible to set methods on those anyway. But we are careful about
### functions like paste() and length(), even though Solr currently
### has no analog.
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


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Expressions
###

setClass("SolrExpression", contains=c("Expression", "VIRTUAL"))

## NOTE: subclasses of this might have to inherit directly from SolrExpression
##       in order to make dispatch unambiguous...
setClass("SolrLuceneExpression", contains=c("SolrExpression", "VIRTUAL"))

setClass("LuceneExpression",
         contains=c("SimpleExpression", "SolrLuceneExpression"))

setClass("SolrFunctionExpression",
         contains=c("SimpleExpression", "SolrExpression"))

setClass("SolrQParserExpression",
         representation(query="Expression"),
         contains="SolrLuceneExpression")

setClass("LuceneQParserExpression",
         representation(op="character",
                        df="characterORNULL",
                        query="SolrLuceneExpression"),
         prototype(op="OR"),
         contains="SolrQParserExpression",
         validity=function(object) {
             c(if (!isSingleString(object@op)) {
                   "'op' parameter must be a single, non-NA string"
               } else if (!object@op %in% c("OR", "AND")) {
                   "'op' must be either OR or AND"
               },
               if (!is.null(object@df) && !isSingleString(object@df)) {
                   "'df' must be a single, non-NA string, or NULL"
               })
         })

setClassUnion("numericORNULL", c("numeric", "NULL"))

setClass("FRangeQParserExpression",
         representation(l="numericORNULL",
                        u="numericORNULL",
                        incl="logical",
                        incu="logical",
                        query="SolrFunctionExpression"),
         prototype(incl=TRUE,
                   incu=TRUE),
         contains="SolrQParserExpression",
         validity=function(object) {
             c(if (!(is.null(object@l) || isSingleNumber(object@l)) ||
                   !(is.null(object@u) || isSingleNumber(object@u))) {
                   "'l' and 'u' each must be a single, non-NA number, or NULL"
               }, if (!isTRUEorFALSE(object@incl) ||
                      !isTRUEorFALSE(object@incu)) {
                   "'incl' and 'incu' must be TRUE or FALSE"
               })
         })

## subset(query, id %in% manu_id[type=="phone"])
##  => {!join from=manu_id to=id}type:phone
setClass("JoinQParserExpression",
         representation(from="character",
                        to="character",
                        query="SolrLuceneExpression"),
         contains="SolrQParserExpression",
         validity=function(object) {
             if (!isSingleString(object@from) || !isSingleString(object@to)) {
                 "'from' and 'to' each must be a single, non-NA string"
             }
         })

setClass("SolrAggregateExpression",
         contains = c("SimpleExpression", "SolrExpression"))

setClassUnion("SolrAggregateArgument",
              c("SolrAggregateSymbol", "SolrFunctionExpression"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Symbols
###

## a symbol that refers to a Solr field
setClass("SolrSymbol", contains=c("Symbol", "SolrExpression"))

## targeting Solr query expressions
setClass("SolrLuceneSymbol", contains="SolrSymbol")
setClassUnion("SolrLuceneExpressionOrSymbol",
              c("SolrLuceneExpression", "SolrLuceneSymbol"))

## targeting plain Solr function expressions, as in sorting and statistics
setClass("SolrFunctionSymbol", contains="SolrSymbol")
setClassUnion("SolrFunctionExpressionOrSymbol",
              c("SolrFunctionExpression", "SolrFunctionSymbol"))

## Result of x[i]
setClass("PredicatedSolrSymbol",
         representation(subject="SolrSymbol",
                        predicate="SolrLuceneExpression"),
         contains="Expression")

setClass("SolrAggregateSymbol", contains = "SolrFunctionSymbol")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

LuceneExpression <- function(x = "") {
    new("LuceneExpression", expr=as.character(x))
}

SolrFunctionExpression <- function(x = "") {
    new("SolrFunctionExpression", expr=as.character(x))
}

SolrQParserExpression <- function() {
    new("SolrQParserExpression")
}

FRangeQParserExpression <- function(query, l = NULL, u = NULL,
                                    incl = TRUE, incu = TRUE)
{
    new("FRangeQParserExpression", query=as(query, "SolrFunctionExpression"),
        l=l, u=u, incl=incl, incu=incu)
}

LuceneQParserExpression <- function(query, op = "OR", df = NULL) {
    new("LuceneQParserExpression", query=as(query, "SolrLuceneExpression"),
        op=op, df=df)
}

JoinQParserExpression <- function(query, from, to) {
    new("JoinQParserExpression", query=as(query, "SolrLuceneExpression"),
        from=as.character(from), to=as.character(to))
}

SolrLuceneSymbol <- function(name) {
    new("SolrLuceneSymbol", name=name)
}

setMethod("Symbol", c("name", "SolrLuceneExpression"),
          function(name, target) {
              SolrLuceneSymbol(name)
          })

SolrFunctionSymbol <- function(name) {
    new("SolrFunctionSymbol", name=name)    
}

setMethod("Symbol", c("name", "SolrFunctionExpression"),
          function(name, target) {
              SolrFunctionSymbol(name)
          })

SolrAggregateSymbol <- function(name) {
    new("SolrAggregateSymbol", name=name)    
}

setMethod("Symbol", c("name", "SolrAggregateExpression"),
          function(name, target) {
              SolrAggregateSymbol(name)
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Serialization
###

setMethod("as.character", "SolrQParserExpression", function(x) {
              params <- slotsAsList(x)
              params$query <- NULL
              params <- Filter(Negate(is.null), params)
              def <- mapply(identical, params,
                            slotsAsList(new(class(x)))[names(params)])
              params <- params[!as.logical(def)]
              logical.params <- vapply(params, is.logical, logical(1))
              params[logical.params] <- tolower(params[logical.params])
              qparser <- qparserFromExpr(x)
              paste0("{!", qparser, if (length(params)) " ",
                     paste(names(params), params, sep="=", collapse=" "), 
                     "}", x@query)
          })

setMethod("as.character", "JoinQParserExpression", function(x) {
              if (length(x@to) == 0L) {
                  stop("incomplete join operation, try syntax: x %in% y[expr]")
              }
              callNextMethod()
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Function overrides
###
### Need to dispatch on variadic pmax/pmin in binary fashion
###

setMethod("overrides", "SolrExpression", function(x) {
              list(pmin=VariadicToBinary(pmin, pmin2),
                   pmax=VariadicToBinary(pmax, pmax2))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Lucene translation
###

setClassUnion("LuceneLiteral",
              c("character", "factor", "numeric", "logical", "POSIXt", "Date"))

setMethod("%in%", c("SolrSymbol", "PredicatedSolrSymbol"),
          function(x, table) {
              JoinQParserExpression(query=table@predicate,
                                    from=table@subject,
                                    to=as.character(x))
          })

setMethod("%in%", c("SolrSymbol", "SolrSymbol"),
          function(x, table) {
              x %in% table[]
          })

setMethod("%in%", c("SolrSymbol", "LuceneLiteral"),
          function(x, table) {
              expr <- wrapParens(paste(normLuceneLiteral(table), collapse=" "))
              LuceneQParserExpression(LuceneExpression(expr),
                                      df=as.character(x))
          })

### Lucene is greedy here, which is probably OK, since most logical
### expressions are conveniently expressed with Lucene syntax...
setMethods("Logic",
           list(c("Expression", "SolrLuceneExpressionOrSymbol"),
                c("SolrLuceneExpressionOrSymbol", "Expression")),
           function(e1, e2) {
               expr <- paste(wrapParens(as(e1, "SolrLuceneExpression")),
                             if (.Generic == "&") "AND" else "",
                             wrapParens(as(e2, "SolrLuceneExpression")))
               LuceneExpression(expr)
           })

setMethod("!", "SolrLuceneExpressionOrSymbol", function(x) {
              if (substring(x, 1L, 1L) == "-") {
                  expr <- substring(as.character(x), 2L)
              } else {
                  expr <- paste0("-", wrapParens(x))
              }
              LuceneExpression(expr)
          })

setMethod("is.na", "SolrLuceneSymbol", function(x) {
              LuceneExpression(paste0("-", as.character(x), ":*.*"))
          })

setMethods("Comparison",
           list(c("SolrSymbol", "LuceneLiteral"),
                c("LuceneLiteral", "SolrSymbol"),
                c("numeric", "SolrSymbol"),
                c("SolrSymbol", "numeric")),
           function(e1, e2) {
               luceneRelational(.Generic, e1, e2)
           })

setMethods("Comparison",
           list(c("SolrExpression", "numeric"),
                c("numeric", "SolrExpression")),
           function(e1, e2) {
               frangeRelational(.Generic, e1, e2)
           })

setMethod("[", "SolrSymbol", function(x, i, j, ..., drop = TRUE) {
              if (!missing(j) || length(list(...)) > 0L || !missing(drop)) {
                  stop("'[' only accepts x[i] or x[] syntax")
              }
              if (missing(i)) {
                  i <- "*.*"
              }
              PredicatedSolrSymbol(subject=x,
                                   predicate=as(i, "SolrLuceneExpression"))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Lucene utilities
###

normLuceneLiteral <- function(x) {
    if (is.factor(x)) {
        x <- as.character(x)
    } else if (is(x, "POSIXt") || is(x, "Date")) {
        x <- toSolr(x, new("solr.DateField"))
    }
    if (is.character(x)) {
        x <- paste0("\"", gsub("\"", "\\\"", x, fixed=TRUE), "\"")
    } else if (is.logical(x)) {
        x <- tolower(as.character(x))
    }
    as.character(x)
}

frangeRelational <- function(fun, x, y) {
    expr <- if (is.numeric(x)) y else x
    num  <- if (is.numeric(x)) x else y
    FRangeQParserExpression(as(expr, "SolrFunctionExpression"),
                            l    = if (fun %in% c(">", ">=", "==")) num,
                            u    = if (fun %in% c("<", "<=", "==")) num,
                            incl = fun %in% c("<=", "=="),
                            incu = fun %in% c(">=", "=="))
}

reverseRelational <- function(call) {
    call$fun <- chartr("<>", "><", call$fun)
    call[c("x", "y")] <- call[c("y", "x")]
    call
}

### NOTE: '==' will *search* text fields, not look for an exact match.
###       String and other fields behave as expected.

luceneRelational <- function(fun, x, y) {
    call <- list(fun=fun, x=x, y=y)
    if (is.name(y)) {
        call <- reverseRelational(call)
    }
    if (fun != "==" && fun != "!=" && !is.numeric(call$y)) {
        stop("non-numeric argument to numeric relational operator")
    }
    call$x <- as.character(call$x)
    call$y <- normLuceneLiteral(call$y)
    expr <- with(call,
                 switch(fun,
                        "==" = paste0(x, ":", y),
                        "!=" = paste0("-", x, ":", y),
                        ">"  = paste0(x, ":", paste0("{", y, " TO *]")),
                        ">=" = paste0(x, ":", paste0("[", y, " TO *]")),
                        "<"  = paste0(x, ":", paste0("[* TO ", y, "}")),
                        "<=" = paste0(x, ":", paste0("[* TO ", y, "]"))))
    LuceneExpression(expr)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Lucene coercion
###

setAs("LuceneExpression", "SolrQParserExpression", function(from) {
          LuceneQParserExpression(from)
      })

setAs("SolrQParserExpression", "LuceneExpression", function(from) {
### NOTE: _query_: prefix not necessary with Solr >= 4.8
          LuceneExpression(paste0("_query_:", normLuceneLiteral(from)))
      })

setAs("SolrFunctionExpression", "SolrLuceneExpression", function(from) {
          FRangeQParserExpression(from, l=1, u=1)
      })

setAs("name", "SolrLuceneExpression", function(from) {
          LuceneExpression(paste0(as.character(from), ":true"))
      })

setAs("name", "SolrQParserExpression", function(from) {
          LuceneQParserExpression(as(from, "SolrLuceneExpression"))
      })


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Function translation
###

setClassUnion("SolrFunctionArg", c("numeric", "logical", "Expression"))

setMethods("Logic",
           list(c("SolrFunctionExpressionOrSymbol",
                  "SolrFunctionExpressionOrSymbol")
                c("SolrFunctionArg", "SolrExpression"),
                c("SolrExpression", "SolrFunctionArg")),
           function(e1, e2) {
               solrCheckArgs(.Generic, e1, e2)
               solrCall(if (.Generic == "&") "and" else "or", e1, e2)
           })

setMethod("!", "SolrFunctionExpressionOrSymbol", function(x) {
              solrCall("not", x)
          })

setMethods("Comparison",
           list(c("Expression", "SolrSymbol"),
                c("SolrSymbol", "Expression"),
                c("SolrSymbol", "SolrSymbol"),
                c("SolrExpression", "SolrExpression")),
           function(e1, e2) {
               switch(.Generic,
                      ">" = solrGreaterThan,
                      "<" = solrLessThan,
                      ">=" = solrGreaterEqualThan,
                      "<=" = solrLessEqualThan,
                      "==" = solrEqual,
                      "!=" = solrNotEqual
                      )(e1, e2)
           })

setMethods("Arith",
           list(c("SolrFunctionArgument", "SolrExpression"),
                c("SolrExpression", "SolrFunctionArgument"),
                c("SolrExpression", "SolrExpression")),
           function(e1, e2) {
               solrCheckArgs(.Generic, e1, e2)
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
                   solrCall(fun, x, y,)
               }
           })

setMethod("-", c("SolrExpression", "missing"), function(e1, e2) {
              e1 * -1L
          })

setMethod("Math", "SolrExpression", function(x) {
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
                  expr <- switch(.Generic, ## will not be as accurate as native
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
                  if (is.null(expr)) {
                      ## cummax, cummin, cumprod, cumsum, log2, *gamma
                      stop(.Generic, "() is not supported by Solr")
                  }
              } else {
                  expr <- solrCall(fun, x)
              }
              expr
          })

setMethod("round", "SolrExpression", function(x, digits = 0L) {
              if (digits != 0L) {
                  stop("'digits' must be 0")
              }
              solrCall("Math.rint", x)
          })

setGeneric("rescale", function(x, ...) standardGeneric("rescale"))

setMethod("rescale", "SolrExpression", function(x, min, max) {
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
          c("SolrExpression", "SolrFunctionArgument", "SolrFunctionArgument"),
          function(test, yes, no) {
              solrCheckArgs("ifelse", yes, no)
              solrCall("if", test, yes, no)
          })

setGeneric("pmax2", function(x, y, na.rm=FALSE) standardGeneric("pmax2"),
           signature=c("x", "y"))

setMethod("pmax2", "ANY", function(x, y, na.rm=FALSE) {
              pmax(x, y, na.rm=na.rm)
          })

setMethods("pmax2",
           list(c("SolrFunctionArgument", "SolrExpression"),
                c("SolrExpression", "SolrFunctionArgument"),
                c("SolrExpression", "SolrExpression")),
           function(x, y, na.rm=FALSE) {
               if (!identical(na.rm, FALSE)) {
                   stop("'na.rm' must be FALSE")
               }
               solrCheckArgs("pmax", x, y)
               solrCall("max", x, y)
           })

setGeneric("pmin2", function(x, y, na.rm=FALSE) standardGeneric("pmin2"),
           signature=c("x", "y"))

setMethod("pmin2", "ANY", function(x, y, na.rm=FALSE) {
              pmin(x, y, na.rm=na.rm)
          })

setMethods("pmin2",
           list(c("SolrFunctionArgument", "SolrExpression"),
                c("SolrExpression", "SolrFunctionArgument"),
                c("SolrExpression", "SolrExpression")),
           function(x, y, na.rm=FALSE) {
               if (!identical(na.rm, FALSE)) {
                   stop("'na.rm' must be FALSE")
               }
               solrCheckArgs("pmin", x, y)
               solrCall("min", x, y)
           })

setMethod("is.na", "SolrFunctionSymbol", function(x) {
              solrCall("not", solrCall("exists", x))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Function utilities
###

solrGreaterThan <- function(x, y) {
    if (!isSolrNumericArg(x) || !isSolrNumericArg(y)) {
        stop("non-numeric argument to relational operator")
    }
    ## crazy construct enables field comparisons using Solr functions
    SolrFunctionExpression(paste0("if(sub(ceil(",
                                  "div(", solrArg(x), ",", solrArg(y), ")",
                                  "),1),1,0)"))
}

solrLessThan <- function(x, y) {
    solrGreaterThan(y, x)
}

solrGreaterEqualThan <- function(x, y) {
    solrCall("not", solrLessThan(x, y))
}

solrLessEqualThan <- function(x, y) {
    solrCall("not", solrGreaterThan(x, y))
}

solrEqual <- function(x, y) {
    solrCall("not", solrNotEqual(x, y))
}

solrNotEqual <- function(x, y) {
    solrCall("abs", solrCall("sub", x, y))
}

solrCheckArgs <- function(fun, ...) {
    args <- list(...)
    exprs <- vapply(args, is, "Expression", FUN.VALUE=logical(1L))
    lens <- lengths(args[exprs])
    if (any(lens != 1L)) {
        argnames <- substitute(list(...))[-1L][exprs]
        stop("some arguments to '", fun, "' have length() != 1: ",
             paste0("'", argnames[lens != 1L], "'", collapse=", "))
    }
}

solrCall <- function(fun, ...) {
    args <- vapply(args, solrArg, character(1L))
    SolrFunctionExpression(paste0(fun, "(", paste(args, collapse=","), ")"))
}

solrArg <- function(x) {
    if (is(x, "Expression")) {
        x <- as(x, "SolrFunctionExpression")
    } else {
        x <- normLuceneLiteral(x)
    }
    as.character(x)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Function coercion
###

setAs("Expression", "SolrFunctionExpression", function(from) {
          ## we assume that the Solr QParser framework is more general
          ## and thus targetable by more languages...
          solrCall("exists", solrCall("query", as(from, "SolrQParserExpression",
                                                  strict=FALSE)))
      })

setAs("name", "SolrFunctionExpression", function(from) {
          SolrFunctionExpression(as.character(from))
      })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Aggregation
###
### We can translate the following functions:
###
### sum => sum [Summary]
### avg => mean
### sumsq ~> var, sd
### min/max => min/max [Summary]
### unique => countUnique? nunique?
### percentile => quantile, median
### min+max => range [Summary] (on Promise and Solr, not SolrExpression)
###

setMethod("Summary", "Expression",
          function (x, ..., na.rm = FALSE) {
              fun <- switch(.Generic,
                            )
              solrCall(fun, as(x, "SolrFunctionExpression"))
          })

setAs("SolrAggregationExpression", "SolrFunctionExpression", function(from) {
          SolrFunctionExpression(fulfill(from))
      })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Query parser utilities
###

targetFromQParser <- function(x) {
    if (!isSingleString(x)) {
        stop("QParser name must be a single, non-NA string")
    }
    cls <- supportedSolrQParsers()[x]
    if (is.na(cls)) {
        stop("QParser '", x, "' is not supported")
    }
    new(cls)
}

qparserFromExpr <- function(x) {
    qparserFromClassName(class(x))
}

qparserFromClassName <- function(x) {
    tolower(sub("QParserExpression$", "", x))
}

supportedSolrQParsers <- function() {
    cls <- signatureClasses(translate, 2)
    ans <- cls[vapply(cls, extends, logical(1), "QParserExpression")]
    setNames(ans, qparserFromClassName(ans))
}
