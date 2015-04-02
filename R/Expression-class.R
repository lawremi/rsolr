### =========================================================================
### Expression objects
### -------------------------------------------------------------------------
###
### Represents a target of the translate() generic.
###

setClass("Expression")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Expressions
###

setClass("SimpleExpression",
         representation(expr="character"),
         prototype(expr=""),
         contains="Expression",
         validity=function(object) {
           if (!isSingleString(object@expr)) {
             stop("'expr' must be a single, non-NA string")
           }
         })

setClass("LuceneExpression", contains="SimpleExpression")

setClass("SolrFunctionExpression", contains="SimpleExpression")

setClass("SolrQParserExpression",
         representation(query="Expression"),
         contains="Expression")

setClassUnion("SolrLuceneExpression",
              c("LuceneExpression", "SolrQParserExpression"))
setIs("SolrLuceneExpression", "Expression")

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
             }, if (!isTRUEorFALSE(object@incl) || !isTRUEorFALSE(object@incu)) {
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
           if (!isSingleString(object@from) ||
               (length(object@to) > 0L && !isSingleString(object@to))) {
             "'from' and 'to' each must be a single, non-NA string"
           }
         })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

LuceneExpression <- function(x = "") {
  new("LuceneExpression", expr=x)
}

SolrFunctionExpression <- function(x = "") {
  new("SolrFunctionExpression", expr=x)
}

SolrQParserExpression <- function() {
  new("SolrQParserExpression")
}

FRangeQParserExpression <- function(query, l = NULL, u = NULL,
                                    incl = TRUE, incu = TRUE)
{
  new("FRangeQParserExpression", query=query, l=l, u=u, incl=incl, incu=incu)
}

LuceneQParserExpression <- function(query, op = "OR", df = NULL) {
  new("LuceneQParserExpression", query=query, op=op, df=df)
}

JoinQParserExpression <- function(query, from) {
  new("JoinQParserExpression", query=query, from=from)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

setMethod("as.character", "SimpleExpression", function(x) {
  x@expr
})

setMethod("as.character", "SolrQParserExpression", function(x) {
  params <- slotsAsList(x)
  params$query <- NULL
  params <- Filter(Negate(is.null), params)
  def <- mapply(identical, params, slotsAsList(new(class(x)))[names(params)])
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
    stop("incomplete join operation, valid syntax: x %in% y[expr]")
  }
  callNextMethod()
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Translation
###

setGeneric("translate", function(x, target, ...) standardGeneric("translate"))

setMethod("translate", c("language", "Expression"),
          function(x, target, ...) {
            as(eval(x, translationContext(target, x, ...)), class(target),
               strict=FALSE)
          })

setGeneric("translationContext",
           function(x, ...) standardGeneric("translationContext"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Lucene translation
###

## NOTE: '==' will *search* text fields, not look for an exact match.
##       String and other fields behave as expected.

### TODO: support %over% for IRanges/GRanges/etc
### pos %over% IRanges(1,10) => pos:[1 TO 10]
### pos %over% GRanges("1", IRanges(1,10)) => pos:[1,1 TO 10,1]
### pos %over% circle(cx,cy,d) => {!frange l=0 u=d}geodist(pos, cx, cy)

luceneEscape <- function(x) {
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
  x
}

isLuceneLiteral <- function(x) {
  is.character(x) || is.numeric(x) || is.logical(x) ||
    is(x, "POSIXt") || is(x, "Date")
}

luceneIn <- function(x, y) {
  if (is(y, "JoinQParserExpression")) {
    if (!is.name(x)) {
      stop("'x' must be a field name")
    }
    initialize(y, to=as.character(x))
  } else {
    y <- luceneEscape(y)
    if (!is.name(x) || !isLuceneLiteral(y)) {
      stop("valid %in% syntax: field %in% c('foo', 'bar')")
    }
    expr <- paste0("(", paste(y, collapse=" "), ")")
    LuceneQParserExpression(LuceneExpression(expr), df=as.character(x))
  }
}

luceneTerm <- function(x) {
  if (is(x, "Expression") || is.name(x)) {
    as(x, "SolrLuceneExpression")
  } else {
    x
  }
}

luceneParen <- function(x) {
  LuceneExpression(paste0("(", luceneTerm(x), ")"))
}

luceneLogical <- function(op) {
  function(x, y) {
    LuceneExpression(paste0(luceneTerm(x), op, luceneTerm(y)))
  }
}

luceneNot <- function(x) {
  LuceneExpression(paste0("-", luceneTerm(x)))
}

luceneBracket <- function (x, i, j, ..., drop = TRUE) {
  if (!missing(j) || length(list(...)) > 0L || !missing(drop)) {
    stop("'[' only accepts x[i] syntax")
  }
  if (!is.name(x)) {
    stop("'x' must be a field name")
  }
  JoinQParserExpression(as(i, "SolrLuceneExpression"), from=x)
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

luceneRelational <- function(fun, x, y) {
  call <- list(fun=fun, x=x, y=y)
  if (is.name(y)) {
    call <- reverseRelational(call)
  }
  if (fun != "==" && fun != "!=" && !is.numeric(call$y)) {
    stop("non-numeric argument to numeric relational operator")
  }
  call$y <- luceneEscape(call$y)
  LuceneExpression(with(call,
                        switch(fun,
                               "==" = paste0(x, ":", y),
                               "!=" = paste0("-", x, ":", y),
                               ">"  = paste0(x, ":", paste0("{", y, " TO *]")),
                               ">=" = paste0(x, ":", paste0("[", y, " TO *]")),
                               "<"  = paste0(x, ":", paste0("[* TO ", y, "}")),
                               "<=" = paste0(x, ":", paste0("[* TO ", y, "]")))))
}

metaRelational <- function(fun, funEnv) {
  function (x, y) {
    canUseLucene <- (is.name(x) && isLuceneLiteral(y)) ||
                    (isLuceneLiteral(x) && is.name(y))
    if (canUseLucene) {
      luceneRelational(fun, x, y)
    } else {
      canUseFRange <- (is.numeric(x) && is(y, "Expression")) ||
                      (is.numeric(y) && is(x, "Expression"))
      if (canUseFRange) {
        frangeRelational(fun, x, y)
      } else {
        canUseFunctions <- (is.name(x) || is(x, "Expression")) &&
                           (is.name(y) || is(y, "Expression"))
        if (canUseFunctions) {
          as(eval(call(fun, x, y), funEnv), "SolrLuceneExpression")
        } else {
          stop("unsupported relational operation")
        }
      }
    }
  }
}

SolrLuceneContext <- function(expr, env = emptyenv()) {
  funEnv <- SolrFunctionContext(expr, env)
  list2env(list(
    "=="   = metaRelational("==", funEnv),
    "!="   = metaRelational("!=", funEnv),
    ">"    = metaRelational(">", funEnv),
    ">="   = metaRelational(">=", funEnv),
    "<"    = metaRelational("<", funEnv),
    "<="   = metaRelational("<-", funEnv),
    "%in%" = luceneIn,
    "("    = luceneParen,
    "|"    = luceneLogical(" "),
    "&"    = luceneLogical(" AND "),
    "!"    = luceneNot,
    "["    = luceneBracket
    ), parent=funEnv)
}

setMethod("translationContext", "SolrLuceneExpression",
          function(x, ...) SolrLuceneContext(...))

setMethod("translationContext", "SolrQParserExpression",
          function(x, ...) SolrLuceneContext(...))

setAs("LuceneExpression", "SolrQParserExpression", function(from) {
  LuceneQParserExpression(from)
})

setAs("SolrQParserExpression", "SolrLuceneExpression", function(from) {
### NOTE: _query_: prefix not necessary with Solr >= 4.8
  LuceneExpression(paste0("_query_:", luceneEscape(from)))
})

setAs("SolrFunctionExpression", "SolrLuceneExpression", function(from) {
  FRangeQParserExpression(from, l=1, u=1)
})

setAs("name", "SolrLuceneExpression", function(from) {
  LuceneExpression(paste0(from, ":true"))
})

setAs("name", "SolrQParserExpression", function(from) {
  LuceneQParserExpression(as(from, "SolrLuceneExpression"))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Function translation
###

solrEscape <- luceneEscape

isSolrNumericArg <- function(x) {
  is.numeric(x) || is.logical(x) || is.name(x) || is(x, "Expression")
}

solrGreaterThan <- function(x, y) {
  if (!isSolrNumericArg(x) || !isSolrNumericArg(y)) {
    stop("non-numeric argument to relational operator")
  }
  SolrFunctionExpression(paste0("if(sub(ceil(",
                                "div(", solrArg(x), ",", solrArg(y), ")",
                                "),1),1,0)"))
}

solrLessThan <- function(x, y) {
  solrGreaterThan(y, x)
}

solrGreaterEqualThan <- function(x, y) {
  SolrFunctionExpression(paste0("not(", solrLessThan(x, y), ")"))
}

solrLessEqualThan <- function(x, y) {
  SolrFunctionExpression(paste0("not(", solrGreaterThan(x, y), ")"))
}

solrEqual <- function(x, y) {
  SolrFunctionExpression(paste0("not(abs(", solrArg(x), "-", solrArg(y), "))"))
}

solrPlus <- function(x, y) {
  if (!isSolrNumericArg(x) || !(missing(y) || isSolrNumericArg(y))) {
    stop("non-numeric argument to arithmetic operation")
  }
  if (!missing(y)) {
    SolrFunctionExpression(paste0("sum(", solrArg(x), ",", solrArg(y), ")"))
  } else {
    x
  }
}

solrMinus <- function(x, y) {
  if (!isSolrNumericArg(x) || !(missing(y) || isSolrNumericArg(y))) {
    stop("non-numeric argument to arithmetic operation")
  }
  if (!missing(y)) {
    SolrFunctionExpression(paste0("sub(", solrArg(x), ",", solrArg(y), ")"))
  } else {
    SolrFunctionExpression(paste0("-", solrArg(x)))
  }
}

solrExists <- function(x) {
  if (!is.name(x)) {
    stop("is.na(x): 'x' must be a field name")
  }
  SolrFunctionExpression(paste0("not(exists(", solrArg(x), "))"))
}

solrCall <- function(fun, min.arity=1L, max.arity=min.arity) {
  function(...) {
    if (nargs() < min.arity | nargs() > max.arity) {
      stop("incorrect number of arguments to '", fun, "'")
    }
    args <- list(...)
    if (!all(vapply(args, isSolrNumericArg, logical(1)))) {
      stop("non-numeric argument to numeric function")
    }
    args <- vapply(args, solrArg, character(1))
    SolrFunctionExpression(paste0(fun, "(", paste(args, collapse=","), ")"))
  }
}

SolrFunctionContext <- function(expr, env) {
  varsEnv <- VarsEnv(expr, env)
  list2env(list(
    '+' = solrPlus,
    '-' = solrMinus,
    '*' = solrCall("product", 2),
    '/' = solrCall("div", 2),
    '%%' = solrCall("mod", 2),
    '^' = solrCall("pow", 2),
    abs = solrCall("abs"),
    log10 = solrCall("log"),
    sqrt = solrCall("sqrt"),
    rescale = solrCall("scale", 3),
    pmax = solrCall("max", 2),
    pmin = solrCall("min", 2),
    log = solrCall("Math.ln"),
    exp = solrCall("Math.exp"),
    sin = solrCall("Math.sin"),
    cos = solrCall("Math.cos"),
    tan = solrCall("Math.tan"),
    asin = solrCall("Math.asin"),
    acos = solrCall("Math.acos"),
    atan = solrCall("Math.atan"),
    sinh = solrCall("Math.sinh"),
    cosh = solrCall("Math.cosh"),
    tanh = solrCall("Math.tanh"),
    ceiling = solrCall("Math.ceil"),
    floor = solrCall("Math.floor"),
    round = solrCall("Math.rint"),
    is.na = solrExists,
    ifelse = solrCall("if", 3),
    '!' = solrCall("not"),
    '&' = solrCall("and", 2),
    '|' = solrCall("or", 2),
    pdist = solrCall("dist", 3, Inf),
    ">" = solrGreaterThan,
    "<" = solrLessThan,
    ">=" = solrGreaterEqualThan,
    "<=" = solrLessEqualThan,
    "==" = solrEqual
    ), parent=varsEnv)
}

setMethod("translationContext", "SolrFunctionExpression",
          function(x, ...) SolrFunctionContext(...))

solrArg <- function(x) {
  if (is(x, "Expression")) {
    x <- as(x, "SolrFunctionExpression")
  } else {
    x <- solrEscape(x)
  }
  as.character(x)
}

setAs("Expression", "SolrFunctionExpression", function(from) {
  SolrFunctionExpression(paste0("exists(query(",
                                as(from, "SolrQParserExpression", strict=FALSE),
                                "))"))
})

setAs("name", "SolrFunctionExpression", function(from) {
  SolrFunctionExpression(as.character(from))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "Expression", function(object) {
  cat(class(object), ": ", as.character(object), "\n", sep="")
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utilities
###

VarsEnv <- function(expr, env) {
  nms <- all.vars(expr)
  list2env(setNames(lapply(nms, as.name), nms), parent=env)
}

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
