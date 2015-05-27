### =========================================================================
### SolrExpression objects
### -------------------------------------------------------------------------

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Base classes
###

setClass("SolrExpression", contains=c("Expression", "VIRTUAL"))

setClass("SolrFunctionExpression",
         representation(name="ANY"),
         contains="SolrExpression")

setClass("SolrLuceneExpression", contains=c("SolrExpression", "VIRTUAL"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Symbols
###

## a symbol that refers to a Solr field
setClass("SolrSymbol", contains=c("SimpleSymbol", "SolrFunctionExpression"))
setClassUnion("SolrSymbolORNULL", c("SolrSymbol", "NULL"))

## for targeting Lucene expressions
setClass("SolrLuceneSymbol", contains="SolrSymbol")

## Result of x[i]
setClass("PredicatedSolrSymbol",
         representation(subject="SolrSymbol",
                        predicate="SolrLuceneExpression"),
         contains="SolrExpression")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### SolrExpression subtypes
###

setClass("SolrLuceneBinaryOperator",
         representation(e1="SolrLuceneExpression",
                        e2="SolrLuceneExpression"),
         contains=c("SolrLuceneExpression", "VIRTUAL"))

setClass("SolrLuceneAND", contains="SolrLuceneBinaryOperator")
setClass("SolrLuceneOR", contains="SolrLuceneBinaryOperator")

setClass("SolrLuceneUnaryOperator",
         representation(e1="SolrLuceneExpression"),
         contains=c("SolrLuceneExpression", "VIRTUAL"))

setClass("SolrLuceneNOT", contains="SolrLuceneUnaryOperator")

setClass("SolrLuceneTerm",
         representation(field="SolrSymbolORNULL",
                        term="ANY"),
         contains="SolrLuceneExpression")

setClass("LuceneRange",
         representation(from="ANY", to="ANY",
                        fromInclusive="logical", toInclusive="logical"),
         prototype(fromInclusive=FALSE, toInclusive=FALSE),
         validity=function(object) {
             if (!isTRUEorFALSE(object@fromInclusive) ||
                 !isTRUEorFALSE(object@toInclusive))
                 "fromInclusive and toInclusive must be TRUE or FALSE"
         })

setClass("SolrLuceneRangeTerm",
         representation(term="LuceneRange"),
         contains="SolrLuceneTerm")

setClass("SolrQParserExpression",
         representation(query="Expression"),
         contains="SolrExpression")

setClass("LuceneQParserExpression",
         representation(op="character",
                        df="SolrSymbolORNULL",
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
         representation(from="SolrSymbol",
                        to="SolrSymbol",
                        query="SolrLuceneExpression"),
         contains="SolrQParserExpression")

setClass("AbstractSolrFunctionCall")

setClass("SolrFunctionCall",
         representation(name="character",
                        args="list"),
         contains=c("AbstractSolrFunctionCall", "SolrFunctionExpression"),
         validity=function(object) {
             if (!isSingleString(object@name))
                 stop("'name' of a Solr function call must be a string")
         })

setClassUnion("functionORNULL", c("function", "NULL"))

setClass("SolrAggregateCall",
         representation(name="character",
                        subject="SolrFunctionExpression",
                        params="list",
                        augment="list",
                        postprocess="functionORNULL"),
         contains = c("AbstractSolrFunctionCall", "SolrExpression"),
         validity=function(object) {
             c(if (!isSingleString(object@name))
                   stop("'name' of a Solr aggregate call must be a string"),
               validHomogeneousList(object@args, "SolrAggregateCall"))
         })

setClass("SolrSortExpression",
         representation(by="SolrFunctionExpression",
                        decreasing="logical"),
         contains="SolrExpression")


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

SolrLuceneOR <- function(e1, e2) {
    new("SolrLuceneOR",
        e1=as(e1, "SolrLuceneExpression", strict=FALSE),
        e2=as(e2, "SolrLuceneExpression", strict=FALSE))
}

SolrLuceneAND <- function(e1, e2) {
    new("SolrLuceneAND",
        e1=as(e1, "SolrLuceneExpression", strict=FALSE),
        e2=as(e2, "SolrLuceneExpression", strict=FALSE))
}

SolrLuceneNOT <- function(e1) {
    new("SolrLuceneNOT",
        e1=as(e1, "SolrLuceneExpression", strict=FALSE))
}

SolrLuceneTerm <- function(field, term) {
    new("SolrLuceneTerm",
        field=if (!is.null(field)) as(field, "SolrSymbol", strict=FALSE),
        term=term)
}

SolrLuceneRangeTerm <- function(field, from, to, fromInclusive, toInclusive) {
    new("SolrLuceneRangeTerm", field=field,
        term=new("LuceneRange", from=from, to=to,
            fromInclusive=fromInclusive, toInclusive=toInclusive))
}

SolrQParserExpression <- function() {
    new("SolrQParserExpression")
}

FRangeQParserExpression <- function(query, l = NULL, u = NULL,
                                    incl = TRUE, incu = TRUE)
{
    new("FRangeQParserExpression",
        query=as(query, "SolrFunctionExpression", strict=FALSE),
        l=l, u=u, incl=incl, incu=incu)
}

LuceneQParserExpression <- function(query, op = "OR", df = NULL) {
    new("LuceneQParserExpression",
        query=as(query, "SolrLuceneExpression", strict=FALSE),
        op=op, df=df)
}

JoinQParserExpression <- function(query, from, to) {
    new("JoinQParserExpression",
        query=as(query, "SolrLuceneExpression", strict=FALSE),
        from=as(from, "SolrSymbol", strict=FALSE),
        to=as(to, "SolrSymbol", strict=FALSE))
}

SolrFunctionExpression <- function(name) {
    new("SolrFunctionExpression", name=name)
}

SolrFunctionCall <- function(name, args) {
    new("SolrFunctionCall", name=name, args=args)
}

SolrAggregateCall <- function(name, subject, na.rm, params=list(),
                                    aux = list(), postprocess = NULL)
{
    new("SolrAggregateCall", name=name,
        subject=as(subject, "SolrFunctionExpression", strict=FALSE),
        na.rm=na.rm, params=params, aux=aux, postprocess=postprocess)
}

SolrSortExpression <- function(decreasing) {
    new("SolrSortExpression", decreasing=decreasing)
}

setMethod("symbolFactory", "SolrExpression", function(x) SolrSymbol)
setMethod("symbolFactory", "SolrQParserExpression",
          function(x) SolrLuceneSymbol)

SolrSymbol <- new("SymbolFactory", function(name) {
    new("SolrSymbol", name=as.character(name))
})

SolrLuceneSymbol <- new("SymbolFactory", function(name) {
    new("SolrLuceneSymbol", name=as.character(name))
})

PredicatedSolrSymbol <- function(subject, predicate) {
    new("PredicatedSolrSymbol", subject=subject,
        predicate=as(predicate, "SolrLuceneExpression", strict=FALSE))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessor
###

setGeneric("args", function(name) standardGeneric("args"))

setMethod("args", "SolrFunctionCall", function(name) name@args)
setMethod("args", "SolrAggregateCall",
          function(name) c(name@subject, name@params))

setGeneric("name", function(x) standardGeneric("name"))
setMethods("name", list("SolrFunctionCall", "SolrAggregateCall"),
           function(x) x@name)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Serialization
###

setMethod("as.character", "SolrLuceneOR", function(x) {
              paste(wrapParens(x@e1), wrapParens(x@e2))
          })

setMethod("as.character", "SolrLuceneAND", function(x) {
              paste(wrapParens(x@e1), "AND", wrapParens(x@e2))
          })

setMethod("as.character", "SolrLuceneNOT", function(x) {
              if (substring(x@e1, 1L, 1L) == "-") {
                  expr <- substring(x@e1, 2L)
              } else {
                  expr <- paste0("-", wrapParens(x@e1))
              }
          })

setMethod("as.character", "SolrLuceneTerm", function(x) {
              term <- normLuceneLiteral(x@term)
              if (!is.null(x@field)) {
                  paste0(x@field, ":", term)
              } else term
          })

setMethod("as.character", "LuceneRange", function(x) {
              paste0(if (x@fromInclusive) "[" else "{",
                     normLuceneLiteral(x@from), " TO ", normLuceneLiteral(x@to),
                     if (x@toInclusive) "]" else "}")
          })

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
              params <- vapply(params, normLuceneLiteral, character(1L))
              paste0("{!", qparser, if (length(params)) " ",
                     paste(names(params), params, sep="=", collapse=" "), 
                     "v=", normLuceneLiteral(x@query), "}")
          })

setMethod("as.character", "JoinQParserExpression", function(x) {
              if (length(x@to) == 0L) {
                  stop("incomplete join operation, try syntax: x %in% y[expr]")
              }
              callNextMethod()
          })

normLuceneLiteral <- function(x) {
    x <- fulfill(x)
    if (is.factor(x)) {
        x <- as.character(x)
    } else if (is(x, "POSIXt") || is(x, "Date")) {
        x <- toSolr(x, new("solr.DateField"))
    } else if (is(x, "Expression")) {
        x <- as.character(x)
    }
    if (is.character(x) && !is(x, "AsIs")) {
        x <- paste0("\"", gsub("\"", "\\\"", gsub("\\", "\\\\", x, fixed=TRUE),
                               fixed=TRUE), "\"")
    } else if (is.logical(x)) {
        x <- tolower(as.character(x))
    }
    if (length(x) > 1L) {
        x <- paste0("(", paste(x, collapse=" "), ")")
    }
    as.character(x)
}

setMethod("as.character", "SolrFunctionExpression", function(x) {
              as.character(x@name)
          })

normSolrArg <- function(x) {
    x <- fulfill(x)
    if (is(x, "Expression")) {
        x <- as(x, "SolrFunctionExpression", strict=FALSE)
    } else {
        x <- normLuceneLiteral(x)
    }
    as.character(x)
}

setMethod("as.character", "AbstractSolrFunctionCall", function(x) {
              args <- vapply(args(x), normSolrArg, character(1L))
              paste0(name(x), "(", paste(args, collapse=","), ")")
          })

setMethod("as.character", "SolrSortExpression", function(x) {
              paste(x@by, if (x@decreasing) "desc" else "asc")
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Translation overrides
###

### Because we store the 'decreasing' arg on the 'target'.  We could
### support decreasing() in the expression itself, which would convert
### a SolrFunctionExpression to a SolrSortExpression, but we want to
### support the separate 'decreasing' arg, for consistency with R.
setMethod("translate", c("ANY", "SolrSortExpression"),
          function(x, target, ...) {
              expr <- translate(x, SolrFunctionExpression(), ...)
              initialize(target, by=expr)
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Lucene coercion
###

setAs("SolrLuceneExpression", "SolrQParserExpression", function(from) {
          LuceneQParserExpression(from)
      })

setAs("SolrQParserExpression", "SolrLuceneExpression", function(from) {
          SolrLuceneTerm("_query_", from)
      })

setAs("SolrFunctionExpression", "SolrQParserExpression", function(from) {
          FRangeQParserExpression(from, l=1, u=1)
      })

setAs("SolrSymbol", "SolrLuceneExpression", function(from) {
          SolrLuceneTerm(from, TRUE)
      })

setAs("character", "SolrLuceneExpression", function(from) {
          SolrLuceneTerm(NULL, from)
      })

setAs("AsIs", "SolrLuceneExpression", function(from) {
          SolrLuceneTerm(NULL, from)
      })

setAs("logical", "SolrLuceneExpression", function(from) {
          if (!isTRUEorFALSE(from)) {
              stop("'from' must be TRUE or FALSE")
          }
          true <- SolrLuceneTerm("*", I("*"))
          if (!from) {
              SolrLuceneNOT(true)
          } else {
              true
          }
      })

setAs("ANY", "SolrQParserExpression", function(from) {
          LuceneQParserExpression(from)
      })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Function coercion
###

setAs("Expression", "SolrFunctionExpression", function(from) {
          ## we assume that the Solr QParser framework is more general
          ## and thus targetable by more languages...
          query <- as(from, "SolrQParserExpression", strict=FALSE)
          arg <- SolrFunctionExpression(query)
          SolrFunctionCall("exists", list(SolrFunctionCall("query", list(arg))))
      })

setAs("ANY", "SolrFunctionExpression", function(from) {
          SolrFunctionExpression(from)
      })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### SolrSymbol coercion
###

setAs("ANY", "SolrSymbol", function(from) SolrSymbol(from))

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
