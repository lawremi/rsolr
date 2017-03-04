### =========================================================================
### SolrExpression objects
### -------------------------------------------------------------------------

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Base classes
###

setClass("SolrExpression", contains=c("Expression", "VIRTUAL"))

setClass("SolrFunctionExpression",
         representation(name="ANY"),
         prototype(name=""),
         contains="SolrExpression")

setClass("SolrLuceneExpression", contains=c("SolrExpression", "VIRTUAL"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Symbols
###

## a symbol that refers to a Solr field
setClass("SolrSymbol",
         representation(missable="logical"),
         prototype(missable=TRUE),
         contains=c("SimpleSymbol", "SolrFunctionExpression"),
         validity=function(object) {
             if (!isTRUEorFALSE(object@missable)) {
                 "'missable' must be TRUE or FALSE"
             }
         })
setClassUnion("SolrSymbolORNULL", c("SolrSymbol", "NULL"))

## for targeting Lucene expressions
setClass("SolrLuceneSymbol", contains="SolrSymbol")

## Result of x[i]
setClass("PredicatedSolrSymbol",
         representation(subject="SolrSymbol",
                        predicate="SolrExpression"),
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

setClass("SolrLuceneProhibit", contains="SolrLuceneUnaryOperator")

setClass("SolrLuceneTerm",
         representation(field="SolrSymbolORNULL",
                        term="ANY",
                        inverted="logical"),
         prototype(inverted=FALSE),
         contains="SolrLuceneExpression",
         validity=function(object) {
             if (!isTRUEorFALSE(object@inverted))
                 "'inverted' must be TRUE or FALSE"
         })

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
         representation(query="Expression",
                        useValueParam="logical",
                        tag="character_OR_NULL"),
         prototype(useValueParam=FALSE),
         contains="SolrExpression",
         validity=function(object) {
             if (!is.null(object@tag) && !isSingleString(object@tag))
                 "'tag' must be a single, non-NA string, or NULL"
         })

setClassUnion("SolrLuceneExpressionOrSymbol",
              c("SolrLuceneExpression", "SolrQParserExpression", "SolrSymbol"))

setClass("LuceneQParserExpression",
         representation(query="SolrLuceneExpression"),
         contains="SolrQParserExpression")

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
                        args="list",
                        missables="character"),
         contains=c("AbstractSolrFunctionCall", "SolrFunctionExpression"),
         validity=function(object) {
             c(if (!isSingleString(object@name))
                   "'name' of a Solr function call must be a string",
               if (any(is.na(object@missables)))
                   "'missables' must not contain NAs")
         })

setClassUnion("functionORNULL", c("function", "NULL"))

setClass("SolrAggregateCall",
         representation(name="character",
                        subject="SolrFunctionExpression",
                        params="list",
                        postprocess="functionORNULL"),
         contains = c("AbstractSolrFunctionCall", "SolrExpression"),
         validity=function(object) {
             if (!isSingleString(object@name))
                 "'name' of a Solr aggregate call must be a string"
         })

setClass("ParentSolrAggregateCall",
         representation(child="SolrAggregateCall"),
         contains="SolrAggregateCall")

setClass("SolrSortExpression",
         representation(by="SolrFunctionExpression",
                        decreasing="logical"),
         contains="SolrExpression")

setClass("SolrFilterExpression",
         representation(filter="SolrQParserExpression",
                        tag="character"),
         contains="SolrExpression",
         validity=function(object) {
         })

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

SolrLuceneProhibit <- function(e1) {
    new("SolrLuceneProhibit",
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

SolrQParserExpression <- function(tag = NULL) {
    if (!is.null(tag)) {
        tag <- as.character(tag)
    }
    new("SolrQParserExpression", tag=tag)
}

FRangeQParserExpression <- function(query, l = NULL, u = NULL,
                                    incl = TRUE, incu = TRUE)
{
    new("FRangeQParserExpression",
        query=as(query, "SolrFunctionExpression", strict=FALSE),
        l=l, u=u, incl=incl, incu=incu)
}

LuceneQParserExpression <- function(query) {
    new("LuceneQParserExpression",
        query=as(query, "SolrLuceneExpression", strict=FALSE))
}

JoinQParserExpression <- function(query, from, to) {
    new("JoinQParserExpression",
        query=as(query, "SolrLuceneExpression", strict=FALSE),
        from=as(from, "SolrSymbol", strict=FALSE),
        to=as(to, "SolrSymbol", strict=FALSE))
}

SolrFunctionExpression <- function(name="") {
    if (isNA(name)) {
        solrNA
    } else {
        new("SolrFunctionExpression", name=name)
    }
}

SolrFunctionCall <- function(name, args) {
    new("SolrFunctionCall", name=name, args=args,
        missables=missablesForArgs(name, args))
}

SolrAggregateCall <- function(name = "",
                              subject = SolrFunctionExpression(),
                              params=list(), child = NULL, postprocess = NULL)
{
    obj <- new("SolrAggregateCall", name=name,
               subject=as(subject, "SolrFunctionExpression", strict=FALSE),
               params=params, postprocess=postprocess)
    if (!is.null(child)) {
        obj <- new("ParentSolrAggregateCall", obj, child=child)
    }
    obj
}

SolrSortExpression <- function(decreasing) {
    new("SolrSortExpression", decreasing=decreasing)
}

setGeneric("SymbolFactory", function(x, ...) standardGeneric("SymbolFactory"))
setMethod("SymbolFactory", "SolrExpression", function(x) SolrSymbol)
setMethod("SymbolFactory", "SolrQParserExpression",
          function(x) SolrLuceneSymbol)

SolrSymbol <- new("SymbolFactory", function(name) {
    new("SolrSymbol", name=as.character(name))
})

SolrLuceneSymbol <- new("SymbolFactory", function(name) {
    new("SolrLuceneSymbol", name=as.character(name))
})

PredicatedSolrSymbol <- function(subject, predicate) {
    new("PredicatedSolrSymbol", subject=subject,
        predicate=as(predicate, "SolrExpression", strict=FALSE))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setGeneric("args", function(name) standardGeneric("args"))

setMethod("args", "SolrFunctionCall", function(name) name@args)
setMethod("args", "SolrAggregateCall",
          function(name) c(name@subject, name@params))

setGeneric("name", function(x) standardGeneric("name"))
setMethod("name", "ANY", function(x) x@name)

`name<-` <- function(x, value) {
    x@name <- value
    x
}

query <- function(x) x@query
`query<-` <- function(x, value) {
    x@query <- value
    x
}

setGeneric("tag", function(x, ...) standardGeneric("tag"))

setMethod("tag", "ANY", function(x) NULL)
setMethod("tag", "SolrQParserExpression", function(x) x@tag)

setGeneric("child", function(x) standardGeneric("child"))
setMethod("child", "ANY", function(x) NULL)
setMethod("child", "ParentSolrAggregateCall", function(x) x@child)

setGeneric("child<-", function(x, value) standardGeneric("child<-"))
setReplaceMethod("child", "SolrAggregateCall", function(x, value) {
                     if (!is.null(value)) {
                         new("ParentSolrAggregateCall", x, child=value)
                     } else {
                         x
                     }
                 })
setReplaceMethod("child", "ParentSolrAggregateCall", function(x, value) {
                     x@child <- value
                     x
                 })

resultWidth <- function(x) {
    if (name(x) == "percentile")
        length(params(x))
    else 1L
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Translation
###

setMethod("translate", c("SolrExpression", "SolrExpression"),
          function(x, target, context, ...) {
              as(x, class(target), strict=FALSE)
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Missing value handling
###
### We try our best to propagate missing values through mathematical
### expressions. This works well, except, as noted below, it breaks
### down in logical expressions and branching.
###

### FIXME: and() and or() are super-problematic, because
### and(FALSE, NA) is FALSE, while or(TRUE, NA) is TRUE.
### Thus, we cannot simply propagate NAs past those two functions. We
### could express them, however, as if() calls:
## or: if(or(a,b), true, if(and(exists(a), exists(b)), false, NA))
## and: if(or(not(a), not(b)), false, if(and(exists(a), exists(b)), true, NA))
### If and/or have any missables in their children, they generate such a
### structure. Note though that they propagate themselves as missable,
### and never drop the missables from their children.

### In the Lucene case, we do support the special cases of 'x |
### is.na(x)' and 'x & !is.na(x)' by simply not forwarding missing
### values past the '|' or '&' that contains the check.

### FIXME: if() currently forwards missings only through its
### condition. When if() is embedded into another expression, it
### should take that protected expression as missable.

missable <- function(x) x@missable
`missable<-` <- function(x, value) {
    x@missable <- value
    x
}

setGeneric("missables", function(x, fields = NULL) standardGeneric("missables"),
           signature="x")

setMethod("missables", "SolrSymbol", function(x) {
              if (missable(x)) {
                  list(name(x))
              } else {
                  list()
              }
          })

setMethod("missables", "SolrFunctionCall", function(x) x@missables)

setMethod("missables", "SolrFunctionExpression",
          function(x) missables(name(x)))

setMethod("missables", "SolrQParserExpression", function(x) missables(query(x)))

setMethod("missables", "SolrLuceneBinaryOperator",
          function(x) {
              missables <- c(missables(x@e1), missables(x@e2))
              naCheck <- if (isLuceneNACheck(x@e1)) x@e1
                         else if (isLuceneNACheck(x@e2)) x@e2
              if (!is.null(naCheck) &&
                  is(x, "SolrLuceneAND") == naCheck@inverted) {
                  missables <- setdiff(missables, name(naCheck@field))
              }
              missables
          })

setMethod("missables", "SolrLuceneUnaryOperator", function(x) missables(x@e1))

isLuceneNACheck <- function(x) {
    is(x, "SolrLuceneTerm") && identical(x@term, I("*"))
}

setMethod("missables", "SolrLuceneTerm", function(x) {
              if (isLuceneNACheck(x)) list() else missables(x@field)
          })

setMethod("missables", "ANY", function(x) list())

`missables<-` <- function(x, value) {
    x@missables <- value
    x
}

isSolrNACheck <- function(fun, args) {
    fun == "exists" &&
        !(is(args[[1L]], "SolrFunctionCall") && name(args[[1L]]) == "query")
}

missablesForArgs <- function(fun, args) {
    if (fun == "if") {
        args <- args[1L]
    }
    as.character(if (!isSolrNACheck(fun, args)) {
                     unique(unlist(lapply(args, missables)))
                 })
}

setGeneric("dropMissables", function(x, which) standardGeneric("dropMissables"))

setMethod("dropMissables", "SolrFunctionCall", function(x, which) {
              x@missables <- setdiff(x@missables, which)
              x@args <- lapply(x@args, dropMissables, which)
              x
          })

setMethod("dropMissables", "SolrFunctionExpression", function(x, which) {
              name(x) <- dropMissables(name(x), which)
              x
          })

setMethod("dropMissables", "SolrQParserExpression", function(x, which) {
              query(x) <- dropMissables(query(x), which)
              x
          })

setMethod("dropMissables", "SolrLuceneBinaryOperator", function(x, which) {
              initialize(x,
                         e1=dropMissables(x@e1, which),
                         e2=dropMissables(x@e2, which))
          })

setMethod("dropMissables", "SolrLuceneUnaryOperator", function(x, which) {
              initialize(x, e1=dropMissables(x@e1, which))
          })

setMethod("dropMissables", "SolrLuceneTerm", function(x, which) {
              x@field <- dropMissables(x@field, which)
              x
          })

setMethod("dropMissables", "SolrSymbol", function(x, which) {
              missable(x) <- missable(x) && !(name(x) %in% which)
              x
          })

setMethod("dropMissables", "ANY", function(x) {
              x
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Inversion
###
### If Lucene expressions are simply inverted with -(.), they will
### match whatever is not in ".", including missing values. Thus, we
### need to explicitly invert our expressions, in order to avoid
### matching missings. At non-range Lucene terms, we end up having to
### place a missing guard (AND foo:*). It might often be more
### efficient to gather these at the top of the expression, but beware
### that is a complicated task, if we care at all about correct NA
### propagation through boolean expressions, etc. So sue me.


setGeneric("invert", function(x) standardGeneric("invert"))

setMethod("invert", "SolrQParserExpression", function(x) {
              x@query <- invert(x@query)
              x
          })

setMethod("invert", "SolrLuceneAND", function(x) {
              SolrLuceneOR(invert(x@e1), invert(x@e2))
          })

setMethod("invert", "SolrLuceneOR", function(x) {
              SolrLuceneAND(invert(x@e1), invert(x@e2))
          })

setMethod("invert", "SolrLuceneProhibit", function(x) {
              x@e1
          })

### We defer inversion until serialization, because there is way to
### invert a Lucene term without explicitly checking for
### non-missing. If we generated that check prior to this point, it
### would result in a more complicated expression, and one that, if
### inverted, would match missings.

setMethod("invert", "SolrLuceneTerm", function(x) {
              x@inverted <- !x@inverted
              x
          })

setMethod("invert", "SolrLuceneRangeTerm", function(x) {
              x@term <- invert(x@term)
              x
          })

setMethod("invert", "LuceneRange", function(x) {
              initialize(x, from=x@to, to=x@from,
                         fromInclusive=!x@toInclusive,
                         toInclusive=!x@fromInclusive)
          })

setMethod("invert", "FRangeQParserExpression", function(x) {
              equals <- !is.null(x@u) && identical(x@u, x@l) && x@incl && x@incu
              if (equals) {
                  num <- if (!is.null(x@u)) x@u else x@l
                  minus <- SolrFunctionCall("sub", list(query(x), num))
                  expr <- SolrFunctionCall("abs", list(minus))
                  FRangeQParserExpression(expr, l=0L, incl=FALSE)
              } else if (is.null(x@u) || is.null(x@l)) {
                  initialize(x, l=x@u, u=x@l, incu=!x@incl, incl=!x@incu)
              } else {
                  stop("cannot invert frange")
              }
          })

setMethod("invert", "SolrFunctionExpression", function(x) {
              SolrFunctionCall("not", list(x))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Serialization
###

wrapParens <- function(x) { # something breaks with (-(foo:bar))
    if (!is(x, "SolrLuceneUnaryOperator")) {
        paste0("(", x, ")")
    } else {
        x
    }
}

setMethod("as.character", "SolrLuceneOR", function(x) {
              paste(wrapParens(x@e1), wrapParens(x@e2))
          })

setMethod("as.character", "SolrLuceneAND", function(x) {
              paste(wrapParens(x@e1), "AND", wrapParens(x@e2))
          })

setMethod("as.character", "SolrLuceneProhibit", function(x) {
              if (substring(x@e1, 1L, 1L) == "-") {
                  expr <- substring(x@e1, 2L)
              } else {
                  expr <- paste0("-", wrapParens(x@e1))
              }
          })

setMethod("as.character", "SolrLuceneTerm", function(x) {
              if (x@inverted) {
                  x@inverted <- FALSE
                  if (is.logical(x@term)) {
                      x@term <- !x@term
                  } else {
                      wrapped <- SolrLuceneProhibit(x)
                      if (!isLuceneNACheck(x)) {
                          wrapped <- SolrLuceneAND(wrapped,
                                                   initialize(x, term=I("*")))
                      }
                      return(as.character(wrapped))
                  }
              }
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
              params$useValueParam <- NULL
              params <- Filter(Negate(is.null), params)
              def <- mapply(identical, params,
                            slotsAsList(new(class(x)))[names(params)])
              params <- params[!as.logical(def)]
              if (x@useValueParam) { # just could not stand the \\\\\" escapes
                  params$v <- x@query
              }
              qparser <- qparserFromExpr(x)
              params <- vapply(params, normLuceneLiteral, character(1L))
              paste0("{!", qparser, if (length(params)) " ",
                     paste(names(params), params, sep="=", collapse=" "), 
                     "}", if (!x@useValueParam) x@query)
          })

setMethod("as.character", "FRangeQParserExpression", function(x) {
              missables <- missables(query(x))
              if (length(missables) > 0L) {
                  expr <- Reduce(SolrLuceneAND,
                                 c(lapply(missables, SolrLuceneTerm, I("*")),
                                   dropMissables(x, missables)))
                  as.character(as(expr, "SolrQParserExpression"))
              } else {
                  callNextMethod()
              }
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
              ans <- as.character(x@name)
              if (is.logical(x@name))
                  ans <- tolower(ans)
              ans
          })

normSolrArg <- function(x) {
    x <- fulfill(x)
    if (is(x, "Expression")) {
        x <- as.character(as(x, "SolrFunctionExpression", strict=FALSE))
    } else {
        x <- normLuceneLiteral(x)
    }
    if (anyNA(x)) {
        stop("cannot represent NA as a Solr function argument")
    }
    x
}

setMethod("as.character", "AbstractSolrFunctionCall", function(x) {
              args <- vapply(args(x), normSolrArg, character(1L))
              paste0(name(x), "(", paste(args, collapse=","), ")")
          })

callQuery <- function(x) {
    query <- as(x, "SolrQParserExpression", strict=FALSE)
    query@useValueParam <- TRUE
    quoted <- SolrFunctionExpression(query) # solr has NSE too!
    SolrFunctionCall("query", list(quoted))
}

symbolExists <- function(x) SolrFunctionCall("exists", list(I(x)))
symbolAnd <- function(x, y) SolrFunctionCall("and", list(x, y))

noneMissing <- function(missables) {
    Reduce(symbolAnd, lapply(missables, symbolExists))
}

setGeneric("propagateNAs",
           function(x, na=solrNA) standardGeneric("propagateNAs"),
           signature="x")

setMethod("propagateNAs", "SolrFunctionCall", function(x, na) {
              missables <- missables(x)
              if (length(missables) == 0L) {
                  return(x)
              }
              condition <- noneMissing(missables)
              x <- dropMissables(x, missables)
              SolrFunctionCall("if", list(condition, x, na))
          })

setMethod("propagateNAs", "SolrSymbol", function(x, na) {
              missable(x) <- FALSE
              x
          })

setMethod("as.character", "SolrFunctionCall", function(x) {
              x <- propagateNAs(x)
              callNextMethod()
          })

setMethod("as.character", "SolrSortExpression", function(x) {
              paste(x@by, if (x@decreasing) "desc" else "asc")
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Lucene coercion
###

setAs("SolrQParserExpression", "SolrLuceneExpression", function(from) {
          SolrLuceneTerm("_query_", from)
      })

setAs("SolrFunctionCall", "SolrQParserExpression", function(from) {
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
          if (isNA(from)) {
              from <- FALSE
          }
          if (!isTRUEorFALSE(from)) {
              stop("'from' must be TRUE, FALSE or NA")
          }
          true <- SolrLuceneTerm("*", I("*"))
          if (!from) {
              SolrLuceneProhibit(true)
          } else {
              true
          }
      })

setAs("logical", "SolrExpression", function(from) {
          as(from, "SolrLuceneExpression")
      })

setAs("ANY", "SolrQParserExpression", function(from) {
          LuceneQParserExpression(from)
      })

setAs("ANY", "SolrLuceneExpressionOrSymbol", function(from) {
          as(from, "SolrQParserExpression")
      })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr Function coercion
###

setAs("Expression", "SolrFunctionExpression", function(from) {
          SolrFunctionCall("exists", list(callQuery(from)))
      })

setAs("ANY", "SolrFunctionExpression", function(from) {
          SolrFunctionExpression(from)
      })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### SolrSymbol coercion
###

setAs("ANY", "SolrSymbol", function(from) SolrSymbol(from))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show methods
###

setMethod("show", "SolrExpression", function(object) {
              cat(as.character(object), "\n")
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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Global Solr NA hack
###

solrQueryNA <- SolrLuceneProhibit(SolrLuceneTerm("*", I("*")))
solrNA <- callQuery(solrQueryNA)

