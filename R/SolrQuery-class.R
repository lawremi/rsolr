### =========================================================================
### SolrQuery objects
### -------------------------------------------------------------------------

### Represents a query to a Solr search engine.

### We want to take a lazy approach to query evaluation. The
### alternative would be specifying a Solr query (which can be quite
### complex) in a single call. The current "best practice" for complex
### commands is to use a parameter object. For us, the parameter
### object is a query, and the query is evaluated by a Solr core. The
### main argument for deferred query evaluation is to manage
### complexity, but there is also opportunity for optimization by
### batching queries.

### A SolrQuery is evaluated by a SolrCore to yield a SolrResult.  All
### operations are deferred until evaluation, i.e., when eval() is
### called. Since the lazy behavior is explicit, it is paramount that
### we do not evaluate any query expressions until the query is
### evaluated. An easy way to achieve that is to simply store the R
### language objects, along with their enclosing context, which
### includes the calling R environment, as well as the expressions
### defining virtual columns via transform(). Those column expressions
### are actively bound to the environment, and the active binding
### evaluates the expression inside the environment (and saves the
### result, making the binding static).  The expression of interest is
### evaluated in the environment. Finally, we extract the expression
### from each promise, coerce it to character, and collapse the
### results to the URL query string.

### What happens if an expression evaluates to something other than a
### SolrPromise that points to our core? Probably should throw an
### error for now.

### Support for Solr extensions
## Custom functions: via methods on SolrPromise
## Custom query parsers: via Expression framework, methods on SolrPromise
## Custom search components: use low-level parameter API
## Custom request handlers: use low-level restfulr API
## Custom response writers and update handlers: via Media framework
## Custom field types: via FieldType framework

### ========================== ISSUES ============================

### The concern is that users might infer behavior that is difficult
### to support with Solr, because it is simply not designed for
### analytics. A good example is the order of operations:

## x <- transform(x, sqrtScore = sqrt(score))
## subset(x, sqrtScore > 10)

### Since 'sqrtScore' is defined by &fl, it is not subject to &fq
### filtering (it is never indexed). In other words, some parameters
### affect the query, while others manipulate the output of the query.
### We can actually work around this by computing sqrtScore as part of
### the query, i.e., we just substitute the expression. Also, any
### aggregation used in queries or transforms are simply fulfilled and
### substituted in the expression, as they are scalar. But there are
### still problems:

### * output restrictions, like head/tail, will not affect e.g. aggregation
###   - we have to document that head/tail is a *summary*, not a filter

### * Solr always applies filters before any aggregation. Aggregation
###   inside transform() calls are fine, since we need to fulfill
###   those before the main query anyway, but for facets we need to
###   use the filter exclusion feature.

setClass("SolrQuery",
         representation(params="list"),
         prototype = list(
           params = list(
             q     = "*:*",
             start = 0L,
             rows  = .Machine$integer.max,
             fl    = list("*"))
           ))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

SolrQuery <- function(expr, requestHandler = NULL, ...)
{
    ans <- new("SolrQuery")
    if (!missing(expr)) {
        ans <- eval(substitute(subset(ans, expr)), parent.frame())
    }
    requestHandler(ans) <- requestHandler
    args <- list(...)
    params(ans)[names(args)] <- args
    ans
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Low-level accessors
###

params <- function(x) {
  x@params
}

`params<-` <- function(x, value) {
  x@params <- value
  x
}

configure <- function(x, ...) {
  args <- c(...)
  x@params[names(args)] <- args
  x
}

json <- function(x) {
    params(x)[["json"]]
}

`json<-` <- function(x, value) {
    params(x)[["json"]] <- value
    x
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### SolrQueryTranslationSource
###
### The intent is to preserve the state when the translation was requested
###

setClassUnion("formulaORNULL", c("formula", "NULL"))

### DOCDB: DocDbQueryTranslationSource does not track a grouping. That
### state should be on SolrQuery, and the DocDbFrame method should
### yield a GroupedSolrFrame if necessary.

QueryHandle <- setRefClass("QueryHandle",
                           fields=c(i="character"),
                           methods=list(
                               get = function() QUERY_HEAP$get(.self),
                               finalize = function() QUERY_HEAP$remove(.self)
                           ))

setMethod("show", "QueryHandle",
          function(object) cat("QueryHandle:", as.character(object), "\n"))

as.character.QueryHandle <- function(x) x$i

QueryHeap_store <- function(query) {
    key <- .self$nextKey()
    .self$queries[[as.character(key)]] <- query
    key
}

QueryHeap_nextKey <- function() {
    if (length(.self$free) > 0L) {
        i <- .self$free[1L]
        .self$free <- .self$free[-1L]
    } else {
        i <- as.character(length(.self))
    }
    QueryHandle(i=i)
}

globalVariables(".self")
QueryHeap_get <- function(key) {
    .self$queries[[as.character(key)]]
}

QueryHeap_remove <- function(key) {
    i <- as.character(key)
    if (length(i) == 1L) { # be robust to dummy handle finalization
        rm(list=i, envir=.self$queries)
        .self$free <- c(.self$free, i)
    }
    invisible(.self)
}

QUERY_HEAP <- setRefClass("QueryHeap",
                           fields=c(queries="environment",
                                    free="character"),
                           methods = list(add=QueryHeap_store,
                                          nextKey=QueryHeap_nextKey,
                                          get=QueryHeap_get,
                                          remove=QueryHeap_remove))()

length.QueryHeap <- function(x) length(x$queries)

setMethod("show", "QueryHeap",
          function(object) cat("QueryHeap with", length(object), "queries\n"))

setClass("SolrQueryTranslationSource",
         representation(expr="ANY",
                        queryHandle="QueryHandle",
                        env="environment",
                        grouping="formulaORNULL"),
         contains="Expression")

setMethod("translate", c("SolrQueryTranslationSource", "Expression"),
          function(x, target, core, ...) {
              query <- x@queryHandle$get()
              frame <- group(.SolrFrame(core, query), x@grouping)
              context <- DelegateContext(frame, x@env)
              translate(expr(x), target, context, ...)
          })

setMethod("as.character", "SolrQueryTranslationSource",
          function(x) {
              if (is.language(expr(x))) {
                  deparse(expr(x))
              } else {
                  as.character(expr(x))
              }
          })

deferTranslation <- function(x, expr, target, env, grouping=NULL) {
    expr <- preprocessExpression(expr, env)
    queryHandle <- QUERY_HEAP$add(x)
    fenv <- funsEnv(expr, env)
    src <- new("SolrQueryTranslationSource", expr=expr, queryHandle=queryHandle,
               env=fenv, grouping=grouping)
    new("TranslationRequest", src=src, target=target)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr query component
###

setMethod("translate", c("SolrQueryTranslationSource", "SolrQParserExpression"),
          function(x, target, ...) {
              expr <- callNextMethod()
              initialize(expr, tag=tag(target))
          })

setMethod("tag", "TranslationRequest", function(x) tag(x@target))

setMethod("subset", "SolrQuery",
          function(x, subset, select, fields, select.from = character()) {
   if (!missing(subset)) {
     tag <- sum(names(params(x)) == "fq")
     fq <- deferTranslation(x, substitute(subset),
                            SolrQParserExpression(tag),
                            top_prenv(subset))
     params(x) <- c(params(x), fq = fq)
  }
  if (!missing(select)) {
    if (!missing(fields)) {
      stop("only one of 'fields' and 'select' can be specified")
    }
    inds <- as.list(seq_along(select.from))
    names(inds) <- select.from
    fields <- select.from[eval(substitute(select), inds, top_prenv(select))]
  }
  if (!missing(fields)) {
    x <- restrictToFields(x, fields)
  }
  x
})

setGeneric("searchDocs", function(x, q, ...) standardGeneric("searchDocs"))

setMethod("searchDocs", c("SolrQuery", "ANY"), function(x, q) {
              q <- as.character(q)
              stopifnot(isSingleString(q))
              params(x)$q <- q
              sort(x, by=~score, decreasing=TRUE)
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Output manipulation
###

transform.SolrQuery <- function(`_data`, ...) transform(`_data`, ...)

setMethod("transform", "SolrQuery", function (`_data`, ...) {
  e <- as.list(substitute(list(...))[-1L])
  if (length(e) == 0L)
    return(`_data`)
  if (is.null(names(e)))
    names(e) <- ""
  fl <- mapply(deferTranslation, list(`_data`), e,
               list(SolrFunctionExpression()),
               top_prenv_dots(...))
  ## If a simple symbol is passed, Solr will rename the field, rather
  ## than copy the column. That is different from transform()
  ## behavior.  Sure, aliasing is sort of pointless server-side, but
  ## we do not want to surprise the user.
  aliased <- vapply(e, is.name, logical(1L))
  identity.aliases <- unique(as.character(e[aliased]))
  params(`_data`)$fl <- c(params(`_data`)$fl, setNames(fl, names(e)),
                          setNames(identity.aliases, identity.aliases))
  `_data`
})

### Also exists in S4Vectors, but the mapping is opposite!
setGeneric("rename", function(x, ...) standardGeneric("rename"))

setMethod("rename", "SolrQuery", function(x, ...) {
              map <- c(...)
              if (!is.character(map) || any(is.na(map))) {
                  stop("arguments in '...' must be character and not NA")
              }
              fl <- lapply(map, as, "SolrFunctionExpression")
              params(x)$fl <- c(params(x)$fl, fl)
              x
          })

solrInf <- SolrFunctionCall("div", list(1, 0))
solrNegInf <- SolrFunctionCall("div", list(-1, 0))

### SOLRBUG:
### Seems that character types do not sort via function queries.
### Any NAs are probably treated as empty strings and therefore come first,
### unless the schema is configured to sort NAs specially.
isCharacterSymbol <- function(expr, core) {
    if (is(expr, "SolrSymbol")) {
        name(expr) != "score" &&
            !is(fieldTypes(schema(core), name(expr))[[1L]], "CharacterField")
    } else {
        FALSE
    }
}

### FIXME: we substitute (-)infinity for NAs so that they sort
### last. Solr itself supports sorting NAs last or first, according to
### a setting in the schema. If we tracked that information, we could
### avoid this hack in the case of symbols. But it's debatable whether
### we should always obey the schema, or try to be always consistent
### with R (and calls, which we would need to treat ourselves).

setMethod("translate", c("SolrQueryTranslationSource", "SolrSortExpression"),
          function(x, target, core, ...) {
              by <- translate(x, target@by, core, ...)
              if (!isCharacterSymbol(by, core)) {
                  inf <- if (target@decreasing) solrNegInf else solrInf
                  by <- propagateNAs(by, inf)
              }
              initialize(target, by=by)
          })

sortParams <- function(x, by, decreasing) {
    lapply(stripI(parseFormulaRHS(by)), deferTranslation, x=x,
           target=SolrSortExpression(decreasing), env=attr(by, ".Environment"))
}

setMethod("sort", "SolrQuery", function (x, decreasing = FALSE, by = ~ score) {
  if (!isTRUEorFALSE(decreasing))
    stop("'decreasing must be TRUE or FALSE")
  sort <- sortParams(x, by, decreasing)
  params(x)$sort <- c(sort, params(x)$sort)
  x
})

rev.SolrQuery <- function (x, ...) rev(x)

setMethod("rev", "SolrQuery", function(x) {
              if (is.null(params(x)$sort))
                  x <- sort(x)
              else {
                  params(x)$sort <- lapply(params(x)$sort, function(xi) {
                                               xi@target@decreasing <-
                                                   !xi@target@decreasing
                                               xi
                                           })
              }
              x
})

### head/tail are complicated, because the indices can be relative to the
### end, which we do not know, because NROWS is not known. We indicate
### this using negative values.

## head(x, -n) => (-)rows = min(rows, NROWS)-n
## head(x, +n) => (+)rows = min(rows, n)
## tail(x, +n) => (-)start = start+min(rows, NROWS)-n; rows = min(rows, n)
## tail(x, -n) => (+)start = start+n; rows = min(rows, NROWS)-n

window.SolrQuery <- function(x, ...) window(x, ...)

setMethod("window", "SolrQuery", function (x, start = 1L, end = NA_integer_) {
  if (!isSingleNumber(start))
    stop("'start' must be a single, non-NA number")
  if (!is.na(end) && !isSingleNumber(end))
    stop("'end' must be a single number")
  rows <- end - start + 1L
  setOutputBounds(x, start, rows)
})

setOutputBounds <- function(x, start = 1L, rows = NA_integer_) {
  start <- start - 1L
  params(x) <- boundsParams(params(x), start, rows)
  x
}

boundsParams <- function(p, start, rows) {
  if (identical(rows, 0L)) { # simplify for trivial case of no results
    p$start <- p$rows <- NULL
  }
  p$start <- c(p$start, start)
  p$rows <- c(p$rows, rows)
  p
}

head.SolrQuery <- function (x, n = 6L, ...) {
  if (!isSingleNumber(n))
    stop("'n' must be a single, non-NA number")
  setOutputBounds(x, rows = n)
}
setMethod("head", "SolrQuery", head.SolrQuery)

tail.SolrQuery <- function (x, n = 6L, ...) {
  if (!isSingleNumber(n))
    stop("'n' must be a single, non-NA number")
  setOutputBounds(x, start = -n + 1L)
}
setMethod("tail", "SolrQuery", tail.SolrQuery)

outputRestricted <- function(x) {
  params(x)$start > 0L || params(x)$rows < .Machine$integer.max
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Group component
###
### Only useful because it truncates by group.
###

configureGroups <- function(x, limit, offset, ...) {
  if (!isSingleNumber(limit)) {
    stop("'limit' must be a single, non-NA number")
  }
  if (!isSingleNumber(offset)) {
    stop("'offset' must be a single, non-NA number")
  }
  if (is.null(params(x)$group.sort)) {
      params(x)$group.sort <- params(x)$sort
      params(x)$sort <- NULL
  }
  configure(x, group="true", group.limit=limit, group.offset=offset, ...)
}

setGeneric("group", function(x, by, ...) standardGeneric("group"))

setMethod("group", c("SolrQuery", "language"),
          function(x, by, limit = .Machine$integer.max, offset = 0L,
                   env = emptyenv()) {
    func <- deferTranslation(x, by, SolrFunctionExpression(), env)
    configureGroups(x, limit, offset, group.func=func)
})

setMethod("group", c("SolrQuery", "name"),
          function(x, by, limit = .Machine$integer.max, offset = 0L, env) {
  group(x, by=as.character(by), limit=limit, offset=offset)
})

setMethod("group", c("SolrQuery", "character"),
          function(x, by, limit = .Machine$integer.max, offset = 0L) {
  if (!isSingleString(by)) {
    stop("'by' must be a single, non-NA string")
  }
  configureGroups(x, limit, offset, group.field=by)
})

setMethod("group", c("SolrQuery", "formula"), function(x, by, ...) {
  hasLHS <- length(by) == 3L
  if (hasLHS) {
    x <- subset(x, fields=as.character(by[[2L]]))
    by[[2L]] <- NULL
  }
  exprs <- parseFormulaRHS(by)
  if (length(exprs) > 1L) {
    stop("'by' must be a formula with a single term")
  }
  sort(group(x, exprs[[1L]], env=attr(by, ".Environment"), ...), by=by)
})

grouped <- function(x) identical(params(x)$group, "true")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Facet component
###

### NOTE: We only support 'exclude' for compatibility with
###       xtabs(na.action=na.pass, exclude=NULL), but with our default,
###       only 'na.action=na.pass' is needed for counting NAs.

normNAAction <- function(na.action) {
    if (missing(na.action)) {
        na.action <- getOption("na.action", na.omit)
    }
    na.action <- match.fun(na.action)
    if (!identical(na.action, na.omit) &&
        !identical(na.action, na.pass)) {
        stop("'na.action' must be either 'na.omit' or 'na.pass'")
    }
    na.action
}

setMethod("xtabs", "SolrQuery",
          function(formula, data,
                   subset, sparse = FALSE, 
                   na.action, exclude = NULL,
                   drop.unused.levels = FALSE) {
              if (!identical(sparse, FALSE)) {
                  stop("'sparse' must be FALSE")
              }
              na.action <- normNAAction(na.action)
              if (!is.null(exclude) &&
                  !(length(exclude)==1L && is.na(exclude))) {
                  stop("'exclude' should be 'NA' or 'NULL'")
              }
              useNA <- is.null(exclude) && identical(na.action, na.pass)
              if (!missing(subset)) {
                  data <- rsolr::subset(data, .(substitute(subset)))
              }
              facet(data, formula, useNA=useNA, drop=drop.unused.levels)
          })

setGeneric("facet", function(x, by = NULL, ...) standardGeneric("facet"))

setMethod("facet", c("SolrQuery", "NULL"),
          function(x, by, ..., useNA=FALSE, sort=NULL,
                   decreasing=FALSE, limit=NA_integer_) {
              stats <- statsParams(x, by, ...)
              facetParams(x)[names(stats)] <- stats
              x
          })

setMethod("facet", c("SolrQuery", "formula"), function(x, by, ...) {
              factors <- parseFormulaRHS(by)
              facetParams(x) <- mergeFacetParams(x, json(x)$facet, factors,
                                                 grouping=by,
                                                 where=attr(by, ".Environment"),
                                                 ...)
              x
          })

setMethod("facet", c("SolrQuery", "character"), function(x, by, ...) {
              facetParams(x)[by] <- facetParams(x, by, ...)
              x
          })

setGeneric("facetParams", function(x, by=list(), ...)
    standardGeneric("facetParams"))

setMethod("facetParams", c("SolrQuery", "missing"), function(x, by, ...) {
              json(x)$facet              
          })

setMethod("facetParams", c("SolrQuery", "character"),
          function(x, by, ..., useNA=FALSE, sort=NULL,
                   decreasing=FALSE, limit=NA_integer_, drop=TRUE) {
              if (!isTRUEorFALSE(useNA)) {
                  stop("'useNA' must be TRUE or FALSE")
              }
              if (any(is.na(by))) {
                  stop("'by' must not contain NAs")
              }
              groupings <- lapply(by, function(f) as.formula(paste("~", f)))
              aliased <- by %in% names(params(x)$fl)
              facetAlias <- function(fl, grouping) {
                  facetParams(x, fl, ..., grouping=grouping, useNA=useNA,
                              sort=sort, decreasing=decreasing, limit=limit,
                              drop=drop)
              }
              params <- mapply(facetAlias, params(x)$fl[by[aliased]],
                               groupings[aliased], SIMPLIFY=FALSE)
              sort <- facetSort(x, sort, decreasing)
              limit <- facetLimit(limit)
              mincount <- facetMinCount(drop, ...)
              params[by[!aliased]] <-
                  mapply(function(f, grouping) {
                             stats <- statsParams(x, grouping, ...)
                             list(type="terms", field=f,
                                  missing=useNA,
                                  facets=stats, sort=sort,
                                  limit=limit, mincount=mincount)
                         }, by[!aliased], groupings[!aliased],
                         SIMPLIFY=FALSE)
              params
          })

setMethod("facetParams", c("SolrQuery", "Symbol"),
          function(x, by, where, grouping, ...) {
              facetParams(x, as.character(by), ...)[[1L]]
          })

setMethod("facetParams", c("SolrQuery", "call"),
          function(x, by, where, grouping, ..., useNA=FALSE, sort=NULL,
                   decreasing=FALSE, limit=NA_integer_, drop=TRUE) {
### FIXME: Support useNA=TRUE for query facets without extra stats,
### since we have the overall count in the parent bucket.
              if (!identical(useNA, FALSE)) {
                  stop("'useNA' must be FALSE for complex facets")
              }
              if (!is.null(sort)) {
                  stop("'sort' is not supported for complex facets")
              }
              if (!identical(limit, NA_integer_)) {
                  stop("'limit' is not supported for complex facets")
              }
              if (by[[1L]] == quote(cut)) {
                  by[[1L]] <- facet_cut
                  params <- eval(by, where)
              } else {
                  query <- deferTranslation(x, by, SolrQParserExpression(),
                                            where)
                  params <- list(type="query", q=query)
              }
              ## NOTE: Solr ignores mincount on query facet, but we handle later
              c(params, list(facet=statsParams(x, grouping, ...),
                             mincount=facetMinCount(drop, ...)))
          })

setMethod("facetParams", c("SolrQuery", "TranslationRequest"),
          function(x, by, ...) {
              facetParams(x, by@src@expr, where=by@src@env, ...)
          })

`facetParams<-` <- function(x, value) {
    if (outputRestricted(x)) {
        warning("facets will ignore head/tail restriction")
    }
    json(x)$facet <- structure(value, class="facetlist")
    x
}

facetLimit <- function(x) {
    if (!isSingleNumberOrNA(x)) {
        stop("facet limit must be a single number, or NA")
    }
    if (is.na(x)) {
        x <- -1L
    }
    x
}

### FIXME: Sorting facet buckets does not handle NAs very well, since
### Solr is doing the sorting, and it does not treat NAs
### reasonably. Sure, we could sort ourselves, but really the whole
### point of sorting is so that we can tell Solr to truncate the output.

facetSort <- function(x, sort, decreasing) {
    if (!isTRUEorFALSE(decreasing)) {
        stop("'decreasing' must be TRUE or FALSE")
    }
    dir <- if (decreasing) "desc" else "asc"
    if (is(sort, "formula")) {
        f <- parseFormulaRHS(sort)
        paste(vapply(f, function(t) {
                         if (is.name(t)) {
                             paste(t, dir)
                         } else {
                             stop("facet sort term must name a statistic")
                         }
                     }, character(1L)),
              collapse=",")
    } else if (is.null(sort)) {
        paste("index", dir)
    } else {
        stop("invalid facet sort specification")
    }
}

facetMinCount <- function(drop, ...) {
    if (!isTRUEorFALSE(drop)) {
        stop("'drop' must be TRUE or FALSE")
    }
    if (!drop && !missing(...)) {
        stop("'drop' must be TRUE when statistics are requested")
    }
    if (drop) 1L else 0L
}

setGeneric("makeName", function(x) standardGeneric("makeName"))

setMethod("makeName", "TranslationRequest", function(x) {
              makeName(x@src@expr)
          })

setMethod("makeName", "name", function(x) {
              x
          })

setMethod("makeName", "call", function(x) {
              paste0(makeName(x[[2L]]), ".", x[[1L]])
          })

setMethod("makeName", "Promise", function(x) {
              makeName(expr(x))
          })

setMethod("makeName", "SolrAggregateCall", function(x) {
              paste0(makeName(x@subject), ".", x@name)
          })

setMethod("makeName", "SolrFunctionCall", function(x) {
              if (length(x@args) > 1L) {
                  x@args <- x@args[-1L]
                  suffix <- x
              } else {
                  suffix <- x@name
              }
              paste0(makeName(x@args[[1L]]), ".", suffix)
          })

setMethod("makeName", "SolrFunctionExpression", function(x) {
              as.character(x@name)
          })

imputeStatNames <- function(x) {
    needsName <- if (is.null(names(x))) {
        rep(TRUE, length(x))
    } else {
        names(x) == ""
    }
    names(x)[needsName] <- vapply(x[needsName], makeName, character(1L))
    x
}

ensureNamesForJSON <- function(x) {
    if (is.null(names(x))) {
        names(x) <- character(0L)
    }
    x
}

statsParams <- function(x, grouping, ..., .stats=list()) {
    requests <- mapply(deferTranslation,
                       expr=as.list(substitute(list(...))[-1L]),
                       env=top_prenv_dots(...),
                       MoreArgs=list(x=x, target=SolrAggregateCall(),
                           grouping=grouping))
    requests <- imputeStatNames(requests)
    ensureNamesForJSON(c(requests, .stats))
}


cutLabelsToLucene <- function(x) {
  chartr("()", "{}", sub(",", " TO ", sub("-?Inf", "*", x)))
}

### FIXME: no counting of NAs for date faceting
###        - could get closer with facet.range.other

checkSolrFacetBound <- function(x) {
  if (is.numeric(x) || is(x, "POSIXt") || is(x, "Date")) {
    x
  } else {
    stop("unsupported breaks type")
  }
}

checkSolrFacetGap <- function(x) {
  if (is.numeric(x) || is(x, "difftime")) {
    x
  } else {
    stop("unsupported breaks type")
  }
}

unique.difftime <- function(x) {
  ans <- NextMethod()
  attributes(ans) <- attributes(x)
  ans
}

facet_cut <- function(x, breaks, include.lowest = FALSE, right = TRUE) {
  var <- deparse(substitute(x))
  if (length(gap <- unique(diff(breaks))) == 1L) {
    facet.include <- if (right) "upper" else "lower"
    if (include.lowest) {
      facet.include <- paste0(facet.include, ",", "edge")
    }
    list(type="range",
         field=var,
         start=checkSolrFacetBound(min(breaks)),
         end=checkSolrFacetBound(max(breaks)),
         gap=checkSolrFacetGap(gap),
         include=facet.include)
  }
  else {
### TODO: use interval facet here, once it is
### supported by the JSON API. Interval facets are actually so general
### that one could implement counting of genomic overlaps. Especially
### if the JSON API kept the interval syntax terse.
    stop("non-uniform cut() breaks not yet supported")
  }
}

mergeFacetParams <- function(x, params, terms, where, grouping, ...) {
    if (is.null(params)) {
        params <- list()
    }
    term <- terms[[1L]]
    pterm <- params[[deparse(term)]]
    if (length(terms) > 1L) {
        if (is.null(pterm)) {
            pterm <- facetParams(x, stripI(term), where, grouping)
        }
        pterm$facet <- mergeFacetParams(x, pterm$facet, terms[-1L],
                                         where, grouping, ...)
    } else {
        newfacet <- facetParams(x, stripI(term), where, grouping, ...)
        subfacets <- pterm$facet[vapply(pterm$facet, is.list, logical(1L))]
        newfacet$facet <- structure(c(newfacet$facet, subfacets),
                                    class="facetlist")
        newfacet$nfq <- sum(names(params(x)) == "fq")
        pterm <- structure(newfacet, class="facet")
    }
    params[[deparse(term)]] <- pterm
    if (pterm$type == "query") {
        not_pterm <- pterm
        not_pterm$q@src@expr <- call("!", pterm$q@src@expr)
        params[[paste0("_not_", deparse(term))]] <- not_pterm
    }
    params
}

faceted <- function(x) {
    !is.null(json(x)$facet)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Request handler
###

requestHandler <- function(x) params(x)$qt
`requestHandler<-` <- function(x, value) {
    if (!is.null(value) && !isSingleString(value)) {
        stop("request handler must be a single string, or NULL")
    }
    params(x)$qt <- value
    x
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Response format
###

typeToFormat <- c(list = "json", data.frame = "csv", XMLDocument = "xml")

responseType <- function(x) {
  if (is.null(params(x)$wt)) {
    NULL
  } else {
    names(typeToFormat)[match(params(x)$wt, typeToFormat)]
  }
}

`responseType<-` <- function(x, value) {
  if (!isSingleString(value)) {
    stop("'value' must be a single, non-NA string")
  }
  format <- typeToFormat[value]
  if (is.na(format)) {
    stop("no format for response type: ", value)
  }
  params(x)$wt <- format
  if (format == "json") {
    params(x)$json.nl <- "map"
    params(x)$csv.null <- NULL
  } else if (format == "csv") {
    params(x)$json.nl <- NULL
    params(x)$csv.null <- "NA"
  }
  x
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

paramToCSV <- function(x) {
    if (!is.null(names(x)))
        x <- .qualifyByName(x, ":")
    if (length(x) > 0L)
        paste(x, collapse=",")
    else x
}

### FIXME: In R <= 3.2.0, rapply() strips attributes. Thus, we need to
### process the parameters in one pass. Since rapply() is S3 based
### (only considers class attribute), and the different methods expect
### different arguments, it seems appropriate to use an S3 generic.
### We ended up writing our own rapply2(), because we want to apply
### the function to any node, not just the leaves.

translateParam <- function(x, ...) UseMethod("translateParam")

translateParam.TranslationRequest <- function(x, context, ...) {
    ans <- eval(x, context)
    if (identical(ans, solrNA)) NULL else ans
}

translateParam.facet <- function(x, fq, ...) {
    if (!is.null(x$nfq)) {
        x$excludeTags <- tail(fq, length(fq) - x$nfq)
        x$nfq <- NULL
    }
    if (length(x$facet) == 0L) {
        x$facet <- NULL
    }
    x
}

translateParam.facetlist <- function(x, ...) {
    ## This must be a well-known functional construct, does it have a name?
    getChildren <- function(xi) {
        child <- child(xi)
        if (!is.null(child)) {
            c(child, getChildren(child))
        }
    }
    children <- unlist(unname(lapply(x, getChildren)))
    x[vapply(children, hiddenName, character(1L))] <- children
    x
}

getFqTags <- function(x) {
    unname(vapply(x[names(x) == "fq"], tag, character(1L)))
}

translateBoundsParams <- function(p, nrows) {
    ans_start <- 0L
    ans_rows <- .Machine$integer.max

    stopifnot(identical(length(p$start), length(p$rows)))
    
    for (i in seq_along(p$start)) {
        head_minus <- p$rows[i] < 0L
        if (isTRUE(head_minus)) {
            ans_rows <- min(ans_rows, nrows) + p$rows[i]
        } else {
            ans_rows <- min(ans_rows, p$rows[i], na.rm=TRUE)
        }
        tail_plus <- p$start[i] < 0L
        if (tail_plus) {
            ans_start <- ans_start + min(ans_rows, nrows) + p$start[i]
            if (is.na(p$rows[i]))
                ans_rows <- min(ans_rows, abs(p$start[i]))
        } else {
            ans_start <- ans_start + p$start[i]
            if (is.na(p$rows[i]))
                ans_rows <- min(ans_rows, nrows) - p$start[i]
        }
    }
    
    p$start <- ans_start
    p$rows <- ans_rows

    p
}

translateParams <- function(p, core, nrows) {
    p <- translateBoundsParams(p, nrows)
### FIRST TIME EVER USING rapply()!!!! Unfortunately, had to write our own.
    rreplace(p, translateParam,
             c("TranslationRequest", "facet", "facetlist"),
             context=core, fq=getFqTags(p))
}

setMethod("translate", c("SolrQuery", "missing"), function(x, target, core) {
              params(x) <- translateParams(params(x), core,
                                           resultLength(core, x))
              params(x)$fl <- Filter(Negate(is.null), params(x)$fl)
              params(x)$sort <- Filter(Negate(is.null), params(x)$sort)
              x
          })

solrFormat <- function(x, ...) UseMethod("solrFormat")

solrFormat.POSIXt <- function(x, ...) {
    toSolr(x, new("solr.DateField"))
}

solrFormat.Date <- function(x, ...) {
    solrFormat(as.POSIXct(x), ...)
}

solrFormat.difftime <- function(x, ...) {
    paste0("+", x, toupper(attr(x, "units")))
}

solrFormat.default <- function(x, ...) {
    if (is.numeric(x) || is.logical(x)) {
        x
    } else {
        as.character(x)
    }
}

paramsAsCharacter <- function(p) {
    p <- rapply(p, solrFormat, how="replace")
    p$sort <- paramToCSV(p$sort)
    p$fl <- paramToCSV(p$fl)
    if (!is.null(p[["json"]])) # partial match hits 'json.nl'
        p[["json"]] <- toJSON(p[["json"]])
    storage.mode(p) <- "character"
    p
}

setMethod("as.character", "SolrQuery",
          function(x) paramsAsCharacter(params(x)))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Utilities related to the 'fl' parameter
###

### This took some thought. The field restriction overrides the
### previous 'fl' parameter, except where it was adding new fields
### (computed or aliased). Those are kept iff they match the new
### restriction. This treats the field restriction almost like a
### "clip" in computer graphics. It can be reset. However, the
### high-level API requires selections be made from the current clip.
restrictToFields <- function(x, fields) {
    fl <- params(x)$fl
    names <- ifelse(nzchar(names(fl)), names(fl), NA_character_)
    globs <- grepl("*", fields, fixed=TRUE)
    exactMatch <- match(fields[!globs], names)
    globMatchMatrix <- vapply(glob2rx(fields[globs]), grepl, names,
                              FUN.VALUE=logical(length(names)))
    globMatch <- which(rowSums(globMatchMatrix) > 0L)
    params(x)$fl <- c(fl[sort(c(exactMatch, globMatch))],
                      setdiff(fields, names[exactMatch]))
    x
}

flNames <- function(fl) {
    if (is.null(names(fl))) {
        as.character(fl)
    } else {
        ans <- names(fl)
        ans[ans == ""] <- as.character(fl[ans == ""])
        ans
    }
}

flIsPattern <- function(query) {
    fl <- params(query)$fl
    setNames(grepl("*", fl, fixed=TRUE), flNames(fl))
}

sortFieldsByQuery <- function(x, query) {
    isPattern <- flIsPattern(query)
    patterns <- which(isPattern)
    m <- globMatchMatrix(names(patterns), x)
    patterns <- patterns[max.col(m, ties.method="first")]
    matched <- rowSums(m) > 0L
    aliases <- which(!isPattern)
    unique(c(names(aliases), x[matched])[order(c(aliases, patterns[matched]))])
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

sortForShow <- function(x) {
    requests <- vapply(x, is, logical(1L), "TranslationRequest")
    decreasing <- vapply(x[requests],
                         function(s) s@target@decreasing, logical(1L))
    str <- paste0("`", vapply(x, as.character, character(1L)), "`")
    str[requests] <- paste0(":", str[requests], ifelse(decreasing, "v", "^"))
    str
}

groupForShow <- function(x) {
    c(x[names(x) == "group.field"],
      paste0("`", lapply(x[names(x) == "group.func"], as.character), "`"))
}

facetForShow <- function(x) {
    if (is(x, "TranslationRequest"))
        return(as.character(x))
    if (x$type == "query") {
        as.character(x$q)
    } else if (x$type == "range") {
        deparse(substitute(cut(field, seq(start, end, gap)), x))
    } else {
        x$field
    }
}

facetsForShow <- function(x) {
    vapply(x, facetForShow, character(1L))
}

showLine <- function(...) {
    cat(labeledLine(...))
}

setMethod("show", "SolrQuery", function(object) {
              cat("SolrQuery object\n")
              p <- params(object)
              fq <- p[names(p) == "fq"]
              if (length(fq) > 0L)
                  showLine("subset", paste0("`", lapply(fq, as.character), "`"))
              if (!identical(p$fl, list("*")))
                  showLine("select", p$fl)
              if (!is.null(p$sort))
                  showLine("sort", sortForShow(p$sort))
              if (grouped(object))
                  showLine("group", groupForShow(p))
              if (faceted(object))
                  showLine("facet", facetsForShow(json(object)$facet))
              if (!identical(p$start, 0L))
                  showLine("start", p$start[-1L])
              if (!identical(p$rows, .Machine$integer.max))
                  showLine("rows", p$rows[-1L])
          })
