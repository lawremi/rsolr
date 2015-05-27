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

SolrQuery <- function(expr) {
    ans <- new("SolrQuery")
    if (!missing(expr)) {
        ans <- eval(substitute(subset(ans, expr)), parent.frame())
    }
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

setClass("SolrQueryTranslationSource",
         representation(expr="ANY",
                        query="SolrQuery",
                        env="environment",
                        grouping="formulaORNULL"),
         contains="Expression")

setMethod("translate", c("SolrQueryTranslationSource", "Expression"),
          function(x, target, core, ...) {
              frame <- group(.SolrFrame(core, x@query), x@grouping)
              context <- DelegateContext(frame, x@env)
              translate(x@expr, target, context, ...)
          })

setMethod("as.character", "SolrQueryTranslationSource",
          function(x) deparse(x@expr))

deferTranslation <- function(x, expr, target, env, grouping=NULL) {
    expr <- preprocessExpression(expr, env)
    src <- new("SolrQueryTranslationSource", expr=expr, query=x, env=env,
               grouping=grouping)
    new("TranslationRequest", src=src, target=target)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr query component
###

setMethod("subset", "SolrQuery",
          function(x, subset, select, fields, select.from = character()) {
  if (!missing(subset)) {
    fq <- deferTranslation(x, substitute(subset), SolrQParserExpression(),
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

### This took some thought. The field restriction overrides the
### previous 'fl' parameter, except where it was adding new fields
### (computed or aliased). Those are kept iff they match the new
### restriction. This treats the field restriction almost like a
### "clip" in computer graphics. It can be reset. However, the
### high-level API requires selections be made from the current clip.
restrictToFields <- function(x, fields) {
  fl <- params(x)$fl
  names <- ifelse(nzchar(names(fl)), names(fl), NA_character_)
  matches <- vapply(glob2rx(fields), grepl, logical(length(names)), names)
  params(x)$fl <- c(fl[rowSums(matches) > 0L], fields)
  x
}

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

sortParams <- function(x, by, decreasing) {
    lapply(parseFormulaRHS(by), deferTranslation, x=x,
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
  group(x, exprs[[1L]], env=attr(by, ".Environment"), ...)
})

grouped <- function(x) identical(params(x)$group, "true")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Facet component
###

setMethod("xtabs", "SolrQuery",
          function(formula, data,
                   subset, sparse = FALSE, 
                   na.action, exclude = NA,
                   drop.unused.levels = FALSE)
          {
            if (!all(names(match.call())[-1L] %in%
                     c("formula", "data", "exclude"))) {
              stop("all args except 'formula', 'data' and 'exclude' ",
                   "are ignored")
            }
            if (!is.na(exclude) && !is.null(exclude)) {
              stop("'exclude' must be 'NA' or 'NULL'")
            }
            if (!is(formula, "formula")) {
              stop("'formula' must be a formula")
            }
            facet(data, formula, useNA=is.null(exclude))
          })

setGeneric("facet", function(x, by = NULL, ...) standardGeneric("facet"))

setMethod("facet", c("SolrQuery", "NULL"), function(x, by, ...) {
              stats <- statParams(x, by, ...)
              facetParams(x)[names(stats)] <- stats
              x
          })

setMethod("facet", c("SolrQuery", "formula"), function(x, by, ...) {
              factors <- attr(terms(by), "variables")[-1L]
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
          function(x, by, useNA=FALSE, sort=NULL,
                   decreasing=FALSE, limit=NA_integer_, ...) {
              if (!isTRUEorFALSE(useNA)) {
                  stop("'useNA' must be TRUE or FALSE")
              }
              if (any(is.na(by))) {
                  stop("'by' must not contain NAs")
              }
              sort <- facetSort(x, sort, decreasing)
              limit <- facetLimit(limit)
              aliased <- by %in% names(params(x)$fl)
              params <- lapply(params(x)$fl[by[aliased]], facetParams)
              params[by[!aliased]] <-
                  lapply(by[!aliased], function(f) {
                             grouping <- as.formula(paste("~", f))
                             stats <- statsParams(x, grouping, ...)
                             list(type="terms", field=f,
                                  missing=useNA,
                                  facet=stats, sort=sort,
                                  limit=limit, mincount=0L)
                         })
              params
          })

setMethod("facetParams", c("SolrQuery", "name"),
          function(x, by, ...) {
              facetParams(x, as.character(by))[[1L]]
          })

setMethod("facetParams", c("SolrQuery", "call"),
          function(x, by, where, useNA=FALSE, ...) {
              if (!identical(useNA, FALSE)) {
                  stop("'useNA' must be FALSE for complex facets")
              }
              by <- stripI(by)
              if (by[[1L]] == quote(cut)) {
                  by[[1L]] <- quote(facet_cut)
                  params <- eval(by, where)
              } else {
                  query <- deferTranslation(x, by, SolrQParserExpression(),
                                            where)
                  params <- list(type="query", q=query)
              }
              c(params, list(facet=statsParams(...)))
          })

setMethod("facetParams", c("SolrQuery", "TranslationRequest"),
          function(x, by, ...) {
              facetParams(x, by@src@expr, ...)
          })

`facetParams<-` <- function(x, value) {
    if (outputRestricted(x)) {
        warning("facets will ignore head/tail restriction")
    }
    json(x)$facet <- value
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

facetSort <- function(x, sort, decreasing) {
    if (!isTRUEorFALSE(decreasing)) {
        stop("'decreasing' must be TRUE or FALSE")
    }
    dir <- if (decreasing) "desc" else "asc"
    if (is.formula(sort)) {
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

setGeneric("makeName", function(x) standardGeneric("makeName"))

setMethod("makeName", "TranslationRequest", function(x) {
              makeName(x@src@expr)
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

setMethod("makeName", "SolrFunctionExpression", function(x) {
              if (length(x@args) > 1L) {
                  x@args <- x@args[-1L]
                  suffix <- x
              } else {
                  suffix <- x@name
              }
              paste0(makeName(x@args[[1L]]), ".", suffix)
          })

imputeStatNames <- function(x) {
    needsName <- if (is.null(names(x))) {
        TRUE
    } else {
        names(x) == ""
    }
    names(x)[needsName] <- lapply(x[needsName], makeName)
    x
}

statsParams <- function(x, grouping, ..., .stats=list()) {
    stats <- as.list(substitute(list(...))[-1L])
    requests <- mapply(deferTranslation, list(x), stats,
                       list(SolrAggregateExpression()),
                       top_prenv_dots(...),
                       grouping=grouping)
    requests <- imputeStatNames(requests)
    c(requests, .stats)
}


cutLabelsToLucene <- function(x) {
  chartr("()", "{}", sub(",", " TO ", sub("-?Inf", "*", x)))
}

### FIXME: no counting of NAs when xtabs(exclude=NULL)
###        - could get closer with facet.range.other

toSolrFacetBound <- function(x) {
  if (is.numeric(x)) {
    x
  } else if (is(x, "POSIXt") || is(x, "Date")) {
    toSolr(x, new("solr.DateField"))
  } else {
    stop("unsupported breaks type")
  }
}

toSolrFacetGap <- function(x) {
  if (is.numeric(x)) {
    x
  } else if (is(x, "difftime")) {
    paste0("+", x, toupper(attr(x, "units")))
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
  var <- substitute(x)
  if (length(gap <- unique(diff(breaks))) == 1L) {
    facet.include <- if (right) "upper" else "lower"
    if (include.lowest) {
      facet.include <- paste0(facet.include, ",", "edge")
    }
    list(type="range",
         start=toSolrFacetBound(min(breaks)),
         end=toSolrFacetBound(max(breaks)),
         gap=toSolrFacetGap(gap),
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

mergeFacetParams <- function(x, params, terms, ...) {
    if (is.null(params)) {
        params <- list()
    }
    term <- terms[[1L]]
    pterm <- params[[deparse(term)]]
    if (length(terms) > 1L) {
        if (is.null(pterm)) {
            pterm <- facetParams(x, term)
        }
        pterm$facet <- facetParams_json(x, params$facet, terms[-1L], ...)
    } else {
        newfacet <- facetParams(x, term, ...)
        subfacets <- pterm$facet[vapply(pterm$facet, is.list, logical(1L))]
        newfacet$facet <- structure(c(newfacet$facet, subfacets),
                                    class="facetlist")
        newfacet$nfq <- sum(names(params(x)) == "fq")
        pterm <- structure(newfacet, class="facet")
    }
    params[[deparse(term)]] <- pterm
    if (pterm$type == "query") {
        not_pterm <- pterm
        not_pterm$query@src <- call("!", pterm$query@src)
        params[[paste0("_not_", deparse(term))]] <- not_pterm
    }
    params
}

faceted <- function(x) {
    !is.null(json(x)$facet)
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
        x <- BiocGenerics:::qualifyByName(x, ":")
    if (length(x) > 0L)
        paste(x, collapse=",")
    else x
}

translateParams <- function(x, context) {
### FIRST TIME EVER USING rapply()!!!!
    params(x) <- rapply(params(x), eval, "TranslationRequest", how="replace",
                        envir=context)
    x
}

prepareExcludeTags <- function(x) {
### SECOND TIME EVER USING rapply()!!!!
    fq <- names(params(x)$fl)
    params(x) <- rapply(params(x), function(xi) {
                            xi$excludeTags <- tail(fq, -xi$nfq)
                            xi$nfq <- NULL
                            xi
                        }, "facet", how="replace")
    x
}

getAuxExpr <- function(x) {
    if (is(x, "SolrAggregateExpression"))
        lapply(x@aux, expr)
}

addAuxStats <- function(x, context) {
### THIRD TIME EVER USING rapply()!!!!
    params(x) <- rapply(params(x), function(xi) {
                            aux <- unlist(unname(lapply(xi, getAuxExpr)))
                            xi[names(aux)] <- aux
                            xi
                        }, "facetlist", how="replace")
    x
}

prepareBoundsParams <- function(p, nrows) {
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

paramsAsCharacter <- function(p) {
    exprClasses <- unique(names(getClass("SolrExpression")@subclasses))
    p <- rapply(p, as.character, exprClasses, how="replace")
    p$sort <- paramToCSV(p$sort)
    p$fl <- paramToCSV(p$fl)
    if (!is.null(p[["json"]])) # partial match hits 'json.nl'
        p[["json"]] <- toJSON(p[["json"]])
    storage.mode(p) <- "character"
    p
}

setMethod("translate", c("SolrQuery", "missing"), function(x, target, core) {
              params(x) <- prepareBoundsParams(params(x), resultLength(core, x))
              if (is.null(responseType(x)))
                  responseType(x) <- "list"
              x <- translateParams(x, core)
              x <- prepareExcludeTags(x)
              x <- addAuxStats(x)
              paramsAsCharacter(params(x))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

sortForShow <- function(x) {
    decreasing <- vapply(x, function(s) s@target@decreasing, logical(1L))
    paste0("`", lapply(x, as.character), "`:", ifelse(decreasing, "v", "^"))
}

groupForShow <- function(x) {
    c(x[names(x) == "group.field"],
      paste0("`", lapply(x[names(x) == "group.func"], as.character), "`"))
}

facetForShow <- function(x) {
    if (x$type == "query") {
        as.character(x$q)
    } else if (type == "range") {
        deparse(substitute(cut(field, seq(start, end, gap)), x))
    } else {
        x$field
    }
}

facetsForShow <- function(x) {
    rapply(x, facetForShow, "facet")
}

showLine <- function(...) {
    cat(BiocGenerics:::labeledLine(...))
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
