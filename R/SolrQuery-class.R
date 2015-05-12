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
             fl    = "*")
           ))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

SolrQuery <- function() {
    new("SolrQuery")
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
    params(x)$json
}

`json<-` <- function(x, value) {
    params(x)$json <- value
    x
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### SolrQueryTranslationSource
###
### The intent is to preserve the state when the translation was requested
###

setClass("SolrQueryTranslationSource",
         representation(expr="language",
                        query="SolrQuery",
                        env="environment"),
         contains="Expression")

setMethod("translate", c("SolrQueryTranslationSource", "ANY"),
          function(x, target, core, ...) {
              context <- DelegateContext(.SolrFrame(x@query, core), x@env)
              translate(x@expr, target, context, ...)
          })

deferTranslation <- function(x, expr, target, env) {
    expr <- preprocessExpression(expr, env)
    src <- new("SolrQueryTranslationSource", expr=expr, query=x, env=env)
    new("TranslationRequest", src=src, target=target)
}

translateParams <- function(x, context) {
### FIRST TIME EVER USING rapply()!!!!
    params(x) <- rapply(params(x), eval, "TranslationRequest", how="replace",
                        envir=context)
    x
}

setExcludeTags <- function(x, context) {
### SECOND TIME EVER USING rapply()!!!!
    fq <- names(params(x)$fl)
    params(x) <- rapply(params(x), function(xi) {
                            xi$excludeTags <- tail(fq, -xi$nfq)
                            xi$nfq <- NULL
                            xi
                        }, "facet", how="replace")
    x
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr query component
###

setMethod("subset", "SolrQuery",
          function(x, subset, select, fields, select.from = character()) {
  if (!missing(subset)) {
    fq <- deferTranslation(x, substitute(subset), SolrLuceneExpression(),
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
  fl <- mapply(deferTranslation, list(x), e, list(SolrFunctionExpression()),
               top_prenv_dots(...))
  ## If a simple symbol is passed, Solr will rename the field, rather
  ## than copy the column. That is different from transform()
  ## behavior.  Sure, aliasing is sort of pointless server-side, but
  ## we do not want to surprise the user.
  aliased <- vapply(e, is.name, logical(1L))
  identity.aliases <- unique(lapply(e[aliased], as, "SolrFunctionExpression"))
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

parseSortFormula <- function(x) {
  if (length(x) != 2L) {
    stop("formula must not have an LHS")
  }
  lapply(attr(terms(x), "variables")[-1L], stripI)
}

sortParams <- function(x, by, decreasing) {
    lapply(parseSortFormula(by), deferTranslation, x=x,
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
  configure(x, group="true", group.limit=limit, group.offset=offset,
            group.sort=params(x)$sort, ...)
}

setGeneric("groups", function(x, by, ...) standardGeneric("groups"))

setMethod("groups", c("SolrQuery", "language"),
          function(x, by, limit = .Machine$integer.max, offset = 0L,
                   env = emptyenv()) {
    func <- deferTranslation(x, by, SolrFunctionExpression(), env)
    configureGroups(x, limit, offset, group.func=func)
})

setMethod("groups", c("SolrQuery", "name"),
          function(x, by, limit = .Machine$integer.max, offset = 0L, env) {
  groups(x, by=as.character(by), limit=limit, offset=offset)
})

setMethod("groups", c("SolrQuery", "character"),
          function(x, by, limit = .Machine$integer.max, offset = 0L) {
  if (!isSingleString(by)) {
    stop("'by' must be a single, non-NA string")
  }
  group.field <- setNames(by, rep("group.field", length(by)))
  configureGroups(x, limit, offset, group.field)
})

setMethod("groups", c("SolrQuery", "formula"), function(x, by, ...) {
  hasLHS <- length(by) == 3L
  if (hasLHS) {
    x <- subset(x, fields=as.character(by[[2L]]))
    by[[2L]] <- NULL
  }
  exprs <- parseSortFormula(by)
  if (length(exprs) > 1L) {
    stop("'by' must be a formula with a single term")
  }
  groups(x, exprs[[1L]], env=attr(by, ".Environment"), ...)
})

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

facet <- function(x, ...) {
  if (outputRestricted(x)) {
    warning("facets will ignore head/tail restriction")
  }
  facetParams(x) <- facetParams(x, ...)
  x
}

setGeneric("facetParams", function(x, by=list(), ...)
    standardGeneric("facetParams"))

setMethod("facetParams", c("SolrQuery", "list"), function(x, by, ...) {
              facetParams_json(x, json(x)$facet, by, ...)
          })

setMethod("facetParams", c("SolrQuery", "name"), function(x, by, ...) {
              facetParams(x, as.character(by), ...)
          })

setMethod("facetParams", c("SolrQuery", "formula"), function(x, by, ...) {
              factors <- attr(terms(by), "variables")[-1L]
              facetParams(factors, where=attr(by, ".Environment"))
          })

setMethod("facetParams", c("SolrQuery", "call"),
          function(x, by, where, sort=NULL, decreasing=FALSE,
                   limit=NA_integer_, ...) {
              by <- stripI(by)
              if (by[[1L]] == quote(cut)) {
                  by[[1L]] <- quote(facet_cut)
                  json <- eval(by, where)
              } else {
                  query <- deferTranslation(x, by, SolrLuceneExpression(),
                                            where)
                  json <- list(type="query", q=query)
              }
              c(json, list(facet=statsParams(...),
                           sort=facetSort(x, sort, decreasing),
                           limit=facetLimit(limit)))
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
              stats <- statsParams(x, ...)
              sort <- facetSort(x, sort, decreasing)
              limit <- facetLimit(limit)
              json <- lapply(by, function(f) {
                                 list(type="terms", field=f, missing=useNA,
                                      facet=stats, sort=sort, limit=limit)
                             })
              names(json) <- by
              json
          })

`facetParams<-` <- function(x, value) {
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
        f <- parseSortFormula(sort)
        paste(vapply(f, function(t) {
                         if (is.name(t)) {
                             paste(t, dir)
                         } else {
                             stop("facet sort term must be a simple name")
                         }
                     }, character(1L)),
              collapse=",")
    } else if (is.null(sort)) {
        paste("index", dir)
    } else {
        stop("invalid facet sort specification")
    }
}

imputeStatNames <- function(x) {
    needsName <- if (is.null(names(x))) {
        TRUE
    } else {
        names(x) == ""
    }
    nameRequest <- function(r) paste0(r@src[[2L]], ".", r@src[[1L]])
    names(x)[needsName] <- lapply(x[needsName], nameRequest)
    x
}

statsParams <- function(x, ..., .stats=list()) {
    stats <- as.list(substitute(list(...))[-1L])
    requests <- mapply(deferTranslation, list(x), stats,
                       list(SolrAggregateExpression()),
                       top_prenv_dots(...))
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
### TODO: use interval facet instead of facet query here, once it is
### supported by the JSON API. Interval facets are actually so general
### that one could implement counting of genomic overlaps. Especially
### if the JSON API kept the interval syntax terse.
    dummy.cut <- cut(integer(), breaks, include.lowest=include.lowest,
                     right=right)
    tokens <- cutLabelsToLucene(levels(dummy.cut))
    facet.query <- setNames(paste0(var, ":", tokens), levels(dummy.cut))
    list(type="query", q=facet.query)
  }
}

facetParams_json <- function(x, params, terms, ...) {
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
    params
}

enablesFacet <- function(x) {
    !is.null(params(x)$json.facet)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Stats component
###
### This is somewhat redundant with facets() now that it supports
### stats. But this conveys the high-level notion of computing stats
### on *fields* rather that computing arbitrarily named stats on
### arbitrary expressions.
###
### Of course, we could use standardized names like .[var].mean and
### attempt to parse those. Then we could remove this.

enablesStats <- function(x) {
  identical(params(x)$stats, "true")
}

setMethod("stats", c("SolrQuery", "character"), function(x, which) {
  if (any(is.na(which))) {
    stop("'which' must be character without NAs")
  }
  params(x) <- c(params(x), stats="true",
                 setNames(which, rep("stats.field", length(which))))
  x
})

### FIXME: not clear if we should still support this. It relies on the
### now deprecated "stats.facet" parameter. Current recommendation is
### to introduce a facet.pivot and link to it via a "tag". This is
### undesirable, since it introduces a facet.pivot into the results,
### and we have already moved to using the JSON facet API, instead of
### the limited facet.pivot stuff.
setMethod("stats", c("SolrQuery", "formula"), function(x, which) {
  f <- parseStatsFormula(which)
  x <- stats(x, f$fields)
  if (!is.null(f$facet)) {
    params(x) <- c(params(x),
                   setNames(rep(f$facet, length(f$fields)),
                            paste0("f.", f$fields, ".stats.facet")))
  }
  x
})

parseStatsFormula <- function(x) {
  vars <- attr(terms(x, allowDotAsName=TRUE), "variables")
  if (length(x) != 3L || length(vars) != 3L) {
    stop("'formula' must contain exactly one term on each side")
  }
  lhs <- vars[[2L]]
  rhs <- vars[[3L]]
  if (is.call(lhs)) {
    if (lhs[[1L]] == quote(c)) {
      lhs <- lhs[-1L]
    } else {
      stop("formula LHS must be a single name or names combined with c()")
    }
  }
  fields <- as.character(lhs)
  if (!is.name(rhs)) {
    stop("formula RHS must be a single symbol")
  }
  if (rhs == quote(.)) {
    facet <- NULL
  } else {
    facet <- as.character(rhs)
  }
  list(fields=fields, facet=facet)
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
  aliased <- nzchar(names(x))
  x[aliased] <- paste0(names(x)[aliased], ":", x[aliased])
  if (length(x) > 0L)
    paste(x, collapse=",")
  else x
}

setMethod("as.character", "SolrQuery", function(x, ...) {
  p <- params(x)
  p$fl <- list(paramToCSV(p$fl))
  p$start <- list(paramToCSV(p$start))
  p$rows <- list(paramToCSV(p$rows))
  param.names <- rep(names(p), elementLengths(unlist(p, recursive=FALSE)))
  setNames(unlist(p, use.names=FALSE), param.names)
})

setAs("SolrQuery", "character", function(from) as.character.SolrQuery(from))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "SolrQuery", function(object) {
  cat("SolrQuery object\n")
  char <- as.character(object)
  cat(BiocGenerics:::labeledLine("params", paste0(names(char), "='", char, "'"),
                                 ellipsisPos = "start"))
})
