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

### Support for Solr extensions
## Custom functions: functions returning literal/name/Expression in custom env
## Custom query parsers: via Expression framework
## Custom request handlers: use low-level restfulr API
## Custom search components: use low-level parameter API
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
### the query, i.e., we just substitute the expression. But there are
### still problems:
### * output restrictions, like head/tail, will not affect e.g. aggregation
###   - not a typical use case, but it's inconsistent with R
### * using the result of aggregation in queries and transforms,
###   where we would need to evaluate the summaries first (where we
###   have access to a core), and substitute the values...
###   - we could even support predicates, like:
###     subset(x, y > mean(z[j > 10]))
###     because the j>10 is a simple filter on the aggregation request

setClass("SolrQuery",
         representation(params="list",
                        queryTarget="SolrLuceneExpression",
                        functionTarget="SolrFunctionExpression",
                        aggregateTarget="SolrAggregateExpression"),
         prototype = list(
           params = list(
             q     = "*:*",
             start = 0L,
             rows  = .Machine$integer.max,
             fl    = "*")
           ))

setClass("SolrQuery5.1", contains="SolrQuery")

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

SolrQuery <- function(version="5.1") {
    version <- as.package_version(version)
    new(paste0("SolrQuery", if (version >= "5.1") "5.1" else ""))
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

setMethod("version", "SolrQuery", function(x) {
              as.package_version(sub("^SolrQuery", "", class(x)))
          })

### These enable the overriding of translation behavior

queryTarget <- function(x) x@queryTarget
functionTarget <- function(x) x@functionTarget
aggregateTarget <- function(x) x@aggregateTarget

`queryTarget<-` <- function(x, value) {
    x@queryTarget <- value
    x
}
`functionTarget<-` <- function(x, value) {
    x@functionTarget <- value
    x
}
`aggregateTarget<-` <- function(x, value) {
    x@aggregateTarget <- value
    x
}


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr query component
###

setMethod("subset", "SolrQuery",
          function(x, subset, select, fields, select.from = character())
{
  if (!missing(subset)) {
    expr <- eval(call("bquote", substitute(subset), top_prenv(subset)))
    query <- translate(expr, queryTarget(x), top_prenv(subset))
    params(x) <- c(params(x), fq = as.character(query))
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
### restriction.
restrictToFields <- function(x, fields) {
  fl <- params(x)$fl
  names <- ifelse(nzchar(names(fl)), names(fl), NA_character_)
  matches <- vapply(glob2rx(fields), grepl, logical(length(names)), names)
  params(x)$fl <- c(fl[rowSums(matches) > 0L], fields)
  x
}

translateToSolrFunction <- function(x, expr, env) {
  as.character(translate(expr, functionTarget(x), env=env))
}

transform.SolrQuery <- function(`_data`, ...) transform(`_data`, ...)

setMethod("transform", "SolrQuery", function (`_data`, ...) {
  e <- as.list(substitute(list(...))[-1L])
  if (length(e) == 0L)
    return(`_data`)
  if (is.null(names(e)))
    names(e) <- ""
  solr <- mapply(translateToSolrFunction, list(x), e, top_prenv_dots(...))
  ## If a simple symbol is passed, Solr will rename the field, rather
  ## than copy the column. That is different from transform()
  ## behavior.  Sure, aliasing is sort of pointless server-side, but
  ## we do not want to surprise the user.
  aliased <- vapply(e, is.name, logical(1L))
  identity.aliases <- unique(as.character(e[aliased]))
  params(`_data`)$fl <- c(params(`_data`)$fl, setNames(solr, names(e)),
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
              params(x)$fl <- c(params(x)$fl, map)
              x
          })

parseSortFormula <- function(x) {
  if (length(x) != 2L) {
    stop("formula must not have an LHS")
  }
  lapply(attr(terms(x), "variables")[-1L], stripI)
}

setMethod("sort", "SolrQuery", function (x, decreasing = FALSE, by = ~ score) {
  if (!isTRUEorFALSE(decreasing))
    stop("'decreasing must be TRUE or FALSE")
  if (enablesFacet(x)) {
    if (!decreasing)
      stop("'decreasing' must be TRUE when sorting a facet query")
    if (!missing(by))
      stop("'by' should be missing when sorting a facet query (by count)")
    x <- sortFacet(x)
  } else {
    direction <- if (decreasing) "desc" else "asc"
    by <- vapply(parseSortFormula(by), translateToSolrFunction, character(1),
                 x=x, env=attr(by, ".Environment"))
    sort <- paste(by, direction)
    params(x)$sort <- csv(c(sort, params(x)$sort))
  }
  x
})

sortFacet <- function(x) {
  if (!is.null(params(x)$facet.query) || !is.null(params(x)$facet.pivot)) {
    stop("sort() does not support complex faceting")
  }
  fields <- params(x)[names(params(x)) == "facet.field"]
  if (length(fields) > 0L) {
    params(x)[paste0("f.", fields, ".facet.sort")] <- "count"
  }
  x
}

rev.SolrQuery <- function (x) {
  if (is.null(params(x)$sort))
    x <- sort(x)
  else {
    terms <- strsplit(params(x)$sort, ",")[[1]]
    params(x)$sort <- csv(gsub(" tmp$", " desc",
                               gsub(" desc$", " asc",
                                    gsub(" asc$", " tmp", terms))))
  }
  x
}

setMethod("rev", "SolrQuery", rev.SolrQuery)

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
  if (enablesFacet(x)) {
    params(x) <- windowFacetParams(params(x), start, rows)
  } else {
    params(x) <- boundsParams(params(x), start, rows)
  }
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

windowFacetParams <- function(p, offset, limit) {
  if (!is.null(p$facet.query) || !is.null(plapply$facet.pivot)) {
    stop("window() does not support complex faceting")
  }
  if (!isTRUE(limit >= 0L)) {
    stop("limit/end must be specified in facet bound restriction")
  }
  
  fields <- p[names(p) == "facet.field"]
  if (length(fields > 0L)) {
    prefix <- paste0("f.", fields, ".facet")
    p[paste0(prefix, ".offset")] <- pmax(p[paste0(prefix, ".offset")], offset)
    p[paste0(prefix, ".limit")] <- pmin(p[paste0(prefix, ".limit")], limit)
  }
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
                   env = emptyenv())
{
    configureGroups(x, limit, offset,
                    group.func=translateToSolrFunction(x, by, env))
})

setMethod("groups", c("SolrQuery", "name"),
          function(x, by, limit = .Machine$integer.max, offset = 0L, env)
{
  groups(x, by=as.character(by), limit=limit, offset=offset)
})

setMethod("groups", c("SolrQuery", "character"),
          function(x, by, limit = .Machine$integer.max, offset = 0L)
{
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


### Solr 5.1 introduced an awesome aggregation framework.

### What we need to do:

### - Rewrite faceting to use the JSON request format
###   We can use http://wiki.apache.org/solr/SystemInformationRequestHandlers
###   to determine the version; then pass some sort of format parameter
###   to SolrQuery, i.e., "json" for the new stuff.
### - facets() needs to support list of function calls for computing
###   statistics.
### - aggregate,Solr() needs to be improved, so that we support:
###     - the LHS of the formula could support the "select" syntax,
###     - a named list of functions, all of which are applied, and their name
###       serves as the postfix,
###     - named, quoted arguments adding stats as columns,
### - support up-front split(x, y? ~ x), followed by aggregate calls
###   that do not require a formula...
###   - could that GroupedSolr object have a sort method? sure...
###   - could it have a transform method that lazily computes summaries
###     (for the known aggregation functions), but otherwise behaves like
###     transform,Solr? Yes, but besides the fast aggregation,
###     the actual transforation would need to happen on the client,
###     where we have the aggregated values, so that would be slow and
###     somewhat complicated...
### - transform improvements:
###   - could support each(select, funs), where each function in
###     'funs' is applied to each column in 'select'.
###   - could support a formula that splits (and selects columns)

### Available statistics:
### sum => sum
### avg => mean
### sumsq ~> var, sd
### min/max => min/max
### unique => countUnique? nunique?
### percentile => quantile, median

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

facetPivot <- function(x, exprs, useNA=FALSE) {
  strs <- as.character(exprs)
  nms <- vapply(exprs, is.name, logical(1L))
  if (!all(nms)) {
    stop("if more than one term, all terms must be names (",
         paste(strs[!nms], collapse=", "), ")")
  }
  facet.params <- list(facet.pivot=paste(strs, collapse=","),
                       facet.pivot.mincount=0L)
  if (useNA) {
    facet.params[paste0("f.", strs, ".facet.missing")] <- "true"
  }
  setFacetParams(x, facet.params)
}

facet <- function(x, ...) {
  facetParams(x) <- facetParams(x, ...)
  x
}

setGeneric("facetParams", function(x, by, ...) standardGeneric("facetParams"))

### TODO: handle SolrQuery5.1,missing case ==> overall stats

setMethod("facetParams", c("SolrQuery", "name"), function(x, by, ...) {
              facetParams(x, as.character(by), ...)
          })

setMethod("facetParams", c("SolrQuery", "formula"), function(x, by, ...) {
              exprs <- attr(terms(by), "variables")[-1L]
              if (length(exprs) > 1L) {
                  facetPivot(x, exprs, ...)
              } else {
                  facet(x, exprs[[1]], ..., where=attr(by, ".Environment"))
              }
          })

setMethod("facetParams", c("SolrQuery5.1", "formula"), function(x, by, ...) {
              terms <- attr(terms(by), "variables")[-1L]
              facetParams_json(x, json(x)$facet, terms,
                               where=attr(by, ".Environment"),
                               ...)
          })

setMethod("facetParams", c("SolrQuery", "call"), function(x, by, where) {
              by <- eval(call("bquote", stripI(by), where=where))
              p <- eval(by, prepareFacetEnv(x, where))
              if (!is.list(p)) {
                  stop("'", by,
                       "' must evaluate to list of Solr facet parameters")
              }
              p
          })

setMethod("facetParams", c("SolrQuery", "character"),
          function(x, by, useNA=FALSE) {
            if (!isTRUEorFALSE(useNA)) {
              stop("'useNA' must be TRUE or FALSE")
            }
            if (any(is.na(by))) {
              stop("'by' must not contain NAs")
            }
            facet.params <- setNames(by, rep("facet.field", length(by)))
            if (useNA) {
              facet.params[paste0("f.", by, ".facet.missing")] <- "true"
            }
            facet.params
          })

statsParams <- function(x, ...) {
  e <- as.list(substitute(list(...))[-1L])
  if (is.null(names(e)) || any(names(e) == "")) {
    stop("statistic arguments must be named")
  }
  mapply(translateToSolrAggregate, list(x), e, top_prenv_dots(...))
}

## stop being a search engine!
### CHECKME: maybe we can set these as top-level parameters, and the JSON
### would inherit them? Otherwise, head/tail on facets will be hard...
.facetConstants <- list(limit=-1L, sort="index")

setMethod("facetParams", c("SolrQuery5.1", "character"),
          function(x, by, useNA=FALSE, ...) {
              if (!isTRUEorFALSE(useNA)) {
                  stop("'useNA' must be TRUE or FALSE")
              }
              if (any(is.na(by))) {
                  stop("'by' must not contain NAs")
              }
              stats <- statsParams(x, ...)
              json <- lapply(by, function(f) {
                                 list(terms=c(list(field=f, missing=useNA),
                                          stats, .facetConstants))
                             })
              names(json) <- by
              json
          })

setMethod("facetParams", c("SolrQuery5.1", "name"),
          function(x, by, ...) {
              callNextMethod()[[1L]]
          })

setMethod("facetParams", c("SolrQuery5.1", "call"),
          function(x, by, where, ...) {
              constants <- facetConstants()
              params <- callNextMethod(x, by, where)
              range <- params$facet.range
              if (!is.null(range)) {
                  json <- list(field=range)
                  subkeys <- c("start", "end", "gap", "include")
                  keys <- paste0("f.", range, ".facet.range.", subkeys)
                  json[subkeys] <- params[keys]
                  type <- "range"
              } else {
                  json <- list(q=params$facet.query)
                  json <- c(json, common)
                  type <- "query"
              }
              ans <- list()
              ans[[type]] <- c(json, statsParams(x, ...), .facetConstants)
### 5.2? ans <- c(type=type, json, statsParams(...), .facetConstants)
              ans
          })

setGeneric("facetParams<-",
           function(x, ..., value) standardGeneric("facetParams<-"),
           signature="x")

setReplaceMethod("facetParams", "SolrQuery", function(x, value) {
                     x <- configure(x, rows=0L, facet="true", facet.limit=-1L,
                                    facet.sort="index")
                     params(x) <- c(params(x), value)
                     x
                 })

setReplaceMethod("facetParams", "SolrQuery5.1", function(x, value) {
                     params(x)$rows <- 0L
                     json(x)$facet <- value
                     x                     
                 })


### In a facet formula, the terms are translated into either a
### facet.field (if a name) or a list of facet parameters (a
### call). Any symbols that should be resolved in the caller should be
### escaped with .() like bquote(). If a term is a call, the call
### should evaluate to a list of facet terms. By default, 'cut' and
### the relational/logical operators are overridden to generate terms.

### FIXME: When we see a call, we assume the output is logical and
### generate a facet.query. This will break when the output should be
### treated as a factor. Such a case might arise in an expression
### generating integer values (we cannot handle strings/factors
### anyway). Using group.func might be a work-around, but it seems
### like an abuse? and we should just wait for the analytics
### component.

prepareFacetEnv <- function(x, env) {
  ans <- list(cut=facet_cut)
  ans[c(">", ">=", "<", "<=", "==", "!=", "(", "&", "|", "!", "%in%")] <-
    list(function(e1, e2) {
      this.call <- sys.call()
      facet.query <- translate(this.call, queryTarget(x), env)
      list(facet.query=setNames(list(as.character(facet.query)),
             deparse(sys.call())))
    })
  list2env(ans, parent=env)
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
    params <- list(f.var.facet.range.start=toSolrFacetBound(min(breaks)),
                   f.var.facet.range.end=toSolrFacetBound(max(breaks)),
                   f.var.facet.range.gap=toSolrFacetGap(gap),
                   f.var.facet.range.include=facet.include)
    names(params) <- sub("var", var, names(params))
    c(facet.range=var, params)
  }
  else {
    dummy.cut <- cut(integer(), breaks, include.lowest=include.lowest,
                     right=right)
    tokens <- cutLabelsToLucene(levels(dummy.cut))
    facet.query <- setNames(paste0(var, ":", tokens), levels(dummy.cut))
    list(facet.query=setNames(list(facet.query), as.character(var)))
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
        pterm <- facetParams(x, term, ...)
    }
    params[[deparse(term)]] <- pterm
    params
}

enablesFacet <- function(x) {
  identical(params(x)$facet, "true")
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Stats component
###
### NOTE: obsolete as of Solr 5.1, but we will keep it for 4.x instances

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
### Summary (combination of facets and stats)
###

summary.SolrQuery <- function(object, ...) summary(object, ...)

setMethod("summary", "SolrQuery",
          function(object, schema,
                   of=setdiff(staticFieldNames(schema), uniqueKey(schema)))
          {
            if (missing(schema)) {
              stop("'schema' cannot be missing")
            }
            if (is(schema, "SolrCore")) {
              schema <- schema(schema)
            }
            if (!is(schema, "SolrSchema")) {
              stop("'schema' must be a SolrCore or SolrSchema")
            }
            types <- fieldTypes(schema, of)
            num <- vapply(types, is, logical(1), "NumericField")
            facet(stats(object, of[num]), of[!num])
          })

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
