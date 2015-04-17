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

### Ignoring the problems, it would be easy to represent a query
### object, a Solr core, and submission of queries to the core. But
### should we use as the API? It could be explicity Solr, which would
### be easy, but it is a new API for users. An alternative would be to
### map Solr syntax to R syntax.

### Query component:
## subset => &fq
## transform => &fl aliasing
## sort(x, by = "rating") => &sort
## window/head/tail => &start/&rows

### Facet component:
## xtabs(~field, x) => facet.field=field, facet.limit=-1, &fq (via 'subset=')
## xtabs(~score + price, x) => facet.pivot=score,price
## head(xtabs(~field, x), n) => facet.limit=n
## window(xtabs(~field, x), i, j) => facet.offset=i, facet.limit=j-i+1
## xtabs(..., drop.unused.levels=TRUE) => facet.mincount=1
## xtabs(..., exclude=NULL) => facet.missing=true
## xtabs(~cut(d, seq(i,j,b)) => facet.range=d,
##                              f.d.facet.range.start=i/end=j/gap=b,
##                              f.d.facet.range.include=upper (lower if right=F)
##                              f.d.facet.range.include=edge (if include.lowest)

### Stats component(stats=true):
## xtabs(price ~ cat, x) => stats.field=price, stats.facet=cat, get sum
### Supported on SolrCore:
## aggregate(price ~ cat, x, FUN) => stats.field=price, stats.facet=cat
## : FUN %in% min,max,sum,length=>count,mean,sd=>stddev,var=>sd^2

### Grouping component (group=true):
### head() by group:
## groups(x, "field", limit=1L) 

### Future Analytics component (Solr 5.0):
### https://issues.apache.org/jira/browse/SOLR-5302
### Will probably make the stats and facet components obsolete for our
### needs. The group component might only be useful for finding top
### documents in each group.

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

setClass("SolrQuery",
         representation(params = "list"),
         prototype = list(
           params = list(
             q     = "*:*",
             start = 0L,
             rows  = .Machine$integer.max,
             fl    = "*") # was list("*")
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
### Low-level parameter access
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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr query component
###

setMethod("subset", "SolrQuery",
          function(x, subset, select, fields,
                   translation.target = SolrQParserExpression(),
                   select.from = character())
{
  if (!missing(subset)) {
    expr <- eval(call("bquote", substitute(subset), top_prenv(subset)))
    query <- translate(expr, translation.target, top_prenv(subset))
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

translateToSolrFunction <- function(expr, env) {
  as.character(translate(expr, SolrFunctionExpression(), env=env))
}

transform.SolrQuery <- function(`_data`, ...) transform(`_data`, ...)

### CHECKME: what happens when an alias already exists?
setMethod("transform", "SolrQuery", function (`_data`, ...) {
  e <- as.list(substitute(list(...))[-1L])
  if (length(e) == 0L)
    return(`_data`)
  if (is.null(names(e)))
    names(e) <- ""
  solr <- mapply(translateToSolrFunction, e, top_prenv_dots(...))
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

### Also exists in S4Vectors!
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
                 env=attr(by, ".Environment"))
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
  configureGroups(x, limit, offset, group.func=translateToSolrFunction(by, env))
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


setMethod("xtabs", "SolrQuery",
          function(formula, data,
                   subset, sparse = FALSE, 
                   na.action, exclude = NA,
                   drop.unused.levels = FALSE)
          {
            if (!all(names(match.call())[-1L] %in%
                     c("formula", "data", "exclude"))) {
              stop("all args except 'formula', 'data' and 'exclude' are ignored")
            }
            if (!is.na(exclude) && !is.null(exclude)) {
              stop("'exclude' must be 'NA' or 'NULL'")
            }
            if (!is(formula, "formula")) {
              stop("'formula' must be a formula")
            }
            facet(data, formula, useNA=is.null(exclude))
          })

### CHECKME: how is sort affected when we set f.foo.facet.limit?
setFacetParams <- function(x, params) {
  x <- configure(x, rows=0L, facet="true", facet.limit=-1L, facet.sort="index")
  params(x) <- c(params(x), params)
  x
}

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

setGeneric("facet", function(x, by, ...) standardGeneric("facet"))

setMethod("facet", c("SolrQuery", "formula"),
          function(x, by, ...) {
            exprs <- attr(terms(by), "variables")[-1L]
            if (length(exprs) > 1L) {
              facetPivot(x, exprs, ...)
            } else {
              facet(x, exprs[[1]], ..., where=attr(by, ".Environment"))
            }
          })

setMethod("facet", c("SolrQuery", "name"),
          function(x, by, where=parent.frame(2), ...) {
            facet(x, as.character(by), ...)
          })

stripI <- function(x) {
  if (is.call(x) && x[[1L]] == quote(I))
    x[[2L]]
  else x
}

callToFacetParams <- function(x, by, where) {
  by <- eval(call("bquote", stripI(by), where=where))
  p <- eval(by, prepareFacetEnv(where))
  if (!is.list(p)) {
    stop("'", by, "' must evaluate to list of Solr facet parameters")
  }
  p
}

setMethod("facet", c("SolrQuery", "call"),
          function(x, by, useNA=FALSE, where=parent.frame(2)) {
            if (!identical(useNA, FALSE)) {
              stop("'useNA' must be FALSE for a facet query")
            }
            facet.params <- callToFacetParams(x, by, where)
            setFacetParams(x, facet.params)
          })

setMethod("facet", c("SolrQuery", "character"),
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
            setFacetParams(x, facet.params)
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

prepareFacetEnv <- function(env) {
  ans <- list(cut=facet_cut)
  ans[c(">", ">=", "<", "<=", "==", "!=", "(", "&", "|", "!", "%in%")] <-
    list(function(e1, e2) {
      this.call <- sys.call()
      facet.query <- translate(this.call, SolrQParserExpression(), env)
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
### Analytics
###

### Solr 5.1 introduced an awesome aggregation framework.

### What we need to do:

### - Rewrite faceting to use the JSON request format
###   We can use http://wiki.apache.org/solr/SystemInformationRequestHandlers
###   to determine the version; then pass some sort of format parameter
###   to SolrQuery, i.e., "json" for the new stuff.
### - facets() needs to support list of function calls for computing
###   statistics.
### - aggregate,Solr() needs to support list of functions, or arbitrarily
###   named, quoted expressions.
### - support up-front split(x, y? ~ x), followed by aggregate calls
###   that do not require a formula...
###   - could that GroupedSolr object have a sort method? sure...

### Available statistics:
### sum => sum
### avg => mean
### sumsq ~> var, sd
### min/max => min/max
### unique => countUnique? nunique?
### percentile => quantile, median

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
