### =========================================================================
### SolrQuery objects
### -------------------------------------------------------------------------

### Represents a query to a Solr search engine.

### We want to take a lazy approach to query evaluation. The
### alternative would be specifying a Solr query (which can be quite
### complex) in a single call. The current "best practice" for complex
### commands is to use a parameter object. For us, the parameter
### object is a query, and the query is evaluated by a Solr core.

### Problem with the lazy query evaluation approach:
### - Error checking is difficult until the end (the "ggplot2" problem)

### Ignoring the problems, it would be easy to represent a query
### object, a Solr core, and submission of queries to the core. But
### should we use as the API? It could be explicity Solr, which would
### be easy, but it is a new API for users. An alternative would be to
### map Solr syntax to R syntax.

### Query component:
## subset => &fq, &fl (via 'select=')
## transform => &fl aliasing
## sort(x, by = "rating") => &sort
## window/head/tail => &start/&rows (tail needs number of documents first)

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
## aggregate(price ~ cat, x, FUN) => stats.field=price, stats.facet=cat
## : FUN %in% min,max,sum,length=>count,mean,sd=>stddev,var=>sd^2

### Function query mappings (used in any expression?):
## '+'=>sum, '-'=>sub, '*'=>product, '/'=>div, '%%'=>mod, '^'=>pow,
## abs, log10=log, sqrt, rescale=>scale, pmax=>max, pmin=>min,
## log=>Math.ln, exp/sin/cos/tan/asin/acos/atan/sinh/cosh/tanh=>Math.*,
## ceiling=>Math.ceil, floor=>Math.floor, round=>Math.rint, pi/e=>Math.*
## exists=>exists, ifelse=>if, !=>not, &=>and, |=>or, dist=>dist (euc/manhat)

### The concern is that users might infer behavior that is difficult
### to support with Solr. A good example is the order of operations:

## x <- transform(x, sqrtScore = sqrt(score))
## subset(x, sqrtScore > 10)

### Since 'sqrtScore' is defined by &fl, it is not subject to &fq
### filtering (it is never indexed). In other words, we are building a
### query, not actually manipulating a query result, and there are
### constraints that need to be documented.

setClass("SolrQuery", representation(params = "list", drop = "logical",
                                     nrow = "logical"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

SolrQuery <- function(...) {
  params <- list(q = "*:*", start = 0L, rows = .Machine$integer.max)
  args <- list(...)
  params[names(args)] <- args
  new("SolrQuery", params = params)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Low-level parameter access
###

setMethod("$", "SolrQuery", function(x, name) {
  x@params[[name]]
})

setMethod("$<-", "SolrQuery", function(x, name, value) {
  x@params[[name]] <- value
  x
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Simple row counting
###

setMethod("nrow", "SolrQuery", function(x) {
  x$rows <- 0L
  x@nrow <- TRUE
  x
})

isRowCount <- function(x) x@nrow

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr query component
###

### Not supporting '[' yet, because there is no server-side mechanism

setMethod("subset", "SolrQuery", function(x, subset, select, drop = FALSE) {
  if (!isTRUEorFALSE(drop))
    stop("'drop' must be TRUE or FALSE")
  if (!missing(subset)) {
    if (!identical(x$q, "*:*"))
      x <- initialize(x, params = c(x@params, fq = x$q))
    vals <- Filter(Negate(is.null),
                   mget(all.names(subset), parent.frame(2), ifnotfound = NULL))
    expr <- substitute(subset, vals)
    x$q <- translateToLuceneQuery(expr)
  }
  if (!missing(select)) {
    nl <- as.list(seq(length(x)))
    ### FIXME: would be nice to have the list of field names
    names(nl) <- names(x)
    ints <- eval(substitute(select), nl, parent.frame(2))
    x$fl <- intersect(x$fl, names(x)[ints])
  }
  x@drop <- x@drop || drop
  x
})

setMethod("transform", "SolrQuery", function (`_data`, ...) {
  e <- as.list(substitute(list(...))[-1L])
  if (length(e) == 0L)
    return(`_data`)
  if (is.null(names(e)))
    names(e) <- ""
  solr <- as.character(lapply(e, translateToSolr))
  fl <- csv(ifelse(nchar(names(e)) > 0L, paste0(names(e), ":", solr), solr))
  append(`_data`, list(fl = fl))
})

setMethod("sort", "SolrQuery", function (x, decreasing = FALSE, by = "score") {
  if (!isTRUEorFALSE(decreasing))
    stop("'decreasing must be TRUE or FALSE")
  if (is.language(by))
    by <- translateToSolrFunction(by)
  if (!is.character(by) || any(is.na(by)))
    stop("'by' must be character without NA's")
  direction <- if (decreasing) "desc" else "asc"
  sort <- paste(by, direction)
  x$sort <- csv(c(sort, x$sort))
  x
})

setMethod("rev", "SolrQuery", function (x) {
  if (is.null(x$sort))
    x <- sort(x)
  else {
    terms <- strsplit(x$sort, ",")[[1]]
    x$sort <- csv(gsub(" tmp$", " desc",
                       gsub(" desc$", " asc",
                            gsub(" asc$", " tmp", terms))))
  }
  x
})

setMethod("window", "SolrQuery", function (x, start = x$start, end = NULL) {
  if (!isSingleNumber(start))
    stop("'start' must be a single, non-NA number")
  if (!is.null(end) && !isSingleNumber(end))
    stop("'end' must be a single, non-NA number")
  offset <- start - 1L
  len <- NULL
  if (!is.null(end))
    len <- end - start + 1L
  if (isFaceted(x)) {
    x$facet.offset <- max(offset, x$facet.offset)
    x$facet.limit <- min(len, x$facet.limit)
  } else {
    x$start <- max(offset, x$start)
    x$rows <- min(len, x$rows)
  }
  x
})

setMethod("head", "SolrQuery", function (x, n) {
  if (!isSingleNumber(n))
    stop("'n' must be a single, non-NA number")
  window(x, end = n)
})

setMethod("tail", "SolrQuery", function (x, n) {
  if (!isSingleNumber(n))
    stop("'n' must be a single, non-NA number")
  window(x, start = -n + 1L)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Facet component
###

### If we switch to faceting, then we need to fix rows=0.
### That means that start, sort, fl, etc no longer have any effect.
### We need to issue a warning if they are present.

### Remember to set facet.limit to a large value.

isFaceted <- function(x) {
  any(grepl("^facet\\.", names(x)))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Stats component
###

isAggregated <- function(x) {
  any(grepl("^stats\\.", names(x)))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Translation
###

### FIXME: 'dist' is not a valid mapping; R is all pairwise, Solr is vectorized
### FIXME: 'exists' might be better mapped from '!is.na' but the ! complicates

callTranslation <- list(
  '+' = "sum", '-' = "sub", '*' = "product", '/' = "div", '%%' = "mod",
  '^' = "pow", abs = "abs", log10 = "log", sqrt = "sqrt", rescale = "scale",
  pmax = "max", pmin = "min", log = "Math.ln", exp = "Math.exp",
  sin = "Math.sin", cos = "Math.cos", tan = "Math.tan", asin = "Math.asin",
  acos = "Math.acos", atan = "Math.atan", sinh = "Math.sinh",
  cosh = "Math.cosh", tanh = "Math.tanh", ceiling = "Math.ceil",
  floor = "Math.floor", round = "Math.rint", exists = "exists",
  ifelse = "if", '!' = "not", '&' = "and", '|' = "or", dist = "dist"
)

## not sure if we want this yet, what if there is a field 'pi'?
nameTranslation <- list(
  pi = "Math.pi"
)

translateToSolrFunction <- function(x) {
  if (is.call(x)) {
    call.name <- as.character(x[[1]])
    solr <- callTranslation[[call.name]]
    if (is.null(solr))
      stop("Unable to map function '", call.name, "' to a Solr function")
    paste0(solr, "(",
           paste(lapply(x[-1L], translateToSolrFunction), collapse = ","),
           ")")
  } else if (is.character(x)) {
    paste0('"', x, '"')
  } else {
    as.character(x)
  }
}

## Rules:
##   '==' => symbol:value
##   '!=' => -symbol:value
##   '>' => symbol:{value TO *]
##   '>=' => symbol:[value TO *]
##   '<' => symbol:[* TO value}
##   '<=' => symbol:{* TO value]
##   '|' =>
##   '&' => [2] AND [3]
##   '!' => -[2]
##   '(' => ([2])

## Wildcard and fuzzy searches are expected to be in ""
 
translateToLuceneQuery <- function(x) {
  if (is.call(x)) {
    call <- normComparisonCall(x)
    call.name <- call[[1L]]
    args <- call[-1L]
    switch(call.name,
           "==" = paste0(args[[1]], ":", args[[2]]),
           "!=" = paste0("-", args[[1]], ":", args[[2]]),
           ">" = paste0(args[[1L]], ":", paste0("{", args[[2L]], " TO *]")),
           ">=" = paste0(args[[1L]], ":", paste0("[", args[[2L]], " TO *]")),
           "<" = paste0(args[[1L]], ":", paste0("[* TO ", args[[2L]], "}")),
           "<=" = paste0(args[[1L]], ":", paste0("[* TO ", args[[2L]], "]")),
           "(" = paste0("(", translateToLuceneQuery(args), ")"),
           "|" = paste(args[[1]], args[[2]]),
           "&" = paste0("+", args[[1]], " +", args[[2]]),
           "!" = paste0("-", args[[1]])
           )
  } else if (is.name(x)) {
    as.character(x)
  }
}

normComparisonCall <- function(x) {
  if (length(x) == 3L) {
    args <- x[-1L]
    sym.ind <- which(sapply(args, is.name))
    if (length(sym.ind) > 1L) {
      stop("arguments in comparison must contain one symbol, and one literal: ",
           csv(sapply(args, deparse)))
    }
    if (sym.ind == 3L) {
      x <- call(chartr("<>", "><", x[[1L]]), x[[3L]], x[[2L]])
    }
  } else {
    x
  }
}

## fieldValuePair <- function(x) {
##   paste0(x[[1L]], ":", deparse(x[[2L]]))
## }

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Evaluation
###

### The actual translation is to a Solr REST request, not (just) a
### Lucene query. We need to support e.g. column-wise subsetting
### ("fl"), faceting, paginated results (show/head/tail),
### etc. Multiple filters might be sent as "fq" parameters, so that
### each filter is cached sepatately. Any arithmetic needs to use
### sum/sub/product/etc. The 'stats' component is especially useful,
### as it yields the aggregate sum, min/max, mean, sd, etc.

setMethod("eval", c("SolrQuery", "SolrCore"),
          function (expr, envir, enclos)
          {
            params <- prepareQueryParams(envir, expr)
            response <- do.call(read, c(envir, params))
            ## some SOLR instances return text/plain for JSON...
            if (params$wt == "json" && is.character(response))
              response <- fromJSON(response)
            if (isFaceted(expr)) {
              ## return a table
            } else if (isAggregated(expr)) {
              ## return a data.frame like aggregate()
            } else if (isRowCount(expr)) {
              ans <- response$response$numFound
            } else {
              if (expr@drop && ncol(df) == 1L)
                ans <- response[[1]]
            }
            ans
          })

prepareQueryParams <- function(x, query) {
  if (query$rows < 0L)
    query$rows <- nrow(x) + query$rows
  if (query$start < 0L)
    query$start <- nrow(x) + query$start
  if (isFaceted(query) || isAggregated(query) || isRowCount(query)) {
    query$wt <- "json"
    query$json.nl <- "map"
  } else {
    query$wt <- "csv"
    query$csv.null <- "NA"
  }
  lapply(query@params, csv)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "SolrQuery", function(object) {
  cat("SolrQuery object\n")
  cat(paste(mapply(BiocGenerics:::labeledLine, names(object@params),
                   object@params, count = FALSE),
            collapse = ""))
})
