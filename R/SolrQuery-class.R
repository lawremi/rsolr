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

### There are many different types of queries, including:
### - Documents: the actual data
### - Table: counts for each unique value in a field, or matching a query
### - Statistics: min/max/mean/sd/etc for an entire field, or each unique value
### - Row count

### Calling certain methods on a query object will generate a
### particular type of query. But how is the query evaluated? There
### could be a general eval,SolrQuery,SolrCore, or there could be
### high-level methods on SolrCore that accept a query. Like
### [,SolrCore would accept a document query. This is probably more
### readable than eval(), because it should be clear to the reader
### what is being returned. Thus, we will provide high-level methods
### and have the eval method implement the low-level REST request.

### Problem with the lazy query evaluation approach:
### - Error checking is difficult until the end (the "ggplot2" problem)

### Ignoring the problems, it would be easy to represent a query
### object, a Solr core, and submission of queries to the core. But
### should we use as the API? It could be explicity Solr, which would
### be easy, but it is a new API for users. An alternative would be to
### map Solr syntax to R syntax.

### Query component:
## subset => &q, &fq, &fl (via 'select=')
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

### TODO: consider switching this to take a single argument, 'expr',
###       that would be passed to subset().
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

params <- function(x) x@params

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

setMethod("subset", "SolrQuery", function(x, subset, fields, drop = FALSE) {
  if (!isTRUEorFALSE(drop))
    stop("'drop' must be TRUE or FALSE")
  if (!missing(subset)) {
    if (!identical(x$q, "*:*"))
      x <- initialize(x, params = c(x@params, fq = x$q))
    expr <- eval(call("bquote", substitute(subset), top_prenv(subset)))
    x$q <- toLucene(expr, top_prenv(subset))
  }
### NOTE: Would be nice to support a column expression for 'select' like
###       subset.data.frame, but this style of column selection would
###       not work in general, due to tricky things like dynamic
###       fields. Bottom-line: Solr stores documents, not tables, so
###       there is no ordering of the fields.
  ## nl <- as.list(seq(length(x)))
  ## names(nl) <- names(x)
  ## ints <- eval(substitute(select), nl, top_prenv(subset))
  ## x$fl <- intersect(x$fl, names(x)[ints])
### So for now we use the argument name 'fields'
### and just assume it is a character vector of field names:
  if (!missing(fields)) {
    x$fl <- intersect(x$fl, fields)
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

postFilterParams <- c("start", "sort", "fl")

setGeneric("facet", function(formula, data, ...) standardGeneric("facet"))

### TODO:
### - head(), tail() should use facet.limit/facet.offset PER facet.field
### - sort() should use facet.sort='count' PER facet.field
### GOTCHA: those will not work with facet.query, so we should warn

setMethod("facet", c("formula", "SolrQuery"),
          function(formula, data) {
            if (data$start == 0L) {
              data$start <- NULL
            }
            pf.params <- intersect(names(data@params), postFilterParams)
            if (length(pf.params) > 0L) {
              warning("post filter parameters (",
                      paste(pf.params, collapse=", "),
                      ") are ignored by faceting")
            }
            env <- attr(formula, ".Environment")
            facetFunctions <- prepareFacetFunctions(env)
            facet.params <- unlist(lapply(formula[-1], function(term) {
              term <- eval(call("bquote", term, where=env))
              if (is.name(term)) {
                list(facet.field=as.character(term))
              } else {
                p <- eval(term, facetFunctions, env)
                if (!is.list(p)) {
                  stop("formula term '", term,
                       "' must evaluate to list of Solr facet parameters")
                }
                p
              }
            }))
            data@params <- c(data@params, facet.params)
            data$rows <- 0L
            data$facet.limit <- -1L
            data$facet.sort <- "index"
            data$facet.missing <- TRUE
            data$facet <- TRUE
            data
          })

### In a facet formula, the terms are translated into either a
### facet.field (if a name) or a list of facet parameters (a
### call). Any symbols that should be resolved in the caller should be
### escaped with .() like bquote(). If a term is a call, the call
### should evaluate to a list of facet terms. By default, 'cut' and
### the relational/logical operators are overridden to generate terms.

prepareFacetFunctions <- function(env) {
  ans <- list(cut=facet_cut)
  ans[c(">", ">=", "<", "<=", "==", "!=", "(", "&", "|", "!", "%in%")] <-
    list(function(e1, e2) {
      list(facet.query=toLucene(sys.call(), env))
    })
  ans
}

cutLabelsToLucene <- function(x) {
  chartr("()", "{}", sub(",", " TO ", sub("[-]Inf", "*", x)))
}

### FIXME: no counting of NAs when xtabs(exclude=NULL)
###        - could get closer with facet.range.other
facet_cut <- function(x, breaks, include.lowest = FALSE, right = TRUE) {
  var <- substitute(x)
  if (length(gap <- unique(diff(breaks))) == 1L) {
    facet.include <- if (right) "upper" else "lower"
    if (include.lowest) {
      facet.include <- paste0(facet.include, ",", "edge")
    }
    params <- list(facet.var.start=min(breaks), facet.var.end=max(breaks),
                   facet.var.gap=gap, facet.var.include=facet.include)
    names(params) <- sub("var", var, names(params))
    c(facet.range=var, params)
  }
  else {
    dummy.cut <- cut(integer(), breaks, include.lowest=include.lowest,
                     right=right)
    tokens <- cutLabelsToLucene(levels(dummy.cut))
    list(facet.query=paste0(var, ":", tokens))
  }
}

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

### To represent a relational operation in Lucene, we need one symbol
### and one literal. Logical operations can happen between two
### sub-queries.

toLucene <- function(x, env) {
  .toLucene <- function(x) {
    query <- if (is.call(x)) {
      call <- normComparisonCall(x, env)
      call.name <- call[[1L]]
      args <- call[-1L]
      switch(as.character(call.name),
             "==" = paste0(args[[1]], ":", args[[2]]),
             "!=" = paste0("-", args[[1]], ":", args[[2]]),
             "%in%" = paste0(localParams(q.op="OR", df=args[[1]]),
               paste0("(", paste(args[[2]], collapse=" "), ")")),
             ">" = paste0(args[[1L]], ":", paste0("{", args[[2L]], " TO *]")),
             ">=" = paste0(args[[1L]], ":", paste0("[", args[[2L]], " TO *]")),
             "<" = paste0(args[[1L]], ":", paste0("[* TO ", args[[2L]], "}")),
             "<=" = paste0(args[[1L]], ":", paste0("[* TO ", args[[2L]], "]")),
             "(" = paste0("(", .toLucene(args), ")"),
             "|" = paste0("(", .toLucene(args[[1]]), " ",
               .toLucene(args[[2]]), ")"),
             "&" = paste0("(+", .toLucene(args[[1]]), " +",
               .toLucene(args[[2]]), ")"),
             "!" = paste0("-", .toLucene(args[[1]])),
             eval(x, env)
             )
    } else if (is.name(x)) {
      paste0(x, ":true")
    } else {
      x
    }
    query <- as.character(query)
    if (!isSingleString(query)) {
      stop("'", x, "' did not evaluate to a single, non-NA string")
    }
    query
  }
  .toLucene(x)
}

isRelational <- function(x) {
  as.character(x[[1]]) %in% c("==", "!=", ">", ">=", "<", "<=", "%in%")
}

isLogical <- function(x) {
  as.character(x[[1]]) %in% c("&", "|", "!")
}

normComparisonCall <- function(x, env) {
  args <- x[-1L]
  if (isRelational(x)) {
    call.arg <- vapply(args, is.call, logical(1))
    args[call.arg] <- lapply(args[call.arg], eval, env)
    name.arg <- vapply(args, is.name, logical(1))
    name.ind <- which(name.arg)
    if (length(name.ind) != 1L) {
      stop("arguments in comparison must contain one symbol, ",
           "and one non-symbol: ", csv(sapply(args, deparse)))
    }
    x[-1L] <- args
    if (name.ind == 3L) {
      if (x[[1]] == quote(`%in%`)) {
        stop("invalid %in% syntax")
      }
      x <- call(chartr("<>", "><", x[[1L]]), x[[3L]], x[[2L]])
    }
  } else if (isLogical(x)) {
    if (any(!vapply(args, is.language, logical(1)))) {
      stop("literal arguments are not allowed for logical operators")
    }
  }
  x
}

## fieldValuePair <- function(x) {
##   paste0(x[[1L]], ":", deparse(x[[2L]]))
## }

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "SolrQuery", function(object) {
  cat("SolrQuery object\n")
  cat(paste(mapply(BiocGenerics:::labeledLine, names(object@params),
                   object@params, count = FALSE),
            collapse = ""))
})
