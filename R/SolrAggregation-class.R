### =========================================================================
### SolrAggregation objects
### -------------------------------------------------------------------------
### 
### Represents an aggregation operation through a formula and a 'Solr'
### object. Methods should be defined on summary generics that query
### Solr and extract the result.
###

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Class
###

setClass("SolrAggregation",
         representation(formula="formula",
                        solr="Solr"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

formula.SolrAggregation <- function(x) x@formula

solr <- function(x) x@solr

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

SolrAggregation <- function(formula, solr) {
  new("SolrAggregation", formula=formula, solr=solr)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Statistics
###

SolrStatsAggregator <- function(STAT) {
  STAT.name <- deparse(substitute(STAT))
  function(x, na.rm=FALSE) {
    s <- facets(stats(core(solr(x)),
                      stats(query(solr(x)), formula(x)))[[1L]])[[1L]]
    df <- setNames(data.frame(levels(s)), field(s))
    lhs <- as.character(formula(x)[[2L]])
    df[[lhs]] <- STAT(s, na.rm=na.rm)
    df
  }
}

setMethod("min", "SolrAggregation", SolrStatsAggregator(min))
setMethod("max", "SolrAggregation", SolrStatsAggregator(max))
setMethod("sum", "SolrAggregation", SolrStatsAggregator(sum))
setMethod("count", "SolrAggregation", SolrStatsAggregator(count)) # custom
setMethod("mean", "SolrAggregation", SolrStatsAggregator(mean))
setMethod("sd", "SolrAggregation", SolrStatsAggregator(sd))
setMethod("var", "SolrAggregation", function(x, y=NULL, na.rm=FALSE, use) {
  if (!is.null(y)) {
    stop("'y' must be NULL")
  }
  if (!missing(use)) {
    warning("'use' is ignored")
  }
  ans <- sd(x, na.rm=na.rm)
  ans[[length(ans)]] <- ans[[length(ans)]]^2
  ans
})
setMethod("length", "SolrAggregation", function(x) count(x))
setMethod("nrow", "SolrAggregation", function(x) {
  as.data.frame(xtabs(formula(x), solr(x)))
})

setMethod("window", "SolrAggregation",
          function(x, start = 1L, end = .Machine$integer.max) {
            query <- groups(query(solr(x)), formula(x),
                            offset=start-1L, limit=end-start+1L)
            g <- groups(core(solr(x)), query)[[1L]]
            gu <- unlist(g, recursive=FALSE, use.names=FALSE)
            df <- cbind(ind = rep(names(g), elementLengths(g)),
                        restfulr:::raggedListToDF(gu, stringsAsFactors=FALSE))
            byname <- as.character(tail(formula(x), 1L)[[1L]])
            df[[byname]] <- NULL
            names(df)[1L] <- byname
            df <- df[order(df[[1L]]),]
            rownames(df) <- NULL
            df
          })

setMethod("head", "SolrAggregation", function(x, n = 6L) {
  if (!isSingleNumber(n))
    stop("'n' must be a single, non-NA number")
  window(x, end = n)
})

setMethod("tail", "SolrAggregation", function (x, n = 6L) {
  if (!isSingleNumber(n))
    stop("'n' must be a single, non-NA number")
  window(x, start = -n + 1L)
})

setMethod("unique", "SolrAggregation", function (x, n) {
  if (length(formula(x)) != 3L) {
    stop("formula must have an LHS and RHS")
  }
  rhs <- formula(x)[[3L]]
  lhs <- formula(x)[[2L]]
  f <- as.formula(call("~", call("+", rhs, lhs)))
  ans <- subset(as.data.frame(xtabs(f, solr(x))), Freq > 0L, select=-Freq)
  ans[order(ans[[1L]]),]
})

## TODO using Solr 5.x Analytics Component:
## - quantile/median
