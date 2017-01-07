### =========================================================================
### SolrSummary objects
### -------------------------------------------------------------------------
###
### Just for storing and printing the summary of a SolrFrame. Very
### specific assumptions are made about the content of "facets", so
### this is not for direct construction.
###

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Classes
###

setClass("SolrSummary",
         representation(facets="Facets",
                        colnames="character",
                        digits="integer",
                        dropped="logical"),
         prototype=list(dropped=FALSE))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructors
###

SolrSummary <- function(facets, colnames, digits)
{
    new("SolrSummary", facets=facets, colnames=colnames, digits=digits)
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setMethod("facets", "SolrSummary", function(x) x@facets)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

transposeStats <- function(x) {
    qtl <- as.vector(x$quantile)
    names(qtl) <- c("1st qu.", "median", "3rd qu.")
    x$quantile <- NULL
    x <- c(x, qtl)
    x <- x[c("min", "1st qu.", "median", "mean", "3rd qu.", "max", "NA's")]
    data.frame(stat=names(x), value=unlist(x, use.names=FALSE))
}

formatColumnSummary <- function(x, maxlen, digits) {
    ans <- paste0(format(x[[1L]]), ":",
                  c(head(format(x[[2L]], digits=digits), -1L),
                    tail(x[[2L]], 1L)),
                  "  ")
    c(ans, rep("", maxlen - length(ans)))
}

setMethod("drop", "SolrSummary", function(x) {
    x@dropped <- TRUE
    x
})

formatVectorSummary <- function(x, digits) {
    m <- signif(x[[2L]], digits)
    names(m) <- x[[1L]]
    class(m) <- c("summaryDefault", "table")
    m
}

formatFrameSummary <- function(x, digits) {
    maxlen <- max(vapply(x, nrow, integer(1L)))
    m <- do.call(cbind,
                 lapply(x, formatColumnSummary, maxlen,
                        digits))
    rownames(m) <- rep("", nrow(m))
    np <- pmax(nchar(m[1,]) - nchar(colnames(m)), 0L) / 2L
    padding <- vapply(np, function(len) {
        paste(character(len + 1L), collapse=" ")
    }, character(1L))
    colnames(m) <- paste0(padding, colnames(m))
    as.table(m)
}

setMethod("as.table", "SolrSummary", function(x) {
              s <- stats(facets(x))
              s$count <- NULL
              dotpos <- regexpr("\\.[^.]*$", colnames(s))
              statnames <- substring(colnames(s), dotpos+1L)
              varnames <- substring(colnames(s), 1L, dotpos-1L)
              colnames(s) <- statnames
              sv <- lapply(split(as.list(s), varnames), transposeStats)
              tabs <- c(sv, lapply(facets(x), stats))[x@colnames]
              if (length(tabs) == 1L && x@dropped) {
                  formatVectorSummary(tabs[[1L]], x@digits)
              } else {
                  formatFrameSummary(tabs, x@digits)
              }
          })

as.table.SolrSummary <- function(x, ...) as.table(x)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "SolrSummary", function(object) {
              print(as.table(object))
          })
