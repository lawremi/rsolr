### =========================================================================
### SolrParamList objects
### -------------------------------------------------------------------------
### The Expression that is evaluated within a SolrCore to produce a SolrResult
###
### There are no enforced constraints on the list structure, but it is
### important to understand the serialization algorithm. The main
### coercion is to a named character vector, where the names
### correspond to the parameter names in the URL query part. Names can
### be duplicated. Except for the special case of 'fl', which is
### collapsed to a comma-separated string, each parameter is unlisted,
### and the top-level name is applied to each element of the unlsited
### form. The underlying REST interface actually generates the URL
### string from the character vector.
###

setClass("SolrParamList",
         prototype = prototype(
             list(
                 q     = "*:*",
                 start = 0L,
                 rows  = .Machine$integer.max,
                 fl    = "*")
             ),
         contains = c("list", "Expression"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Response format
###

typeToFormat <- c(list = "json", data.frame = "csv", XMLDocument = "xml")

responseType <- function(x) {
    if (is.null(x$wt)) {
        NULL
    } else {
        names(typeToFormat)[match(x$wt, typeToFormat)]
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
    x$wt <- format
    if (format == "json") {
        x$json.nl <- "map"
        x$csv.null <- NULL
    } else if (format == "csv") {
        x$json.nl <- NULL
        x$csv.null <- "NA"
    }
    x
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Serialization
###

paramToCSV <- function(x) {
    aliased <- nzchar(names(x))
    x[aliased] <- paste0(names(x)[aliased], ":", x[aliased])
    if (length(x) > 0L)
        paste(x, collapse=",")
    else x
}

setMethod("as.character", "SolrParamsList", function(x, ...) {
              x$fl <- list(paramToCSV(x$fl))
### 'start' and 'rows' are resolved length 1 before being sent to Solr
              x$start <- list(paramToCSV(x$start))
              x$rows <- list(paramToCSV(x$rows))
              param.names <- rep(names(x), lengths(x))
              setNames(as.character(unlist(x, use.names=FALSE)), param.names)
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "SolrParamsList", function(object) {
              cat("SolrParams object\n")
              char <- as.character(object)
              cat(BiocGenerics:::labeledLine(names(char), char,
                                             ellipsisPos="start",
                                             vectorized=TRUE))
          })
