### =========================================================================
### DocCollection objects
### -------------------------------------------------------------------------

### The virtual DocCollection class, which contains one or more
### documents, via two initial implementations: list (of lists) and
### data.frame. These differ from ordinary lists and data.frames by
### the relationship between the list and data.frame representation. A
### DocList has elements corresponding to the *rows* of a data.frame,
### not the columns.
###
### Thus, we need a separate abstraction, and any
### DocCollection object is contracted to implement this API:
### - Two dimensional [,] extraction
### - ids() returns the document IDs
### - fieldNames() returns the names of the unique document fields
###
### The data.frame object already supports the first, and it would be
### easy to add data.frame methods for the second two, but we want a
### data.frame that extends DocCollection, so that methods based on
### the DocCollection API can restrict their signature to
### DocCollection.
###

setClass("DocCollection")
setClass("DocList", contains = c("list", "DocCollection"),
         validity = function(object) {
           if (any(vapply(lapply(object, names), is.null, logical(1)) &
                   !vapply(object, is.null, logical(1))))
             "all non-NULL elements must have names"
         })
setClass("DocDataFrame", contains = c("data.frame", "DocCollection"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion/Construction
###

setAs("ANY", "DocCollection", function(from) {
  as(from, "DocDataFrame")
})

setAs("list", "DocCollection", function(from) {
  as(from, "DocList")
})

setAs("data.frame", "DocCollection", function(from) {
  as(from, "DocDataFrame")
})

setAs("ANY", "DocList", function(from) {
  new("DocList", lapply(from, as, "list"))
})

setAs("data.frame", "DocList", function(from) {
          l <- lapply(split(from, rownames(from)), as, "list")
          new("DocList", lapply(l, setNames, colnames(from)))
      })

setAs("DocDataFrame", "list", function(from) {
          if (strict) {
              setNames(from@.Data, names(from))
          } else {
              from
          }
      })

setAs("ANY", "DocDataFrame", function(from) {
  new("DocDataFrame", as.data.frame(from))
})

## otherwise, methods package inserts its own method, because
## DocDataFrame ultimately contains 'list'.
setAs("list", "DocDataFrame", function(from) {
  new("DocDataFrame", as.data.frame(from))
})

setAs("DocDataFrame", "data.frame", function(from) {
          if (strict) {
              S3Part(from, TRUE)
          } else {
              from
          }
      })

setAs("DocList", "data.frame", function(from) {
          as(from, "DocDataFrame")
      })

setAs("DocList", "DocDataFrame", function(from) {
          as.data.frame(from, optional=TRUE)
      })

as.data.frame.DocCollection <-
    function (x, row.names = NULL, optional = FALSE, ...) {
        as.data.frame(x, row.names=row.names, optional=optional)
    }

setMethod("as.data.frame", "DocList",
          function (x, row.names = NULL, optional = FALSE) {
              if (!isTRUEorFALSE(optional)) {
                  stop("'optional' must be TRUE or FALSE")
              }
              ans <- as(restfulr:::raggedListToDF(x), "DocDataFrame")
              if (is.null(row.names)) {
                  row.names <- ids(x)
              }
              rownames(ans) <- row.names
              if (!optional) {
                  names(ans) <- make.names(names(ans))
              }
              ans
          })

## 'c' is a primitive, so we could define an S4 method without an S3 method,
## but the S4 generic for 'c' is problematic
c.DocCollection <- function(...) {
  as(NextMethod(), "DocCollection")
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setGeneric("ids", function(x) standardGeneric("ids"))
setMethod("ids", "DocCollection", function(x) ROWNAMES(x))

setGeneric("ids<-", function(x, value) standardGeneric("ids<-"))
setReplaceMethod("ids", "DocList", function(x, value) {
    names(x) <- value
    x
})
setReplaceMethod("ids", "DocDataFrame", function(x, value) {
  rownames(x) <- value
  x
})

unid <- function(x) {
  ids(x) <- NULL
  x
}

setReplaceMethod("names", "DocList", function(x, value) {
                     names(S3Part(x, TRUE)) <- value
                     x
                 })

setGeneric("fieldNames", function(x, ...) standardGeneric("fieldNames"))
setMethod("fieldNames", "DocList", function(x) {
  as.character(unique(unlist(lapply(x, names), use.names=FALSE)))
})
setMethod("fieldNames", "DocDataFrame", function(x) names(x))

setGeneric("fieldNames<-", function(x, value) standardGeneric("fieldNames<-"))
setReplaceMethod("fieldNames", "DocList", function(x, value) {
  initialize(x, lapply(x, setNames, value))
})
setReplaceMethod("fieldNames", "DocDataFrame", function(x, value) {
  colnames(x) <- value
  x
})

setGeneric("ndoc", function(x, ...) standardGeneric("ndoc"))
setMethod("ndoc", "DocCollection", function(x) NROW(x))

setGeneric("nfield", function(x, ...) standardGeneric("nfield"))
setMethod("nfield", "ANY", function(x) length(fieldNames(x)))
setMethod("nfield", "DocList", function(x) length(unique(fieldNames(x))))

uncommonFields <- function(x, j) {
  fieldtab <- table(unlist(lapply(x, names), use.names=FALSE))
  common <- names(fieldtab)[fieldtab == length(x)]
  setdiff(j, common)
}

setMethod("[", "DocList", function(x, i, j, ..., drop = TRUE) {
  if (!isTRUEorFALSE(drop)) {
    stop("'drop' should be TRUE or FALSE")
  }
  if (!missing(i)) {
### FIXME: have to call S3Part() here due to bug in
### callNextMethod(). It should probably use the C-level
### callNextMethod, but when we specify arguments, it calls
### .nextMethod(), which does not work when .nextMethod is a
### primitive (infinite recursion).
    ans <- callNextMethod(S3Part(x, TRUE), i)
  } else {
    ans <- x
  }
  dropped <- FALSE
  if (!missing(j)) {
    if (!is.character(j)) {
      stop("'j' must be character")
    }
    if (drop && length(j) == 1L) {
      ans <- simplify2array2(lapply(unname(ans), `[[`, j))
      dropped <- TRUE
    } else {
      ans <- lapply(ans, function(d) {
                      d <- d[j]
                      d[!is.na(names(d))]
                    })
    }
  }
  if (dropped) {
    ans
  } else {
    initialize(x, ans)
  }
})

setMethod("[", "DocDataFrame", function(x, i, j, ..., drop = TRUE) {
              ans <- callNextMethod()
              if (is.data.frame(ans)) {
                  as(ans, "DocDataFrame")
              } else {
                  ans
              }
          })

setReplaceMethod("[", "DocList", function(x, i, j, ..., value) {
  if (missing(j)) {
      S3Part(x, TRUE)[i] <- value
      return(x)
  }
  if (missing(i)) {
    i <- seq_along(x)
  }
  if (is.null(value)) {
    value <- rep(list(NULL), length(x[i]))
  } else if (is.atomic(value) && !is.array(value)) {
    if (!is.vector(value)) {
      value <- lapply(value, list) # cannot rely on [<- to do this; drops attrs
    }
  } else {
    value <- as(value, "DocList")
  }
  x[i] <- mapply(function(xi, valuei) {
    xi[j] <- valuei
    xi
  }, x[i], value, SIMPLIFY=FALSE)
  x
})

setGeneric("meta", function(x) standardGeneric("meta"))

setMethod("meta", "ANY", function(x) {
  attr(x, "meta")
})

setMethod("meta", "DocList", function(x) {
  lapply(x, attr, "meta")
})

setGeneric("meta<-", function(x, value) standardGeneric("meta<-"))

setReplaceMethod("meta", c("DocList", "NULL"), function(x, value) {
  value <- rep(list(NULL), length(x))
  callGeneric()
})

setReplaceMethod("meta", c("DocList", "list"), function(x, value) {
  initialize(x, mapply(function(xi, valuei) {
    attr(xi, "meta") <- valuei
    xi
  }, x, value, SIMPLIFY=FALSE))
})

setReplaceMethod("meta", c("DocDataFrame", "data.frame"), function(x, value) {
  stopifnot(nrow(value) == nrow(x))
  attr(x, "meta") <- value
  x
})

setReplaceMethod("meta", c("ANY", "NULL"), function(x, value) {
  attr(x, "meta") <- NULL
  x
})

unmeta <- function(x) {
  meta(x) <- NULL
  x
}

setGeneric("docs", function(x, ...) standardGeneric("docs"))

setMethod("docs", "DocCollection", function(x) x)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Iteration
###

setGeneric("convertFields",
           function(x, types, FUN) standardGeneric("convertFields"),
           signature="x")

setMethod("convertFields", "DocDataFrame", function(x, types, FUN) {
  if (length(x) == 0L) {
    return(x)
  }
  initialize(x, as.data.frame(mapply(FUN, x, types, SIMPLIFY=FALSE),
                              optional=TRUE, stringsAsFactors=FALSE,
                              row.names=rownames(x)))
})

setMethod("convertFields", "DocList", function(x, types, FUN) {
  initialize(x, lapply(x, function(xi) {
    if (is.null(xi)) {
      NULL
    } else {
      mapply(FUN, xi, types[names(xi)], SIMPLIFY=FALSE)
    }
  }))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "DocCollection", function(object) {
  cat(class(object), " (", ndoc(object), "x", nfield(object), ")\n", sep="")
})


showDoc <- function(x, title) {
    cat(title, "\n")
    lines <- labeledLine(names(x), unname(x),
                         count = lengths(x) > 1L,
                         vectorized=TRUE)
    cat(lines, "\n", sep="")
}

setMethod("show", "DocList", function(object) {
  callNextMethod()
  if (length(object) > 0L) {
    showDoc(object[[1L]], "First document")
  }
})

setMethod("show", "DocDataFrame", function(object) {
  callNextMethod()
  out <- makePrettyMatrixForCompactPrinting(object)
  print(out, quote=FALSE, right=TRUE, max=length(out))
})
