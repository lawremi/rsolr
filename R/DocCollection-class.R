### =========================================================================
### DocCollection objects
### -------------------------------------------------------------------------

### The virtual DocCollection class, which contains one or more
### documents, via two initial implementations: list (of lists) and
### data.frame. These differ from ordinary lists and data.frames by
### the relationship between the list and data.frame representation. A
### DocList has elements corresponding to the *rows* of a data.frame,
### not the columns. This means that a DocDataFrame will yield the
### correct JSON when converted.
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
  new("DocList", lapply(from, as.list))
})

setAs("data.frame", "DocList", function(from) {
  new("DocList", split(from, rownames(from)))
})

setAs("ANY", "DocDataFrame", function(from) {
  new("DocDataFrame", as.data.frame(from))
})

## otherwise, methods package inserts its own method, because
## DocDataFrame ultimately contains 'list'.
setAs("list", "DocDataFrame", function(from) {
  new("DocDataFrame", as.data.frame(from))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

setGeneric("ids", function(x) standardGeneric("ids"))
setMethod("ids", "DocList", function(x) names(x))
setMethod("ids", "DocDataFrame", function(x) rownames)

setGeneric("ids<-", function(x, value) standardGeneric("ids<-"))
setReplaceMethod("ids", "DocList", function(x, value) {
  names(x@.Data) <- value
  x
})
setReplaceMethod("ids", "DocDataFrame", function(x, value) {
  rownames(x) <- value
  x
})

setGeneric("fieldNames", function(x) standardGeneric("fieldNames"))
setMethod("fieldNames", "DocList", function(x) {
  as.character(unique(unlist(lapply(x, names), use.names=FALSE)))
})
setMethod("fieldNames", "DocDataFrame", function(x) names(x))

uncommonFields <- function(x, j) {
  fieldtab <- table(unlist(lapply(x, names), use.names=FALSE))
  common <- names(fieldtab)[fieldtab == length(x)]
  setdiff(j, common)
}

setMethod("[", "DocList", function(x, i, j, ..., drop = TRUE) {
  if (!missing(i)) {
### callNextMethod(x, i) broken because [ is primitive
    d <- x@.Data
    names(d) <- names(x)
    ans <- d[i]
  } else {
    ans <- x
  }
  if (!missing(j)) {
    if (!is.character(j)) {
      stop("'j' must be character")
    }
    badj <- uncommonFields(x, j)
    if (length(badj) > 0L) {
      stop("fields ", paste(badj, collapse=", "), " not found in all documents")
    }
    if (drop) {
      ans <- simplify2array2(lapply(ans, `[[`, j))
    } else {
      ans <- lapply(ans, `[`, j)
    }
  }
  if (drop && ((!missing(j) && length(j) == 1L) || !missing(drop))) {
    ans
  } else {
    initialize(x, ans)
  }
})

setReplaceMethod("[", "DocList", function(x, i, j, ..., value) {
  if (missing(j)) {
    x@.Data[i] <- value # broken: callNextMethod()
    return(x)
  }
  if (missing(i)) {
    i <- seq_along(x)
  }
  if (is.null(value)) {
    value <- rep(list(NULL), length(x))
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

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Iteration
###

setGeneric("convertFields",
           function(x, types, FUN) standardGeneric("convertFields"),
           signature="x")

setMethod("convertFields", "DocDataFrame", function(x, types, FUN) {
  initialize(x, as.data.frame(mapply(FUN, x, types, SIMPLIFY=FALSE)))
})

setMethod("convertFields", "DocList", function(x, types, FUN) {
  initialize(x, lapply(x, function(xi) {
    mapply(FUN, xi, types[names(xi)], SIMPLIFY=FALSE)
  }))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "DocList", function(object) {
  cat("DocList object\n")
  meta(object) <- NULL
  show(do.call(list, object))
})
