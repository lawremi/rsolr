### =========================================================================
### FieldInfo objects
### -------------------------------------------------------------------------
###
### Internal helper class
###

slotLengths <- function(x) {
  vapply(slotNames(x), function(s) length(slot(x, s)), integer(1))
}

slotHasNAs <- function(x) {
  vapply(slotNames(x), function(s) any(is.na(slot(x, s))), logical(1))
}

setClass("FieldInfo",
         representation(name="character",
                        typeName="character",
                        dynamic="logical",
                        multivalued="logical"),
         validity=function(object) {
           if (length(unique(slotLengths(object))) != 1L)
             "all slots must have the same length"
           if (any(slotHasNAs(object)))
             "one or more slots contain NA values"
         })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

dynamic <- function(x) x@dynamic
multivalued <- function(x) x@multivalued
typeName <- function(x) x@typeName

setMethod("length", "FieldInfo", function(x) length(x@name))

setMethod("names", "FieldInfo", function(x) x@name)

setMethod("[", "FieldInfo", function(x, i, j, ..., drop=TRUE) {
  if (is.character(i)) {
    i <- match(i, x@name)
  }
  initialize(x,
             name=x@name[i],
             typeName=x@typeName[i],
             dynamic=x@dynamic[i],
             multivalued=x@multivalued[i])
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Combination
###

setMethod("append", c("FieldInfo", "FieldInfo"),
          function(x, values, after = length(x)) {
            initialize(x,
                       name=append(x@name, values@name, after),
                       typeName=append(x@typeName, values@typeName, after),
                       dynamic=append(x@dynamic, values@dynamic, after),
                       multivalued=append(x@multivalued, values@multivalued,
                         after))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

as.data.frame.FieldInfo <-
  function(x, row.names = NULL, optional = FALSE, ...) {
    if (!missing(row.names) || !missing(optional) ||
        length(list(...)) > 0L) {
      warning("all arguments besides 'x' are ignored")
    }
    with(attributes(x),
         data.frame(row.names=name, typeName, dynamic, multivalued))
  }

setAs("FieldInfo", "data.frame", function(from) as.data.frame(from))

as.list.FieldInfo <- function(x) {
  lapply(seq_len(length(x)), function(i) {
    x[i]
  })
}

setMethod("as.list", "FieldInfo", as.list.FieldInfo)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Resolution of dynamic fields
###

setGeneric("resolve", function(x, field, ...) standardGeneric("resolve"))

### FIXME: ugly code
setMethod("resolve", c("character", "FieldInfo"), function(x, field) {
  static.field <- field[!dynamic(field)]
  static.ind <- match(x, names(static.field))
  dyn.x <- x[is.na(static.ind)]
  dyn.field <- field[dynamic(field)]
  dyn.field <- dyn.field[order(nchar(names(dyn.field)), decreasing=TRUE)]
  rx <- setNames(glob2rx(names(dyn.field)), names(dyn.field))
  hits <- lapply(rx, grep, dyn.x, value=TRUE)
  dyn.ind <- as.character(with(stack(hits), ind[match(dyn.x, values)]))
  if (any(is.na(dyn.ind))) {
    stop("field(s) ",
         paste(dyn.x[is.na(dyn.ind)], collapse = ", "),
         " not found in schema")
  }
  dyn.matched <- dyn.field[dyn.ind]
  dyn.matched@name <- dyn.x
  append(static.field[na.omit(static.ind)], dyn.matched)[x]
})

## alternative, looping over 'x'
## setMethod("resolve", c("character", "FieldInfo"), function(x, field) {
##   static.field <- field[!dynamic(field)]
##   dyn.field <- field[dynamic(field)]
##   rx <- glob2rx(names(dyn.field))[order(nchar(dyn.field), decreasing=TRUE)]
##   do.call(c, lapply(x, function(xi) {
##     static.ind <- match(xi, names(static.field))
##     if (!is.na(static.ind)) {
##       static.field[static.ind]
##     } else {
##       dyn.ind <- which(vapply(rx, grepl, logical(1), xi))[1]
##       if (is.na(dyn.ind)) {
##         stop("unable to resolve field name ", xi)
##       }
##       f <- dyn.field[dyn.ind]
##       f@name <- xi
##       f
##     }
##   }))
## })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "FieldInfo", function(object) {
  show(as.data.frame(object))
})
