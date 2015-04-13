### =========================================================================
### FieldInfo objects
### -------------------------------------------------------------------------
###
### Internal helper class
###

setClass("FieldInfo",
         representation(name="character",
                        typeName="character",
                        dynamic="logical",
                        multivalued="logical",
                        required="logical",
                        indexed="logical",
                        stored="logical",
                        docValues="logical"),
         validity=function(object) {
           c(if (length(unique(slotLengths(object))) != 1L)
               "all slots must have the same length",
             if (any(slotHasNAs(object)))
               "one or more slots contain NA values")
         })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

dynamic <- function(x) x@dynamic
multivalued <- function(x) x@multivalued
typeName <- function(x) x@typeName
indexed <- function(x) x@indexed
stored <- function(x) x@stored
hidden <- function(x) grepl("^_.*_$", names(x))
required <- function(x) x@required
docValues <- function(x) x@docValues

setMethod("length", "FieldInfo", function(x) length(x@name))

setMethod("names", "FieldInfo", function(x) x@name)

setReplaceMethod("names", "FieldInfo", function(x, value) {
  x@name <- as.character(value)
  x
})

setMethod("[", "FieldInfo", function(x, i, j, ..., drop=TRUE) {
  if (!missing(j) || length(list(...)) || !missing(drop)) {
    warning("arguments 'j', 'drop' and those in '...' are ignored")
  }
  if (is.character(i)) {
    i <- match(i, x@name)
  }
  initialize(x,
             name=x@name[i],
             typeName=x@typeName[i],
             dynamic=x@dynamic[i],
             multivalued=x@multivalued[i],
             required=x@required[i],
             indexed=x@indexed[i],
             stored=x@stored[i],
             docValues=x@docValues[i])
})

setReplaceMethod("[", c(x="FieldInfo", value="FieldInfo"),
                 function(x, i, j, ..., value) {
                   if (!missing(j) || length(list(...))) {
                     warning("argument 'j', and those in '...' are ignored")
                   }
                   if (is.character(i)) {
                     i <- match(i, x@name)
                   }
                   replaceSlot <- function(xs, vs) {
                     xs[i] <- vs
                     xs
                   }
                   xs <- mapply(replaceSlot,
                                slotsAsList(x)[slotNames("FieldInfo")],
                                slotsAsList(value)[slotNames("FieldInfo")],
                                SIMPLIFY=FALSE)
                   do.call(initialize, c(list(x), xs))
                 })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

FieldInfo <- function(name, typeName, dynamic=FALSE, multivalued=FALSE,
                      required=FALSE, indexed=FALSE, stored=FALSE,
                      docValues=FALSE)
{
  len <- length(name)
  new("FieldInfo",
      name=name,
      typeName=recycleVector(typeName, len),
      dynamic=recycleVector(dynamic, len),
      multivalued=recycleVector(multivalued, len),
      required=recycleVector(required, len),
      indexed=recycleVector(indexed, len),
      stored=recycleVector(stored, len),
      docValues=recycleVector(docValues, len))
}

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
                         after),
                       required=append(x@required, values@required, after),
                       indexed=append(x@indexed, values@indexed, after),
                       stored=append(x@stored, values@stored, after),
                       docValues=append(x@docValues, values@docValues, after))
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

as.data.frame.FieldInfo <-
  function(x, row.names = NULL, optional = FALSE, ...) {
    as.data.frame(x, row.names=row.names, optional=optional, ...)
  }

setMethod("as.data.frame", "FieldInfo",
          function(x, row.names = NULL, optional = FALSE, ...) {
              if (!missing(row.names) || !missing(optional) ||
                  length(list(...)) > 0L) {
                  warning("all arguments besides 'x' are ignored")
              }
              with(attributes(x),
                   data.frame(row.names=name, typeName, dynamic, multivalued,
                              required, indexed, stored, docValues))
          })
          
setAs("FieldInfo", "data.frame",
      function(from) as.data.frame(from, optional=TRUE))

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
