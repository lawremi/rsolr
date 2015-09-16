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
                        multiValued="logical",
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
multiValued <- function(x) x@multiValued
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
    return(resolve(i, x))
  }
  initialize(x,
             name=x@name[i],
             typeName=x@typeName[i],
             dynamic=x@dynamic[i],
             multiValued=x@multiValued[i],
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

FieldInfo <- function(name, typeName, dynamic=FALSE, multiValued=FALSE,
                      required=FALSE, indexed=FALSE, stored=FALSE,
                      docValues=FALSE)
{
  len <- length(name)
  new("FieldInfo",
      name=name,
      typeName=recycleCharacterArg(typeName, "typeName", len),
      dynamic=recycleLogicalArg(dynamic, "dynamic", len),
      multiValued=recycleLogicalArg(multiValued, "multiValued", len),
      required=recycleLogicalArg(required, "required", len),
      indexed=recycleLogicalArg(indexed, "indexed", len),
      stored=recycleLogicalArg(stored, "stored", len),
      docValues=recycleLogicalArg(docValues, "docValues", len))
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
                       multiValued=append(x@multiValued, values@multiValued,
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
                   data.frame(row.names=name, typeName, dynamic, multiValued,
                              required, indexed, stored, docValues))
          })
          
setAs("FieldInfo", "data.frame",
      function(from) as.data.frame(from, optional=TRUE))

as.list.FieldInfo <- function(x, ...) {
  lapply(seq_len(length(x)), function(i) {
    x[i]
  })
}

setMethod("as.list", "FieldInfo", as.list.FieldInfo)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Resolution of dynamic fields
###

setGeneric("resolve", function(x, field, ...) standardGeneric("resolve"))

### FIXME: this is slow for dynamic fields
setMethod("resolve", c("character", "FieldInfo"), function(x, field) {
              static.x <- match(x, names(field), 0)
              static.fields <- field[static.x]
              dyn.x <- setdiff(x, names(static.fields))
              if (length(dyn.x) == 0L) {
                  return(static.fields)
              }
              dyn.fields <- field[dynamic(field)]
              hits <- globMatchMatrix(names(dyn.fields), dyn.x)
              nohits <- rowSums(hits) == 0L
              if (any(nohits)) {
                  stop("field(s) ",
                       paste(dyn.x[nohits], collapse = ", "),
                       " not found in schema")
              }
              selected <- max.col(hits, ties.method="first")
              dyn.fields <- dyn.fields[selected]
              dyn.fields@name <- dyn.x
              ans <- append(static.fields, dyn.fields)
              ans[match(x, names(ans))]
          })

setMethod("%in%", c("character", "FieldInfo"), function(x, table) {
              ans <- x %in% names(table)
              if (!all(ans)) {
                  hits <- globMatchMatrix(names(table)[dynamic(table)], x[!ans])
                  ans[!ans] <- rowSums(hits) > 0L
              }
              ans
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "FieldInfo", function(object) {
  show(as.data.frame(object))
})
