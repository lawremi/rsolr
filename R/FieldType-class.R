### =========================================================================
### Solr Field Types
### -------------------------------------------------------------------------

### To be flexible, we need to support coercion
### between specific R and Solr types. Coercing between the abstract
### "SolrString" may not be sufficient. There could be multiple ways
### to represent an R object in Solr, and sometimes the representation
### will not be a string.

### If Solr types need to be parameterized, then we need
### objects corresponding to the types (as the Solr implementation
### itself has). Currently, the Solr types correspond to classes on
### the data, not the type itself.

### Solutions:

### (a) Have a SolrField that contains 'vector' and has a
###     SolrFieldType. This avoids having a SolrField subclass for
###     every type. The problem: we need to dispatch on the type, so
###     setAs() would no longer work, and the fromSolrField generic
###     would have a redundancy: the data (referencing the type) and
###     the type itself would be passed as parameters. In general, it
###     seems strange to store type information separately from the
###     class. Alternative: completely forego any special class on the
###     data. Coercion takes the R vector and the type information as
###     separate parameters. The downside is that the semantics of the
###     data are no longer encapsulated with the data. But do we need
###     this? The Solr representation is internal. It would be nice to
###     somehow mark the list of records for what it is, to enable
###     easy coercion to e.g. a data.frame or JSON (an ordinary list
###     would be treated as a list of columns). This solution is
###     analogous to the ID conversion in GSEABase.

### (b) Stick with specific typing of the data. Do exactly what
###     setAs() does with the coerce generic, i.e., make a prototype
###     object, and stick parameters on that prototype. The difference
###     is that the coercion function receives the prototype, instead
###     of just 'from'. This prototype-based dispatch would also
###     support extensible schema parsing. In fact, this mechanism
###     would also work for the entire document: a prototype of the
###     desired document type would be passed to the function. We
###     might call this function 'conform' -- the result should
###     conform to the prototype.

### (c) Have two parallel hierarchies, one for the data, one for the
###     type information. This is a lot more complex.

### A major difference between (a) and (b) is whether the data are
### specially typed when they conform to the Solr schema. For example,
### RCurl defines the base64 class to semantically label the character
### vector as being of base64 encoding. Choice (b) takes this route.

### This makes sense for base64, since base64 will always be a
### character vector. Does the Solr type imply an R type? One could
### imagine a character Rle that conforms to the Solr Date format. Are
### Solr types more like formats than data structures? Could be either
### way. Probably need to reduce the objects to atomic vectors for
### easy JSON export. So, in general, it *is* a coercion. But consider
### the advantages of classing the data: are there any?  This is
### purely an internal representation.

### Advantages of separate type objects:
### - Possibly cleaner schema representation. Right now, we take a
###   vectorized/tabular approach, which is efficient but not
###   extensible. There is no need for efficiency in the schema.
### - Conceptually cleaner: aspects like the multivalue separator
###   really pertain to the *type*; they are not data.

###   For the media types, we took the simple data classing route,
###   which is conceptually ugly (content types can be parameterized,
###   too), but worked because there is really no schema in
###   REST. Content types are essentially per-response. The client
###   code is not aware of what the backend will return, so the type
###   needs to be encapsulated in the return value.  Because there is
###   no schema, we assign some default content type to each R object,
###   and there has not been a need to be specific about
###   parameters. We just pick default values.

### Let's try (a).

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Abstract field types (for our convenience, not necessarily defined by Solr)
###

setClass("FieldType",
         representation(multiValued="logical",
                        required="logical",
                        indexed="logical",
                        stored="logical",
                        docValues="logical",
                        useDocValuesAsStored="logical"),
         prototype(multiValued=FALSE,
                   required=FALSE,
                   indexed=TRUE,
                   stored=TRUE,
                   docValues=FALSE,
                   useDocValuesAsStored=TRUE),
         contains="VIRTUAL")

setClass("FieldTypeList", contains="list",
         validity=function(object) {
           validHomogeneousList(object, "FieldType")
         })

### NOTE:  The "list" class does not have a "names" slot (since the
###        names attribute is optional), so we need to explicitly set
###        the names on the S3 part.
setReplaceMethod("names", "FieldTypeList", function(x, value) {
  names(S3Part(x, TRUE)) <- value
  x
})

FieldTypeList <- function(...) {
  args <- list(...)
  if (length(args) == 1L && is.list(args[[1L]])) {
    args <- args[[1L]]
  }
  new("FieldTypeList", args)
}

setMethod("append", c("FieldTypeList", "FieldTypeList"),
          function(x, values, after = length(x)) {
            new("FieldTypeList", callNextMethod())
          })

setMethod("[", "FieldTypeList", function(x, i, j, ..., drop=TRUE) {
  initialize(x, callNextMethod())
})

setClass("LogicalField", contains=c("FieldType", "VIRTUAL"))
setClass("CharacterField", contains=c("FieldType", "VIRTUAL"))
setClass("NumericField", contains=c("FieldType", "VIRTUAL"))
setClass("IntegerField", contains=c("NumericField", "VIRTUAL"))

### Basic field types

setClass("solr.TextField", contains="CharacterField")
setClass("solr.StrField", contains="CharacterField")
setClass("solr.BCDStrField", contains="solr.StrField")
setClass("solr.UUIDField", contains="CharacterField")
setClass("solr.BinaryField", contains="CharacterField")
setClass("solr.BoolField", contains="LogicalField")
setClass("solr.DoubleField", contains="NumericField")
setClass("solr.TrieDoubleField", contains="solr.DoubleField")
setClass("solr.SortableDoubleField", contains="solr.DoubleField")
setClass("solr.FloatField", contains="NumericField")
setClass("solr.TrieFloatField", contains="solr.FloatField")
setClass("solr.SortableFloatField", contains="solr.FloatField")
setClass("solr.IntField", contains="IntegerField")
setClass("solr.TrieIntField", contains="solr.IntField")
setClass("solr.SortableIntField", contains="solr.IntField")
setClass("solr.BCDIntField", contains="solr.IntField")
setClass("solr.LongField", contains="NumericField")
setClass("solr.TrieLongField", contains="solr.LongField")
setClass("solr.SortableLongField", contains="solr.LongField")
setClass("solr.BCDLongField", contains="solr.LongField")

### Special types

setClassUnion("SubFieldType",
              c("FieldType", "FieldTypeList", "character", "NULL"))
setClass("SubTypeFieldType",
         representation(subFieldType="SubFieldType",
                        subFieldSuffix="character_OR_NULL"),
         contains=c("CharacterField", "VIRTUAL"),
         validity=function(object) {
           if (!is.null(subFieldType) && !is.null(subFieldSuffix))
             "only one of subFieldType and subFieldSuffix should be NULL"
         })
setClass("solr.PointType", representation(dimension="integer"),
         contains="SubTypeFieldType")
setClass("solr.LatLonType", contains="SubTypeFieldType")

## TODO: in theory, could make a WKT parser
setClass("solr.AbstractSpatialFieldType", contains="CharacterField")
setClass("solr.LatLonPointSpatialField",
         contains="solr.AbstractSpatialFieldType")
setClass("solr.SpatialRecursivePrefixTreeFieldType",
         contains="solr.AbstractSpatialFieldType")
setClass("solr.RptWithGeometrySpatialField",
         contains="solr.SpatialRecursivePrefixTreeFieldType")
setClass("solr.BBoxField", contains="solr.AbstractSpatialFieldType")

setClass("solr.DateField", contains="CharacterField")
setClass("solr.TrieDateField", contains="solr.DateField")

## TODO: make a 2 column data.frame with value and currency
setClass("solr.CurrencyField", contains="CharacterField")

## Seems like there is no easy way to get value set
setClass("solr.EnumField", contains="CharacterField")

setClass("solr.PreAnalyzedField", contains="CharacterField")
setClass("solr.ICUCollationField", contains="CharacterField")
setClass("solr.RandomSortField", contains="FieldType")
setClass("solr.ExternalFileField", contains="NumericField")
setClass("solr.GeoHashField", contains="CharacterField")

setClass("AnyField", contains="FieldType")

setClassUnion("FacetableField",
              c("LogicalField", "CharacterField", "NumericField",
                "IntegerField", "solr.DateField"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Resolution of type against a field definition
###

setMethod("resolve", c("FieldType", "FieldInfo"), function(x, field, schema) {
  x@multiValued <- field@multiValued
  x@required <- field@required
  x@indexed <- field@indexed
  x@stored <- field@stored
  x@docValues <- field@docValues
  x
})

setMethod("resolve", c("SubTypeFieldType", "FieldInfo"),
          function(x, field, schema) {
            x <- callNextMethod()
            x@subFieldType <- if (is.character(x@subFieldType)) {
              fieldTypes(schema)[[x@subFieldType]]
            } else if (!is.null(x@subFieldSuffix)) {
              typeNames <- typeName(fields(schema, subFieldNames(x, field)))
              types <- fieldTypes(schema)[typeNames]
              if (length(unique(typeNames)) == 1L) {
                types[[1]]
              } else {
                types
              }
            } else {
              x@subFieldType
            }
            x
          })

setGeneric("subFieldNames", function(x, field) standardGeneric("subFieldNames"),
           signature="x")

setMethod("subFieldNames", "solr.PointType", function(x, field) {
  paste0(field@name, "_", seq_len(x@dimension)-1L, "_", x@subFieldSuffix)
})

setMethod("subFieldNames", "solr.LatLonType", function(x, field) {
  paste0(field@name, "_", 0:1, "_", x@subFieldSuffix)
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### R modes for Solr types
###

setGeneric("solrMode", function(x) standardGeneric("solrMode"))

setMethod("solrMode", "AnyField", function(x) "any")
setMethod("solrMode", "LogicalField", function(x) "logical")
setMethod("solrMode", "CharacterField", function(x) "character")
setMethod("solrMode", "NumericField", function(x) "numeric")
setMethod("solrMode", "IntegerField", function(x) "integer")
setMethod("solrMode", "FieldTypeList", function(x) {
  vapply(x, solrMode, character(1L))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Solr types for R classes
###

setGeneric("solrType", function(x) standardGeneric("solrType"))

setMethod("solrType", "logical", function(x) new("solr.BoolField"))
setMethod("solrType", "integer", function(x) new("solr.TrieIntField"))
setMethod("solrType", "numeric", function(x) new("solr.TrieDoubleField"))
setMethod("solrType", "character", function(x) new("solr.StrField"))
setMethod("solrType", "factor", function(x) new("solr.StrField"))
setMethod("solrType", "list",
          function(x) {
            type <- solrType(unlist(x, use.names=FALSE))
            initialize(type, multiValued=TRUE)
          })
setMethod("solrType", "POSIXt", function(x) new("solr.TrieDateField"))
setMethod("solrType", "raw", function(x) new("solr.BinaryField"))

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Conversion
###

setGeneric("toSolr", function(x, type, ...) standardGeneric("toSolr"))
setGeneric("fromSolr", function(x, type, ...) standardGeneric("fromSolr"))

convertAtomic <- function(x, type, FUN) {
    ans <- as.vector(x, solrMode(type))
    if (storage.mode(x) != storage.mode(ans)) {
        ans <- FUN(ans, type)
    }
    ans
}

convertList <- function(x, type, FUN) {
    if (!multiValued(type)) {
        return(callNextMethod())
    }
    if (length(x) == 0L) {
        return(x)
    }
    ans <- unname(lapply(x, as.vector, solrMode(type)))
    redo <- vapply(ans, storage.mode, character(1L)) !=
        vapply(x, storage.mode, character(1L))
    ans[redo] <- FUN(ans[redo], type)
    ans
}

setMethod("toSolr", c("ANY", "FieldType"), function(x, type) {
              convertAtomic(x, type, toSolr)
          })

setMethod("toSolr", c("list", "FieldType"), function(x, type) {
              convertList(x, type, toSolr)
          })

setMethod("toSolr", c("AsIs", "FieldType"), function(x, type) {
              I(toSolr(unclass(x), type))
          })

setMethod("fromSolr", c("ANY", "FieldType"), function(x, type) {
              convertAtomic(x, type, fromSolr)
          })

setMethod("fromSolr", c("list", "FieldType"), function(x, type) {
              convertList(x, type, fromSolr)
          })

setMethod("fromSolr", c("character", "solr.BinaryField"),
          function(x, type) {
            base64(x, encode=FALSE)
          })

setMethod("toSolr", c("ANY", "solr.BinaryField"),
          function(x, type) {
            base64(x)
          })

setMethod("toSolr", c("matrix", "solr.PointType"), function(x, type) {
  if (ncol(x) != type@dimension) {
    stop("matrix must have ", type@dimension, " columns")
  }
  if (!is(type@subType, "FieldType")) {
    stop("subType must be resolved to a single type")
  }
  mode(x) <- solrMode(type@subType)
  do.call(paste, c(split(x, col(x)), sep=","))
})

setMethod("fromSolr", c("character", "solr.PointType"), function(x, type) {
  if (!is(type@subType, "FieldType")) {
    stop("subType must be resolved to a single type")
  }
  tokens <- strsplit(x, ",", fixed=TRUE)
  v <- fromSolr(unlist(tokens), type@subType)
  matrix(v, nrow=length(v)/type@dimension, ncol=type@dimension, byrow=TRUE)
})

setMethod("toSolr", c("POSIXt", "solr.DateField"), function(x, type) {
  format(x, "%Y-%m-%dT%H:%M:%SZ", tz="UTC")
})

setMethod("fromSolr", c("character", "solr.DateField"), function(x, type) {
  as.POSIXct(strptime(x, "%Y-%m-%dT%H:%M:%SZ", tz="UTC"))
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Parsing
###

setGeneric("parseFieldType",
           function(x, proto) standardGeneric("parseFieldType"),
           signature="proto")

setMethod("parseFieldType", "FieldType", function(x, proto) {
  proto@multiValued <- if (is.null(x$multiValued)) FALSE else x$multiValued
  proto@required <- if (is.null(x$required)) FALSE else x$required
  proto@indexed <- if (is.null(x$indexed)) TRUE else x$indexed
  proto@docValues <- if (is.null(x$docValues)) FALSE else x$docValues
  proto@stored <- if (is.null(x$stored)) TRUE else x$stored
  proto@useDocValuesAsStored <- if (is.null(x$useDocValuesAsStored)) TRUE
                                else x$useDocValuesAsStored
  proto
})

setMethod("parseFieldType", "SubTypeFieldType", function(x, proto) {
  proto <- callNextMethod()
  if (!is.null(x$subFieldType)) {
    proto@subFieldType <- x$subFieldType
  } else {
    proto@subFieldSuffix <- x$subFieldSuffix
  }
  proto
})

setMethod("parseFieldType", "solr.PointType", function(x, proto) {
  proto <- callNextMethod()
  proto@dimension <- as.integer(x$dimension)
  proto
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

as.data.frame.FieldTypeList <-
    function(x, row.names = NULL, optional = FALSE, ...) {
        as.data.frame(x, row.names=row.names, optional=optional, ...)
    }

setMethod("as.data.frame", "FieldTypeList",
          function(x, row.names = NULL, optional = FALSE, ...) {
              if (!missing(row.names) || !missing(optional) ||
                  length(list(...)) > 0L) {
                  warning("all arguments besides 'x' are ignored")
              }
              slots <- lapply(slotNames("FieldType"), function(s) {
                                  vapply(x, slot, s, FUN.VALUE=logical(1L))
                              })
              names(slots) <- slotNames("FieldType")
              data.frame(class=vapply(x, class, character(1)), slots)
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "FieldType", function(object) {
  cat(labeledLine("class", class(object), count=FALSE), sep="")
})

setMethod("show", "SubTypeFieldType", function(object) {
  callNextMethod()
  if (!is.null(object@subFieldType)) {
    cat(labeledLine("subFieldType", object@subFieldType, count=FALSE))
  } else {
    cat(labeledLine("subFieldSuffix", object@subFieldSuffix,
                                   count=FALSE))
  }
})

setMethod("show", "solr.PointType", function(object) {
  callNextMethod()
  cat(labeledLine("dimension", object@dimension, count=FALSE))
})

setMethod("show", "FieldTypeList", function(object) {
  show(as.data.frame(object))
})
