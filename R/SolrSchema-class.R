### =========================================================================
### SolrSchema objects
### -------------------------------------------------------------------------

### Some options for representing field types:

### (a) Mirror Solr schema w/ fields/dynamicFields naming field types,
###     and a separated named list of FieldType objects. This is the
###     "normalized" design. To store the multivalued attribute, we
###     will need a named logical vector slot mapping field name to
###     multivalued. That is a bit ugly, because it assumes no
###     overlap between field and dynamicField names. We may need a
###     Field object that refers to type and has a multivalued slot.
###
### (b) Denormalize to have the (dynamic)Field slots be lists of
###     FieldType objects that store the multivalued setting
###     per-field. There would be one FieldType per field, and the
###     schema definitions would be lost. This would avoid the
###     headache of merging the multivalued setting into the FieldType
###     sent to to/fromSolr(). But thanks to dynamic fields, there is
###     always going to be a headache. This representation is simpler,
###     but we lose the type definitions from the schema. Probably
###     best from the user POV if we are consistent with the schema
###     (even though we discard much of the information, we are at
###     least a subset).
###
### (c) Represent both ordinary and dynamic fields with a table
###     containing: fieldName, fieldTypeName, dynamic, multivalued. And
###     then a fieldTypes[name] list.

### Think top-down, a bit about the API:

### - Perhaps a fields(x, which) function that returns a
###   data.frame of information (name, type name, dynamic,
###   multivalued), and is smart enough to resolve dynamic fields
###   (dynamic is TRUE if a dynamic field was matched).  If which is
###   missing, all fields and dynamic fields are described. This table
###   is for user consumption, and just names types according to
###   the schema.
###
### - Copy fields are kind of their own beast: copyFields(x) returns a
###   named character vector (from=>to).
###
### - fieldTypes(x, fields) that returns a list of FieldType
###   objects. If fields is specified, they are resolved against the
###   field names, and multivalue information is merged; otherwise,
###   just the field types are returned. The list is named by the name
###   of the type. List probably needs special type to help with
###   pretty-printing, at least.

### All this points to (c), having a tabular representation of field
### info (name, type name, dynamic, multivalued), with a separate
### fieldTypes list slot.

setOldClass("package_version")

setClass("SolrSchema",
         representation(name="character",
                        version="package_version",
                        uniqueKey="characterORNULL",
                        copyFields="graph",
                        fields="FieldInfo",
                        fieldTypes="FieldTypeList"),
         validity = function(object) {
           c(if (!isSingleString(object@name))
             "name must be a single, non-NA string",
             if (!is.null(object@uniqueKey) && isSingleString(uniqueKey))
             "uniqueKey must be a scalar, non-NA string, or NULL")
         })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Constructor
###

## unexported
SolrSchema <- function(schema) {
  new("SolrSchema",
      name=schema$name,
      version=package_version(schema$version),
      uniqueKey=schema$uniqueKey,
      fields=append(parseFields(schema$fields, dynamic=FALSE),
        parseFields(schema$dynamicFields, dynamic=TRUE)),
      copyFields=parseCopyFields(schema$copyFields),
      fieldTypes=parseFieldTypes(schema$fieldTypes))
}

parseFields <- function(fields, dynamic) {
  new("FieldInfo",
      name=as.character(pluck(fields, "name")),
      typeName=as.character(pluck(fields, "type")),
      multivalued=as.logical(pluck(fields, "multiValued", FALSE)),
      dynamic=rep(dynamic, length(fields)))
}

parseCopyFields <- function(copy.fields) {
  dest <- as.character(pluck(copy.fields, "dest"))
  source <- as.character(pluck(copy.fields, "source"))
  nodes <- union(dest, source)
  edgeL <- split(dest, factor(source, nodes))
  ##edgeL <- lapply(split(match(dest, nodes), source), function(x) list(edges=x))
  graphNEL(nodes, edgeL, "directed")
}

parseFieldTypes <- function(types) {
  setNames(new("FieldTypeList", lapply(types, function(t) {
    parseFieldType(t, new(t$class))
  })), pluck(types, "name"))
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Accessors
###

version <- function(x) x@version
uniqueKey <- function(x) x@uniqueKey
copyFields <- function(x) x@copyFields

fieldInfo <- function(x, which) {
  if (missing(which)) {
    x@fields
  } else {
    if (!is.character(which) || any(is.na(which))) {
      stop("'which' must be a character vector without NA's")
    }
    resolve(which, x@fields)
  }
}

setGeneric("fields", function(x, ...) standardGeneric("fields"))

setMethod("fields", "SolrSchema", function(x, which) {
  as.data.frame(fieldInfo(x, which))
})

setGeneric("fieldTypes", function(x, ...) standardGeneric("fieldTypes"))

setMethod("fieldTypes", "SolrSchema",  function(x, fields) {
  if (missing(fields)) {
    x@fieldTypes
  } else {
    if (!is.character(fields) || any(is.na(fields))) {
      stop("'fields' must be a character vector without NAs")
    }
    resolved.fields <- fieldInfo(x, fields)
    types <- x@fieldTypes[typeName(resolved.fields)]
    new("FieldTypeList",
        mapply(resolve, types, as.list(resolved.fields),
               MoreArgs=list(schema=schema)))
  }
})


### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Conversion
###

convertCollection <- function(x, type, FUN) {
  x <- as(x, "DocCollection")
  fields <- fieldNames(x)
  convertFields(x, setNames(fieldTypes(type, fields), fields), FUN)
}

resolveUniqueKey <- function(x, type) {
  uk <- uniqueKey(type)
  if (!is.null(uk)) {
    if (!is.null(ids(x)) && !uk %in% fieldNames(x)) {
      ids <- ids(x)
      if (is.null(ids)) {
        stop("uniqueKey (", uk, ") not found")
      }
      x[,uk] <- ids
    }
  }
  x
}

setMethod("toSolr", c("ANY", "SolrSchema"),
          function(x, type) {
            ans <- convertCollection(x, type, toSolr)
            unname(resolveUniqueKey(ans, type))
          })

resolveIds <- function(x, type) {
  uk <- uniqueKey(type)
  if (!is.null(uk) && uk %in% fieldNames(x)) {
    ids(x) <- x[,uk]
  }
  x
}

resolveMeta <- function(x) {
  internal <- grep("^_.*_$", fieldNames(x), value=TRUE)
  meta(x) <- x[,internal,drop=FALSE]
  x[,internal] <- NULL
  x
}

setMethod("fromSolr", c("ANY", "SolrSchema"),
          function(x, type) {
            ans <- convertCollection(x, type, fromSolr)
            ans <- resolveIds(ans, type)
            ans <- resolveMeta(ans)
            ans
          })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

setMethod("show", "SolrSchema", function(object) {
  cat("SolrSchema object\n")
  labeledLine <- BiocGenerics:::labeledLine
  cat(labeledLine("name", name(object), count=FALSE),
      labeledLine("version", version(object), count=FALSE),
      labeledLine("uniqueKey", uniqueKey(object), count=FALSE),
      labeledLine("fields", name(fieldInfo(object))),
      labeledLine("copyFields", paste0(names(copyFields(object)), "=>",
                                       copyFields(object))),
      sep="")
})
