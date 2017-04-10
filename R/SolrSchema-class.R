### =========================================================================
### SolrSchema objects
### -------------------------------------------------------------------------

setOldClass("package_version")

setClass("SolrSchema",
         representation(name="character",
                        version="package_version",
                        uniqueKey="character_OR_NULL",
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

parseSchemaXML <- function(doc) {
  attrsToList <- function(x) {
    lapply(x, function(xi) {
      attrs <- as.list(xmlAttrs(xi))
      logicals <- attrs == "true" | attrs == "false"
      attrs[logicals] <- as.logical(attrs[logicals])
      attrs
    })
  }
  schema <- xmlRoot(doc)
  uniqueKey <- schema[["uniqueKey"]]
  if (!is.null(uniqueKey)) {
    uniqueKey <- xmlValue(uniqueKey)
  }
  likeREST <-
    list(name=as.character(xmlAttrs(schema)["name"]),
         version=as.numeric(xmlAttrs(schema)["version"]),
         uniqueKey=uniqueKey,
         fields=attrsToList(getNodeSet(schema, "//field")),
         dynamicFields=attrsToList(getNodeSet(schema, "//dynamicField")),
         copyFields=attrsToList(getNodeSet(schema, "//copyField")),
         fieldTypes=attrsToList(getNodeSet(schema, "//fieldType")))
  ans <- parseSchemaFromREST(likeREST)
  ## do not force dynamic fields after static fields
  fieldsInOrder <- unlist(getNodeSet(schema,
                                     "//field/@name | //dynamicField/@name"),
                          use.names=FALSE)
  fields(ans) <- fields(ans)[fieldsInOrder]
  ans
}

parseSchemaFromREST <- function(schema) {
  fieldTypes <- parseFieldTypes(schema$fieldTypes)
  SolrSchema(
    name=schema$name,
    version=package_version(schema$version),
    uniqueKey=schema$uniqueKey,
    fields=append(parseFields(schema$fields, fieldTypes, dynamic=FALSE),
      parseFields(schema$dynamicFields, fieldTypes, dynamic=TRUE)),
    copyFields=parseCopyFields(schema$copyFields),
    fieldTypes=fieldTypes
  )
}

SolrSchema <- function(name, version, uniqueKey, fields, copyFields, fieldTypes)
{
  new("SolrSchema",
      name=name,
      version=version,
      uniqueKey=uniqueKey,
      fields=fields,
      copyFields=copyFields,
      fieldTypes=fieldTypes)
}

### FIXME: the multivalued, indexed, stored defaults come from the
### type, so the NA values should be resolved, as long as the type
### knows (if not, it depends on the Java class, so who knows).

parseFields <- function(fields, fieldTypes, dynamic) {
  typeName <- vpluck(fields, "type", character(1L))
  resolveFromType <- function(attrName) {
    attr <- vpluck(fields, attrName, logical(1L), required=FALSE)
    accessor <- match.fun(attrName)
    attr[is.na(attr)] <-
      vapply(fieldTypes[typeName[is.na(attr)]], accessor, logical(1L))
    attr
  }
  FieldInfo(name=vpluck(fields, "name", character(1L)),
            typeName=typeName,
            multiValued=resolveFromType("multiValued"),
            dynamic=rep(dynamic, length(fields)),
            required=resolveFromType("required"),
            indexed=resolveFromType("indexed"),
            stored=resolveFromType("stored"),
            docValues=resolveFromType("docValues"))
}

parseCopyFields <- function(copy.fields) {
  dest <- as.character(pluck(copy.fields, "dest"))
  source <- as.character(pluck(copy.fields, "source"))
  nodes <- union(dest, source)
  edgeL <- split(dest, factor(source, nodes))
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

setGeneric("fields", function(x, ...) standardGeneric("fields"))

setMethod("fields", "SolrSchema", function(x, which) {
  if (missing(which)) {
    x@fields
  } else {
    if (!is.character(which) || any(is.na(which))) {
      stop("'which' must be a character vector without NA's")
    }
    resolve(which, x@fields)
  }
})

`fields<-` <- function(x, value) {
  x@fields <- value
  x
}

setGeneric("fieldTypes", function(x, ...) standardGeneric("fieldTypes"))

setMethod("fieldTypes", "SolrSchema",  function(x, fields) {
  if (missing(fields)) {
    x@fieldTypes
  } else {
    if (!is.character(fields) || any(is.na(fields))) {
      stop("'fields' must be a character vector without NAs")
    }
    resolved.fields <- fields(x, fields)
    types <- x@fieldTypes[typeName(resolved.fields)]
    new("FieldTypeList",
        mapply(resolve, types, as.list(resolved.fields),
               MoreArgs=list(schema=x)))
  }
})

`fieldTypes<-` <- function(x, value) {
  x@fieldTypes <- value
  x
}

setGeneric("staticFieldNames",
           function(x, include.hidden = FALSE)
           standardGeneric("staticFieldNames"),
           signature="x")

setMethod("staticFieldNames", "SolrSchema", function(x, include.hidden = FALSE) {
  info <- fields(x)
  names(info)[!dynamic(info) & (include.hidden | !hidden(info))]
})

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Conversion
###

convertCollection <- function(x, type, FUN) {
  x <- as(x, "DocCollection")
  fields <- fieldNames(x)
  types <- fieldTypes(type, fields)
  convertFields(x, setNames(types, fields), FUN)
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
            unid(resolveUniqueKey(ans, type))
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

fromSolr_default <- function(x, type, query=NULL) {
  if (!is.null(query)) {
    type <- augment(type, query)
  }
  ans <- convertCollection(x, type, fromSolr)
  ans <- resolveIds(ans, type)
  ans <- resolveMeta(ans)
  ans <- ans[,sortFieldNames(fieldNames(ans), type, query),drop=FALSE]
  ans
}

setMethod("fromSolr", c("ANY", "SolrSchema"), fromSolr_default)

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Augmentation: if we request new fields with 'fl', we have
### to add those fields to our schema during conversion.
###

representsSymbol <- function(x) {
    is(x, "Symbol") || (is(x, "TranslationRequest") &&
                        representsSymbol(x@src@expr))
}

augment <- function(x, query) {
  fl <- params(query)$fl
  new.fl <- fl[nzchar(names(fl))]
  computed <- vapply(new.fl, Negate(representsSymbol), logical(1L))
  x <- augmentComputed(x, new.fl[computed])
  x <- augmentAliases(x, new.fl[!computed])
  x
}

augmentAliases <- function(x, fl) {
  fl <- vapply(fl, as.character, character(1L))
  alias.info <- fields(x)[fl]
  names(alias.info) <- names(fl)
  fields(x) <- append(fields(x), alias.info)
  x
}

augmentToInclude <- function(x, fields) {
    existing <- fields %in% fields(x)
    augmentComputed(x, fields[!existing])
}

.logical_funs <- c("and", "or", "xor", "not", "exists", "gt", "gte", "lt",
                   "lte", "eq", "query")

returnsLogical <- function(x) {
    if (is(x, "SolrFunctionCall")) {
        if (x@name == "if")
            returnsLogical(x@args[[2L]]) && returnsLogical(x@args[[3L]])
        else x@name %in% .logical_funs
    } else is.logical(x)
}

augmentComputed <- function(x, fl) {
  if (length(fl) == 0L) {
    return(x)
  }
  typeName <- "..computed.."
  if (is.list(fl)) {
      typeName <- ifelse(vapply(fl, returnsLogical, logical(1L)),
                         "..logical..", "..numeric..")
      fl <- names(fl)
  }
  computed.info <- FieldInfo(name=fl,
                             typeName=typeName,
                             stored=TRUE)
  computed.type <- FieldTypeList(..computed.. = new("AnyField"),
                                 ..logical.. = solrType(logical()),
                                 ..numeric.. = solrType(numeric()))
  fields(x) <- append(fields(x), computed.info)
  fieldTypes(x) <- append(fieldTypes(x), computed.type)
  x
}

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Coercion
###

asAttr <- function(x) {
  ans <- as.character(x)
  if (is.logical(x)) {
    ans <- tolower(ans)
  }
  ans
}

addFieldNodes <- function(xml, schema, dynamic=FALSE) {
  f <- fields(schema)
  f <- f[dynamic(f) == dynamic]
  mapply(function(...) {
    xml$addNode(if (dynamic) "dynamicField" else "field", attrs=list(...))
  }, name=names(f), type=typeName(f), required=asAttr(required(f)),
     indexed=asAttr(indexed(f)), stored=asAttr(stored(f)),
     multiValued=asAttr(multiValued(f)),
     docValues=asAttr(docValues(f)))
}

addDynamicFieldNodes <- function(xml, schema) {
  addFieldNodes(xml, schema, dynamic=TRUE)
}

addCopyFieldNodes <- function(xml, schema) {
  copyEdges <- edges(copyFields(schema))
  if (length(copyEdges) > 0L) {
    copyEdges <- stack(copyEdges)
    apply(copyEdges, 1L, function(e) {
      xml$addNode("copyField", attrs=list(source=e[[2L]], dest=e[[1L]]))
    })
  } else {
    list()
  }
}

addTypeNodes <- function(xml, schema) {
  types <- fieldTypes(schema)
  mapply(function(t, nm) {
    xml$addNode("fieldType",
                attrs=list(name=nm,
                    class=asAttr(class(t)),
                    indexed=asAttr(indexed(t)),
                    stored=asAttr(stored(t)),
                    multiValued=asAttr(multiValued(t))))
  }, types, names(types))
}

setAs("SolrSchema", "XMLDocument", function(from) {
          as(from, "XMLInternalDOM")$doc()
      })

setAs("SolrSchema", "XMLInternalDOM", function(from) {
          xml <- suppressWarnings(xmlTree("schema", attrs=list(name=name(from),
                                       version=as.character(version(from)))))
            if (!is.null(uniqueKey(from)))
                xml$addNode("uniqueKey", uniqueKey(from))
            xml$addNode("fields", close=FALSE)
              addFieldNodes(xml, from)
              addDynamicFieldNodes(xml, from)
            xml$closeTag()
            addCopyFieldNodes(xml, from)
            xml$addNode("types", close=FALSE)
              addTypeNodes(xml, from)
            xml$closeTag()
          xml$closeTag()
          xml
      })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Export
###

setMethod("saveXML", "SolrSchema",
          function(doc, file = NULL, compression = 0, indent = TRUE,
                   prefix = "<?xml version=\"1.0\"?>\n",  doctype = NULL,
                   encoding = getEncoding(doc), ...)
            {
              doc <- as(doc, "XMLDocument")
              callGeneric()
            })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Autogeneration
###

setGeneric("deriveSolrSchema",
           function(x, name=deparse(substitute(x)), ...) {
             force(name)
             standardGeneric("deriveSolrSchema")
           }, signature="x")

isDocValueType <- function(x) {
  is(x, "solr.StrField") ||
    is(x, "solr.TrieIntField") ||
      is(x, "solr.TrieDoubleField")
}

setMethod("deriveSolrSchema", "ANY",
          function(x, name, ...) {
            x <- as.data.frame(x)
            deriveSolrSchema(x, name, ...)
          })

anyEmpty <- function(x) {
    anyNA(x) ||
        (is(solrType(x), "CharacterField") && any(as.character(x) == ""))
}

setMethod("deriveSolrSchema", "data.frame",
          function(x, name, version="1.5",
                   uniqueKey=NULL,
                   required=colnames(Filter(Negate(anyEmpty), x)),
                   indexed=colnames(x), stored=colnames(x),
                   includeVersionField=TRUE)
            {
              if (!isSingleString(name)) {
                stop("'name' must be a single, non-NA string")
              }
              if (!is.null(uniqueKey) && !isSingleString(uniqueKey)) {
                stop("if not NULL, 'uniqueKey' must be a single, non-NA string")
              }
              if (!is.null(uniqueKey) && !uniqueKey %in% colnames(x)) {
                stop("'uniqueKey' does not name a column in 'x'")
              }
              required <- normColIndex(x, required)
              indexed <- normColIndex(x, indexed)
              stored <- normColIndex(x, stored)
              types <- lapply(x, solrType)
              firstClass <- function(xi) class(xi)[1L]
              names(types) <- vapply(x, firstClass, character(1L))
              docValues <- vapply(types, isDocValueType, logical(1L))
              fields <- FieldInfo(names(x),
                                  typeName=names(types),
                                  dynamic=FALSE,
                                  multiValued=vapply(x, is.list, logical(1L)),
                                  required=required,
                                  indexed=indexed,
                                  stored=stored,
                                  docValues=docValues)
              if (!is.null(uniqueKey)) {
                fields[uniqueKey] <- FieldInfo(uniqueKey, typeName="character",
                                               indexed=TRUE, stored=TRUE,
                                               required=TRUE)
                types$character <- solrType(character())
              }
              if (includeVersionField) {
                versionField <- FieldInfo("_version_", typeName="long",
                                          indexed=TRUE, stored=TRUE,
                                          docValues=TRUE)
                fields <- append(fields, versionField)
                types[[typeName(versionField)]] <- new("solr.TrieLongField")
              }
              copyFields <- new("graphNEL")
              types <- types[!duplicated(names(types))]
              SolrSchema(name, as.package_version(version),
                         uniqueKey, fields, copyFields,
                         FieldTypeList(types))
            })

### - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
### Show
###

copyEdgesAsCharacter <- function(object) {
  copyEdges <- edges(copyFields(object))
  if (length(copyEdges) > 0L) {
    copyEdges <- stack(copyEdges)
    paste0(copyEdges[[2L]], "=>", copyEdges[[1L]])
  } else {
    character()
  }
}

setMethod("show", "SolrSchema", function(object) {
  cat("SolrSchema object\n")
  cat(labeledLine("name", name(object), count=FALSE),
      labeledLine("version", version(object), count=FALSE),
      if (!is.null(uniqueKey(object)))
        labeledLine("uniqueKey", uniqueKey(object), count=FALSE),
      labeledLine("fields", name(fields(object))),
      labeledLine("copyFields", copyEdgesAsCharacter(object)),
      sep="")
})
