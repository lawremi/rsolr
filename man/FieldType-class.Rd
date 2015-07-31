\name{FieldType-class}
\docType{class}

\alias{class:FieldType}
\alias{FieldType-class}

\alias{class:FieldTypeList}
\alias{FieldTypeList-class}

% Accessors:
\alias{[,FieldTypeList-method}
\alias{names<-,FieldTypeList,ANY-method}

% Coercion:
\alias{as.data.frame,FieldTypeList-method}

% Show:
\alias{show,FieldType-method}
\alias{show,FieldTypeList-method}

\title{FieldType}
\description{
  The \code{FieldType} object represents the type of a document field. A
  list of these objects is formally represented as \code{FieldTypeList}
  object, an instance of which is provided by
  \code{\linkS4class{SolrSchema}}. Internally, \code{FieldType} objects
  are central to the conversion between \R and Solr types. At the user
  level, they are mostly useful for displaying the schema.
}

\author{ Michael Lawrence }

\seealso{
  \code{\linkS4class{SolrSchema}}, which communicates information on
  field types using these classes
}

\keyword{methods}
\keyword{classes}
