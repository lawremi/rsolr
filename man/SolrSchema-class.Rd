\name{SolrSchema-class}
\docType{class}

\alias{class:SolrSchema}
\alias{SolrSchema-class}

% Accessors:
\alias{uniqueKey}
\alias{fields}
\alias{fields,SolrSchema-method}
\alias{fieldTypes}
\alias{fieldTypes,SolrSchema-method}
\alias{copyFields}
\alias{name}
\alias{name,ANY-method}

% Generation:
\alias{deriveSolrSchema}
\alias{deriveSolrSchema,ANY-method}
\alias{deriveSolrSchema,data.frame-method}
\alias{saveXML,SolrSchema-method}

% Show:
\alias{show,SolrSchema-method}

\title{SolrSchema}
\description{
  The \code{SolrSchema} object represents the schema of a Solr core.
  Not all of the information in the schema is represented; only the
  relevant elements are included. The user should not need to interact
  with this class very often.

  One can infer a \code{SolrSchema} from a data.frame with
  \code{deriveSolrSchema} and then write it out to a file for use with
  Solr.
}

\section{Accessors}{
  \describe{
    \item{}{
      \code{name(x)}: Gets the name of the schema/dataset.
    }
    \item{}{
      \code{uniqueKey(x)}: Gets the field that serves as the unique key,
      i.e., the document identifier.
    }
    \item{}{
      \code{fields(x, which)}: Gets a \code{\linkS4class{FieldInfo}}
      object, restricted to the fields indicated by \code{which}.
    }
    \item{}{
      \code{fieldTypes(x, fields)}: Gets a
      \code{\linkS4class{FieldTypeList}} object, containing the type
      definition for each field named in \code{fields}.
    }
    \item{}{
      \code{copyFields(x)}: Gets the copy field relationships as
      a \link[graph:graph-class]{graph}.
    }
  }
}

\section{Generation and Export}{
  It may be convenient for R users to autogenerate a Solr schema from a
  prototypical data frame. Note that to harness the full power of Solr,
  it pays to get familiar with the details. After deriving a schema with
  \code{deriveSolrSchema}, save it to the standard XML format with
  \code{saveXML}. See the vignette for an example.
  
  \describe{
    \item{}{
      \code{deriveSolrSchema(x, name, version="1.5", uniqueKey=NULL,
        required=colnames(Filter(Negate(anyEmpty), x)),
        indexed=colnames(x), stored=colnames(x),
        includeVersionField=TRUE)}: Derives a \code{SolrSchema} from a
      data.frame (or data.frame-coercible) \code{x}. The \code{name}
      is taken by quoting \code{x}, by default. Specify a unique key
      via \code{uniqueKey}. The \code{required} fields are those that
      are not allowed to contain missing/empty values. By default, we
      guess that a field is required if it does not contain any NAs or
      empty strings (both are the same as far as Solr is
      concerned). The \code{indexed} and \code{stored} arguments name
      the fields that should be indexed and stored, respectively (see
      Solr docs for details). If \code{includeVersionField} is
      \code{TRUE}, the magic \code{_version_} field is added to the
      schema, and Solr will use it to track document versions, which
      is needed for certain advanced features and generally recommended.
    }
    \item{}{
      \code{saveXML(doc, file = NULL, compression = 0, indent = TRUE,
        prefix = "<?xml version=\"1.0\"?>\n", doctype = NULL, encoding =
        getEncoding(doc), ...)}: Writes the schema to XML. See
      \code{\link[XML]{saveXML}} for more details.
    }
  }
}

\author{ Michael Lawrence }

\keyword{methods}
\keyword{classes}
