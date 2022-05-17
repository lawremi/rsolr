\name{FieldInfo-class}
\docType{class}

\alias{class:FieldInfo}
\alias{FieldInfo-class}

% Accessors:
\alias{dynamic}
\alias{multiValued}
\alias{typeName}
\alias{indexed}
\alias{stored}
\alias{required}
\alias{docValues}
\alias{length,FieldInfo-method}
\alias{names,FieldInfo-method}
\alias{names<-,FieldInfo,ANY-method}
\alias{\%in\%,character,FieldInfo-method}

% Restriction and manipulation:
\alias{[,FieldInfo-method}
\alias{[<-,FieldInfo,ANY,ANY,FieldInfo-method}

% Combination:
\alias{append,FieldInfo,FieldInfo-method}

% Coercion:
\alias{as.data.frame.FieldInfo}
\alias{as.data.frame,FieldInfo-method}
\alias{coerce,data.frame,FieldInfo-method}
\alias{as.list.FieldInfo}
\alias{as.list,FieldInfo-method}

% Show:
\alias{show,FieldInfo-method}

\title{FieldInfo}
\description{
  The \code{FieldInfo} object is a vector of field entries from the Solr
  schema. Typically, one retrieves an instance with \code{\link{fields}}
  and shows it on the console to get an overview of the schema. The
  vector-like nature means that functions like \code{[}  and
  \code{length} behave as expected.
}

\section{Accessors}{
  These functions get the \dQuote{columns} from the field information
  \dQuote{table}:
  \itemize{
    \item{
      \code{name(x)}: Gets the name of the field.
    }
    \item{
      \code{typeName(x)}: Gets the name of the field type, see
      \code{\link{fieldTypes}}.
    }
    \item{
      \code{dynamic(x)}: Gets whether the field is dynamic, i.e.,
      whether its name is treated as a wildcard glob. If a document
      field does not match a static field name, it takes its
      properties from the first dynamic field (in schema order) that it
      matches.
    }
    \item{
      \code{multiValued(x)}: Gets whether the field accepts multiple
      values. A multi-valued field is manifested in R as a list.
    }
    \item{
      \code{required(x)}: Gets whether the field must have a value in
      every document. A non-required field will sometimes have NAs. This
      is useful for both ensuring data integrity and optimizations.
    }
    \item{
      \code{indexed(x)}: Gets whether the field has been indexed. A
      field must be indexed for us to filter by it. Faceting requires a
      field to be indexed or have doc values.
    }
    \item{
      \code{stored(x)}: Gets whether the data for a field have been
      stored in the database. We can search on any (indexed) field, but
      we can only retrieve data from stored fields.
    }
    \item{
      \code{docValues(x)}: Gets whether the data have been additionally
      stored in a columnar format that accelerates Solr function calls
      (\code{transform}) and faceting (\code{aggregate}).
    }
  }
}

\section{Utilities}{
  \itemize{
    \item{
      \code{x \%in\% table}: Returns whether each field name in \code{x}
      matches a field defined in \code{table}, a \code{FieldInfo}
      object. This convenience is particularly needed when the schema
      contains dynamic fields.
    }
  }
}

\author{ Michael Lawrence }

\seealso{
  \code{\linkS4class{SolrSchema}} that holds an instance of this object
}

\keyword{methods}
\keyword{classes}
