\name{DocCollection-class}
\docType{class}

\alias{class:DocCollection}
\alias{DocCollection-class}

% Accessors:
\alias{[,DocCollection-method}
\alias{fieldNames,DocCollection-method}
\alias{ids}
\alias{ids,DocCollection-method}
\alias{ids<-}
\alias{ids<-,DocCollection-method}  
\alias{meta}
\alias{meta,ANY-method}
\alias{unmeta}
\alias{ndoc}
\alias{ndoc,DocCollection-method}
\alias{nfield}
\alias{docs,DocCollection-method}
\alias{nfield,ANY-method}

% Coercion:

% Show:
\alias{show,DocCollection-method}

\title{DocCollection}

\description{
  \code{DocCollection} is a virtual class for all representations of
  document collections. It is made concrete by
  \code{\linkS4class{DocList}} and
  \code{\linkS4class{DocDataFrame}}. This is mostly to achieve an
  abstraction around tabular and list representations of documents.
}

\section{Accessors}{
  These are the accessors that should apply equivalently to any
  derivative of \code{DocCollection}, which provides reasonable default
  implementations for most of them.
  
  \itemize{
    \item{
      \code{ndoc(x)}: Gets the number of documents
    }
    \item{
      \code{nfield(x)}: Gets the number of fields
    }
    \item{
      \code{ids(x), ids(x) <- value}: Gets or sets the document unique
      identifiers (may be \code{NULL})
    }
    \item{
      \code{fieldNames(x, includeStatic=TRUE, ...)}: Gets the field names
    }
    \item{
      \code{docs(x)}: Just returns \code{x}, as \code{x} already
      represents a set of documents
    }
    \item{
      \code{meta(x)}: Gets an auxillary collection of \dQuote{meta}
      fields that hold fields that describe, rather than compose, the
      documents. This feature should be considered unstable. Stay away
      for now.
    }
    \item{
      \code{unmeta(x)}: Clears the metadata.
    }
  }
}

\author{ Michael Lawrence }

\seealso{
  \code{\linkS4class{DocList}} and \code{\linkS4class{DocDataFrame}} for
  concrete implementations
}

\keyword{methods}
\keyword{classes}
