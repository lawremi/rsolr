\name{DocDataFrame-class}
\docType{class}

\alias{class:DocDataFrame}
\alias{DocDataFrame-class}

% Accessors:
\alias{[,DocDataFrame-method}
\alias{fieldNames,DocDataFrame-method}
\alias{ids<-,DocDataFrame-method}

% Show:
\alias{show,DocDataFrame-method}

\title{DocDataFrame}

\description{
  The \code{DocDataFrame} object wraps a \code{data.frame} in a
  document-oriented interface that is shared with
  \code{\linkS4class{DocList}}. This is mostly to achieve an abstraction
  around tabular and list representations of
  documents. \code{DocDataFrame} should behave just like a
  \code{data.frame}, except it adds the accessors described below.
}

\section{Accessors}{
  These are some accessors that \code{DocDataFrame} adds on top of the
  basic data frame accessors. Using these accessors allows code to be
  agnostic to whether the data are stored as a list or data.frame.
  
  \itemize{
    \item{
      \code{ndoc(x)}: Gets the number of documents (rows)
    }
    \item{
      \code{nfield(x)}: Gets the number of fields (columns)
    }
    \item{
      \code{ids(x), ids(x) <- value}: Gets or sets the document unique
      identifiers (may be \code{NULL}, treated as rownames)
    }
    \item{
      \code{fieldNames(x, includeStatic=TRUE, ...)}: Gets the field
      (column) names
    }
    \item{
      \code{docs(x)}: Just returns \code{x}, as \code{x} already
      represents a set of documents
    }
    \item{
      \code{meta(x)}: Gets an auxillary data.frame of \dQuote{meta}
      columns that hold fields that describe, rather than compose, the
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
  \code{\linkS4class{DocList}} for representing a document collection as
  a list instead of a table
}

\keyword{methods}
\keyword{classes}
