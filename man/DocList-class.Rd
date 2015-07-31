\name{DocList-class}
\docType{class}

\alias{class:DocList}
\alias{DocList-class}

% Accessors:
\alias{[,DocList-method}
\alias{[<-,DocList,ANY,ANY,ANY-method}
\alias{fieldNames,DocList-method}
\alias{ids<-,DocList-method}
\alias{names<-,DocList,ANY-method}
\alias{meta,DocList-method}
\alias{nfield,DocList-method}

% Coercion:
\alias{as.data.frame,DocList-method}

% Show:
\alias{show,DocList-method}

\title{DocList}

\description{
  The \code{DocList} object wraps a \code{list} in a document-oriented
  interface that is shared with \code{\linkS4class{DocDataFrame}}. This
  is mostly to achieve an abstraction around tabular and list
  representations of documents. \code{DocList} should behave just like a
  \code{list}, except it adds the accessors described below.
}

\section{Accessors}{
  These are some accessors that \code{DocList} adds on top of the
  basic list accessors. Using these accessors allows code to be
  agnostic to whether the data are stored as a list or data.frame.
  
  \describe{
    \item{}{
      \code{ndoc(x)}: Gets the number of documents (elements)
    }
    \item{}{
      \code{nfield(x)}: Gets the number of unique field names over all
      of the documents
    }
    \item{}{
      \code{ids(x), ids(x) <- value}: Gets or sets the document unique
      identifiers (may be \code{NULL}, treated as names)
    }
    \item{}{
      \code{fieldNames(x, includeStatic=TRUE, ...)}: Gets the set of
      unique field names
    }
    \item{}{
      \code{meta(x)}: Gets an auxillary list of \dQuote{meta} documents
      (lists) that hold fields that describe, rather than compose, the
      actual documents. This feature should be considered unstable. Stay
      away for now.
    }
    \item{}{
      \code{unmeta(x)}: Clears the metadata.
    }
  }
}

\author{ Michael Lawrence }

\seealso{
  \code{\linkS4class{DocDataFrame}} for representing a document collection as
  a table instead of a list
}

\keyword{methods}
\keyword{classes}
