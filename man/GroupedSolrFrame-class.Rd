\name{GroupedSolrFrame-class}
\docType{class}

\alias{class:GroupedSolrFrame}
\alias{GroupedSolrFrame-class}

% Accessors:
\alias{grouping}
\alias{grouping,GroupedSolrFrame-method}
\alias{ndoc,GroupedSolrFrame-method}
\alias{rownames,GroupedSolrFrame-method}
\alias{nrow,GroupedSolrFrame-method}
\alias{[<-,GroupedSolrFrame,ANY,ANY,ANY-method}
\alias{[,GroupedSolrFrame-method}
\alias{ngroup}
\alias{ngroup,GroupedSolrFrame-method}

% Manipulation:
\alias{group,GroupedSolrFrame,ANY-method}
\alias{ungroup}
\alias{ungroup,GroupedSolrFrame-method}
\alias{ungroup,data.frame-method}
\alias{ungroup,ANY-method}
\alias{window,GroupedSolrFrame-method}
\alias{head,GroupedSolrFrame-method}
\alias{tail,GroupedSolrFrame-method}
\alias{windows}
\alias{windows,GroupedSolrFrame-method}
\alias{heads}
\alias{heads,ANY-method}
\alias{tails}
\alias{tails,ANY-method}
\alias{unique,GroupedSolrFrame-method}
  
% Coercion:
\alias{as.data.frame,GroupedSolrFrame-method}
\alias{coerce,GroupedSolrFrame,data.frame-method}

% Show:
\alias{show,GroupedSolrFrame-method}

\title{GroupedSolrFrame}
\description{
  The \code{GroupedSolrFrame} is a \emph{highly experimental} extension
  of \code{\linkS4class{SolrFrame}} that models each column as a list,
  formed by splitting the original vector by a common set of grouping
  factors.
}

\details{
  A \code{GroupedSolrFrame} should more or less behave analogously to a
  data frame where every column is split by a common grouping. Unlike
  \code{SolrFrame}, columns are \emph{always} extracted lazily. Typical
  usage is to construct a \code{GroupedSolrFrame} by calling
  \code{group} on a \code{SolrFrame}, and then to extract columns (as
  promises) and aggregate them (by e.g. calling \code{mean}).

  Functions that group the data, such as \code{group} and
  \code{aggregate}, simply add to the existing grouping. To clear the
  grouping, call \code{ungroup} or just coerce to a \code{SolrFrame} or
  \code{SolrList}.
}

\section{Accessors}{
  As \code{GroupedSolrFrame} inherits much of its functionality from
  \code{SolrFrame}; here we only outline concerns specific to grouped
  data.
  
  \itemize{
    \item{
      \code{ndoc(x)}: Gets the number of documents per group
    }
    \item{
      \code{rownames(x)}: Forms unique group identifiers by
      concatenating the grouping factor values.
    }
    \item{
      \code{x[i, j] <- value}: Inserts \code{value} into the Solr
      core, where \code{value} is a data.frame of lists, or just a list
      (representing a single column). Preferably, \code{i} is a promise,
      because we need to the IDs of the selected documents in order to
      perform the atomic update, and the promise lets us avoid
      downloading \emph{all} of the IDs. But otherwise, if \code{i} is
      atomic, then it indexes into the groups. If \code{i} is a list,
      then its names are matched to the group names, and its elements
      index into the matching group. The list does not need to be named
      if the elements are character vectors (and thus represent document
      IDs).
    }
    \item{
      \code{x[i, j, drop=FALSE]}: Extracts data from \code{x}, as usual,
      but see the entry immediate above this one for the expectations of
      \code{i}. Try to make it a promise, so that we do not need to
      download IDs and then try to serialize them into a query, which
      has length limitations.
    }
  }
}

\section{Extended API}{
  Most of the typical data frame accessors and data manipulation
  functions will work analogously on \code{GroupedSolrFrame} (see
  Details). Below, we list some of the non-standard methods that might
  be seen as an extension of the data frame API.

  \itemize{
    \item{\code{heads(x, n)}, \code{tails(x, n)}, \code{windows(x,
      start, end)}: Perform \code{head}, \code{tail} or \code{window} on
      each group separately, returning a data.frame with grouped (list)
      columns.
    }
    \item{\code{ngroup(x)}: The number of groups, i.e., the number of
      rows.
    }
  }
}

\author{ Michael Lawrence }

\keyword{methods}
\keyword{classes}
