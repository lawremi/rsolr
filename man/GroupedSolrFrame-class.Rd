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
\alias{[<-,GroupedSolrFrame-method}
\alias{[,GroupedSolrFrame-method}

% Manipulation:
\alias{group,GroupedSolrFrame,ANY-method}
\alias{ungroup}
\alias{ungroup,GroupedSolrFrame-method}
\alias{ungroup,data.frame-method}
\alias{window,GroupedSolrFrame-method}
\alias{head,GroupedSolrFrame-method}
\alias{tail,GroupedSolrFrame-method}
\alias{windows,GroupedSolrFrame-method}
\alias{heads,GroupedSolrFrame-method}
\alias{tails,GroupedSolrFrame-method}
\alias{unique,GroupedSolrFrame-method}

% Coercion:
\alias{as.data.frame,GroupedSolrFrame-method}
\alias{coerce,Solr,data.frame-method}

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
}

\section{Accessors}{
  As \code{GroupedSolrFrame} inherits much of its functionality from
  \code{SolrFrame}; here we only outline concerns specific to grouped
  data.
  
  \describe{
    \item{}{
      \code{ndoc(x)}: Gets the number of documents per group
    }
    \item{}{
      \code{rownames(x)}: Forms unique group identifiers by
      concatenating the grouping factor values.
    }
    \item{}{
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
    \item{}{
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

  \describe{
    \item{}{\code{aggregate(x, data, FUN, ..., subset, na.action,
        simplify = TRUE, count = FALSE)}: If \code{x} is a formula,
        aggregates \code{data}, grouping by \code{x}, by either applying
        \code{FUN}, or evaluating an aggregating expression in \dots, on
        each group. If \code{count} is \code{TRUE}, a \dQuote{count}
        column is added with the number of elements in each group. The
        rest of the arguments behave like those for the base
        \code{\link{aggregate}}.

        There are two main modes: aggregating with \code{FUN}, or, as an
        extension to the base \code{aggregate}, aggregating with
        expressions in \code{\dots}, similar to the interface for
        \code{transform}. If \code{FUN} is specified, then behavior is
        much like the original, except one can omit the LHS on the
        formula, in which case the entire frame is passed to
        \code{FUN}. In the second mode, there is a column in the result
        for each argument in \dots, and there must not be an LHS on the
        formula.

        See the documentation for the underlying \code{\link{facet}}
        function for details on what is supported on the formula RHS.

        For global aggregation, simply pass the \code{GroupedSolrFrame} as
        \code{x}, in which case the \code{data} argument does not exist.

        Note that the function or expressions are only
        \emph{conceptually} evaluated on each group. In reality, the
        computations occur on grouped columns/promises, which are
        modeled as lists. Thus, there is potential for conflict, in
        particular with \code{length}, which return the number of
        groups, instead of operating group-wise. One should use the
        abstraction \code{ndoc} instead of \code{length}, since
        \code{ndoc} always returns document counts, and thus will return
        the size of each group.
    }
    \item{}{\code{rename(x, ...)}: Renames the columns of \code{x},
      where the names and character values of \dots indicates the
      mapping (\code{newname = oldname}). 
    }
    \item{}{\code{group(x, by)}: Returns a
      \code{\linkS4class{GroupedGroupedSolrFrame}} that is grouped by the
      factors in \code{by}, typically a formula.
    }
    \item{}{\code{defer(x)}: Returns a \code{GroupedSolrFrame} that yields
      \code{\linkS4class{SolrPromise}} objects instead of vectors
      whenever a field is retrieved
    }
  }
}

\section{Constructor}{
  \describe{
    \item{}{
      \code{GroupedSolrFrame(uri, ...)}:
      Constructs a new \code{GroupedSolrFrame} instance, representing a Solr
      core located at \code{uri}, which should be a string or a
      \code{\link[restfulr]{RestUri}} object. If a string, then the
      \dots are passed to the \code{RestUri} constructor.
    }
  }
}


\section{Evaluation}{
  \describe{
    \item{}{
      \code{eval(expr, envir, enclos)}: Evaluates \code{expr} in the
      \code{GroupedSolrFrame} \code{envir}, using \code{enclos} as the
      enclosing environment. The \code{expr} can be an R language object
      or a \code{SolrExpression}, either of which are lazily evaluated
      if \code{defer} has been called on \code{envir}.
    }
  }
}

\section{Coercion}{
  \describe{
    \item{}{
      \code{as.data.frame(x, row.names=NULL, optional=FALSE, fill=TRUE)}:
      Downloads the data into an actual data.frame, specifically an
      instance of \code{\linkS4class{DocDataFrame}}. If \code{fill} is
      FALSE, only the fields represented in at least one document are
      added as columns. 
    }
    \item{}{
      \code{as.list(x)}: Essentially \code{as.list(as.data.frame(x))},
      except returns a list of promises if \code{x} is deferred.
    }
  }
}

\author{ Michael Lawrence }

\keyword{methods}
\keyword{classes}
