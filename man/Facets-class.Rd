\name{Facets-class}
\docType{class}

\alias{class:Facets}
\alias{Facets-class}

% Accessors:
\alias{stats}
\alias{[,Facets-method}
\alias{[[,Facets-method}
\alias{[[,Facets,formula-method}

% Coercion:
\alias{as.table.Facets}
\alias{as.table,Facets-method}

% Show:
\alias{show,Facets-method}

\title{Facets}
\description{
  The \code{Facets} object represents the result of a Solr facet
  operation and is typically obtained by calling \code{\link{facets}} on
  a \code{SolrCore}. Most users should just call
  \code{\link[=Solr-class]{aggregate}} or \code{xtabs} instead of
  directly manipulating \code{Facets} objects.
}

\details{
  \code{Facets} extends \code{list} and each node adds a grouping factor
  to the set defined by its ancestors. In other words, parent-child
  relationships represent interactions between factors. For example,
  \code{x$a$b} gets the node corresponding to the interaction of
  \code{a} and \code{b}.

  In a single request to Solr, statistics may be calculated for multiple
  interactions, and they are stored as a \code{data.frame} at the
  corresponding node in the tree. To retrieve them, call the
  \code{stats} accessor, e.g., \code{stats(x$a$b)}, or \code{as.table}
  for getting the counts as a table (Solr always computes the counts).
}

\section{Accessors}{
  \describe{
    \item{}{
      \code{x$name}, \code{x[[i]]}: Get the node that further groups by
      the named factor. The \code{i} argument can be a formula, where
      \code{[[} will recursively extract the corresponding element.
    }
    \item{}{
      \code{x[i]}: Extract a new \code{Facets} object, restricted to the
      named groupings.
    }
    \item{}{
      \code{stats(x)}: Gets the statistics at the current facet level.
    }
  }
}

\section{Coercion}{
  \describe{
    \item{}{\code{as.table(x)}: Converts the current node to a
      table of conditional counts.
    }
  }
}

\author{ Michael Lawrence }

\seealso{
  \code{\link[=Solr-class]{aggregate}} for a simpler interface that
  computes statistics for only a single interaction  
}

\keyword{methods}
\keyword{classes}
