\name{Grouping-class}
\docType{class}

\alias{class:Grouping}
\alias{Grouping-class}

% Accessors:
\alias{ngroup,Grouping-method}
\alias{schema,Grouping-method}

% Coercion:
\alias{coerce,Grouping,list-method}
\alias{coerce,Grouping,data.frame-method}

% Show:
\alias{show,Grouping-method}

\title{Grouping}
\description{
  The \code{Grouping} object represents a collection of documents split
  by some interaction of factors. It is extremely low-level, and its
  only use is to be coerced to something else, either a \code{list} or
  \code{data.frame}, via \code{as}.
}

\author{ Michael Lawrence }

\seealso{
  \code{\linkS4class{ListSolrResult}}, which provides this object via
  its \code{groupings} method.
}

\keyword{methods}
\keyword{classes}
