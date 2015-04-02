\name{Solr-class}
\docType{class}

\alias{class:Solr}
\alias{Solr-class}

% Accessors:
\alias{nrow,Solr-method}

% Constructor:
\alias{Solr}

% Coercion:
\alias{as.data.frame.Solr}
\alias{as.data.frame,Solr-method}
\alias{coerce,Solr,DataFrame-method}

% Subsetting:
\alias{[,Solr-method}
\alias{[[,Solr-method}
\alias{[[<-,Solr-method}
\alias{$<-,Solr-method}

% Utilities:

% Show:
\alias{show,Solr-method}

\title{Solr Dataset}
\description{
  \code{Solr} ...
}

\details{
}

\section{Accessor methods}{
  In the code snippets below, \code{x} is a \code{Solr} object.

  The following accessors treat the data as a contiguous dataset,
  ignoring the division into spaces:
  \describe{
    \item{}{Array accessors:
      \describe{
        \item{}{
          \code{nrow(x)}: The number of ranges in \code{x}.
        }
      }
    }
  }
}

\section{Constructor}{
  \describe{
    \item{}{
      \code{}:
    }
  }
}

\section{Coercion}{
  \describe{
    \item{}{
      \code{as.data.frame(x, row.names=NULL, optional=FALSE, ...)}:
      Copy the start, end, width of the ranges and all of the variables
      as columns in a \code{data.frame}. This is a bridge to existing
      functionality in R, but of course care must be taken if the data
      is large. Note that \code{optional} and \code{...} are ignored.
    }
    \item{}{
      \code{as(from, "DataFrame")}: Like \code{as.data.frame} above,
      except the result is an \code{\linkS4class{DataFrame}} and it
      probably involves less copying, especially if there is only a
      single space.
    }
  }
}

\section{Subsetting and Replacement}{
  In the code snippets below, \code{x} is a \code{Solr} object.

  \describe{
    \item{}{
      \code{x[i]}:
    }
    \item{}{
      \code{x[i,j]}:
    }
    \item{}{
      \code{x[[i]]}:
    }
    \item{}{
      \code{x$name}: similar to above, where \code{name} is taken
      literally as a column name in the data.
    }
    \item{}{
      \code{x[[i]] <- value}:
    }
    \item{}{
      \code{x$name <- value}: similar to above, where \code{name} is taken
      literally as a column name in the data.
    }
  }
}

\author{ Michael Lawrence }

\seealso{
}

\examples{
}

\keyword{methods}
\keyword{classes}
