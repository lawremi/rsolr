\name{SolrPromise-class}
\docType{class}

\alias{class:SolrPromise}
\alias{SolrPromise-class}

\alias{class:SolrLucenePromise}
\alias{SolrLucenePromise-class}

\alias{class:SolrFunctionPromise}
\alias{SolrFunctionPromise-class}

\alias{class:SolrReducePromise}
\alias{SolrReducePromise-class}

\alias{class:SolrAggregatePromise}
\alias{SolrAggregatePromise-class}

\alias{class:SolrSymbolPromise}
\alias{SolrSymbolPromise-class}

\alias{class:SolrLuceneSymbolPromise}
\alias{SolrLuceneSymbolPromise-class}

\alias{class:PredicatedSolrSymbolPromise}
\alias{PredicatedSolrSymbolPromise-class}

% Vector API:
\alias{length,SolrPromise-method}
\alias{lengths,SolrSymbolPromise-method}
\alias{lengths,SolrPromise-method}
\alias{\%in\%,SolrSymbolPromise,PredicatedSolrSymbolPromise-method}
\alias{\%in\%,SolrSymbolPromise,SolrSymbolPromise-method}
\alias{\%in\%,SolrSymbolPromise,vector-method}
\alias{!,SolrAggregatePromise-method}
\alias{!,SolrFunctionPromise-method}
\alias{!,SolrLuceneSymbolPromise-method}
\alias{!,SolrPromise-method}
\alias{-,SolrPromise,missing-method}
\alias{Arith,SolrAggregatePromise,SolrAggregatePromise-method}
\alias{Arith,SolrAggregatePromise,numeric-method}
\alias{Arith,SolrPromise,SolrPromise-method}
\alias{Arith,SolrPromise,logical-method}
\alias{Arith,SolrPromise,numeric-method}
\alias{Arith,logical,SolrPromise-method}
\alias{Arith,numeric,SolrAggregatePromise-method}
\alias{Arith,numeric,SolrPromise-method}
\alias{Compare,AsIs,SolrSymbolPromise-method}
\alias{Compare,SolrAggregatePromise,SolrAggregatePromise-method}
\alias{Compare,SolrAggregatePromise,numeric-method}
\alias{Compare,SolrPromise,SolrPromise-method}
\alias{Compare,SolrPromise,numeric-method}
\alias{Compare,SolrSymbolPromise,AsIs-method}
\alias{Compare,SolrSymbolPromise,numeric-method}
\alias{Compare,SolrSymbolPromise,vector-method}
\alias{Compare,numeric,SolrAggregatePromise-method}
\alias{Compare,numeric,SolrPromise-method}
\alias{Compare,numeric,SolrSymbolPromise-method}
\alias{Compare,vector,SolrSymbolPromise-method}
\alias{IQR,SolrPromise-method}
\alias{Logic,SolrAggregatePromise,SolrAggregatePromise-method}
\alias{Logic,SolrAggregatePromise,logical-method}
\alias{Logic,SolrFunctionPromise,SolrFunctionPromise-method}
\alias{Logic,SolrLucenePromise,SolrLucenePromise-method}
\alias{Logic,SolrLucenePromise,SolrPromise-method}
\alias{Logic,SolrLucenePromise,SolrSymbolPromise-method}
\alias{Logic,SolrPromise,SolrLucenePromise-method}
\alias{Logic,SolrPromise,SolrSymbolPromise-method}
\alias{Logic,SolrPromise,logical-method}
\alias{Logic,SolrSymbolPromise,SolrLucenePromise-method}
\alias{Logic,SolrSymbolPromise,SolrPromise-method}
\alias{Logic,SolrSymbolPromise,SolrSymbolPromise-method}
\alias{Logic,logical,SolrAggregatePromise-method}
\alias{Logic,logical,SolrPromise-method}
\alias{Math,SolrAggregatePromise-method}
\alias{Math,SolrPromise-method}
\alias{Summary,SolrPromise-method}
\alias{[,SolrPromise-method}
\alias{[,SolrSymbolPromise-method}
\alias{[<-,Promise,ANY,ANY,ANY-method}
\alias{[<-,SolrPromise,SolrPromise,ANY,ANY-method}
\alias{anyNA,SolrPromise-method}
\alias{complete.cases,SolrFunctionPromise-method}
\alias{complete.cases,SolrLuceneSymbolPromise-method}
\alias{ftable,SolrSymbolPromise-method}
\alias{grep,ANY,SolrSymbolPromise-method}
\alias{grepl,character,SolrSymbolPromise-method}
\alias{head,SolrPromise-method}
\alias{ifelse,SolrAggregatePromise-method}
\alias{ifelse,SolrPromise-method}
\alias{intersect,SolrSymbolPromise,SolrSymbolPromise-method}
\alias{is.na,SolrAggregatePromise-method}
\alias{is.na,SolrFunctionPromise-method}
\alias{is.na,SolrLuceneSymbolPromise-method}
\alias{mad,SolrPromise-method}
\alias{mean,SolrPromise-method}
\alias{median,SolrPromise-method}
\alias{ndoc,SolrPromise-method}
\alias{nunique}
\alias{nunique,ANY-method}
\alias{nunique,SolrPromise-method}
\alias{nunique,factor-method}
\alias{pmax2,SolrAggregatePromise,SolrAggregatePromise-method}
\alias{pmax2,SolrAggregatePromise,numeric-method}
\alias{pmax2,SolrPromise,SolrPromise-method}
\alias{pmax2,SolrPromise,numeric-method}
\alias{pmax2,numeric,SolrAggregatePromise-method}
\alias{pmax2,numeric,SolrPromise-method}
\alias{pmax2,ANY,ANY-method}
\alias{pmin2,ANY,ANY-method}
\alias{pmin2,SolrAggregatePromise,SolrAggregatePromise-method}
\alias{pmin2,SolrAggregatePromise,numeric-method}
\alias{pmin2,SolrPromise,SolrPromise-method}
\alias{pmin2,SolrPromise,numeric-method}
\alias{pmin2,numeric,SolrAggregatePromise-method}
\alias{pmin2,numeric,SolrPromise-method}
\alias{quantile,SolrPromise-method}
\alias{rescale}
\alias{rescale,SolrPromise-method}
\alias{round,SolrAggregatePromise-method}
\alias{round,SolrPromise-method}
\alias{sd,SolrPromise-method}
\alias{setdiff,SolrSymbolPromise,SolrSymbolPromise-method}
\alias{signif,SolrPromise-method}
\alias{summary,SolrPromise-method}
\alias{summary.SolrPromise}
\alias{table,SolrSymbolPromise-method}
\alias{tail,SolrPromise-method}
\alias{union,SolrSymbolPromise,SolrSymbolPromise-method}
\alias{unique,PredicatedSolrSymbolPromise-method}
\alias{unique,SolrSymbolPromise-method}
\alias{var,SolrPromise,ANY-method}
\alias{weighted.mean,SolrPromise,SolrPromise-method}
\alias{window,SolrPromise-method}
\alias{windows,SolrPromise-method}

% Coercion:
\alias{as.data.frame,SolrPromise-method}
\alias{fulfill,PredicatedSolrSymbolPromise-method}

% Should be in BiocGenerics:
\alias{ftable}
\alias{mad}
\alias{pmax}
\alias{pmax2}
\alias{pmin}
\alias{pmin2}

\title{SolrPromise}
\description{
  \code{SolrPromise} is a vector-like representation of a deferred
  computation within Solr. It may promise to simply return a field, to
  perform arithmetic on a combination of fields, to aggregate a field,
  etc. Methods on \code{SolrPromise} allow the R user to
  manipulate Solr data with the ordinary R API. The typical way to
  fulfill a promise is to explicitly coerce the promise to a
  materialized data type, such as an R vector.
}

\details{
  In general, \code{SolrPromise} acts just like an R vector. It supports
  all of the basic vector manipulations, including the
  \code{\link{Logic}}, \code{\link{Compare}}, \code{\link{Arith}},
  \code{\link{Math}}, and \code{\link{Summary}} group generics, as well
  as \code{length}, \code{lengths}, \code{\%in\%},
  \code{complete.cases}, \code{is.na}, \code{[}, \code{grepl},
  \code{grep}, \code{round}, \code{signif}, \code{ifelse},
  \code{pmax}, \code{pmin},
  \code{cut}, \code{mean}, \code{quantile}, \code{median},
  \code{weighted.mean}, \code{IQR}, \code{mad}, \code{anyNA}. All of
  these functions are lazy, in that they return another promise.

  The promise is really only known to rsolr, as all actual Solr queries
  are eager. \code{SolrPromise} does its best to defer computations, but
  the computations will be forced if one performs an operation that is
  not supported by Solr.

  These functions are also supported, but they are eager: \code{cbind},
  \code{rbind}, \code{summary}, \code{window},
  \code{head}, \code{tail}, \code{unique}, \code{intersect},
  \code{setdiff}, \code{union}, \code{table} and \code{ftable}. These
  functions from the \code{Math} group generic are eager: \code{cummax},
  \code{cummin}, \code{cumprod}, \code{cumsum}, \code{log2}, and
  \code{*gamma}.

  The \code{[<-} function will be lazy as long as both \code{x} and
  \code{i} are promises. \code{i} is assumed to represent a logical
  subscript. Otherwise, \code{[<-} is eager.

  \code{SolrPromise} also extends the R API with some new operations:
  \code{nunique} (number of unique elements), \code{rescale} (rescale
  to within a min/max), \code{\link{ndoc}}, \code{\link{windows}},
  \code{\link{heads}}, \code{\link{tails}}.
}

\section{Limitations}{
  This section outlines some limitations of \code{SolrPromise} methods,
  compared to the base vector implementation. The primary limitation is
  that binary operations generally only work between two promises that
  derive from the same data source, including all pending manipulations
  (filters, ordering, etc). Operations between a promise and an ordinary
  vector usually only work if the vector is of length one (a scalar).

  Some specific notes:
  \describe{
    \item{}{\code{x[i]}: The index \code{i} is ideally a promise. The
      return value will be restricted such that it will only combine
      with promises with the same restriction.
    }
    \item{}{\code{x \%in\% table}: The \code{x} argument must always
      refer to a simple field, and the \code{table} argument should be
      either a field, potentially predicated via \code{table[i]} (where
      the index \code{i} is a promise), or a \dQuote{short} vector.
    }
    \item{}{\code{grepl(pattern, x, fixed = FALSE)}: Applies when
      \code{x} is a promise. Besides \code{pattern}, only the
      \code{fixed} argument is supported from the base function.
    }
    \item{}{\code{grep(pattern, x, value = FALSE, fixed = FALSE, invert
      = FALSE)}: One must always set \code{value=TRUE}. Beyond that,
      only \code{fixed} and \code{invert} are supported from the base
      function.
    }
    \item{}{\code{cut(x, breaks, include.lowest = FALSE, right = TRUE)}:
      Only supports uniform (constant separation) breaks.
    }
    \item{}{\code{mad(x, center = median(x, na.rm=na.rm), constant =
        1.4826, na.rm = FALSE, low = FALSE, high = FALSE)}: The
      \code{low} and \code{high} parameters must be \code{FALSE}. If
      there any NAs, then \code{na.rm} must be \code{TRUE}. Does not
      work when the context is grouped.
    }
  }
}

\author{ Michael Lawrence }

\seealso{
  \code{\linkS4class{SolrFrame}}, which yields promises when it is
  \code{defer}red.
}

\keyword{methods}
\keyword{classes}
