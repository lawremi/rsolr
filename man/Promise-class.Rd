\name{Promise-class}
\docType{class}

\alias{class:Promise}
\alias{Promise-class}

\alias{class:SimplePromise}
\alias{SimplePromise-class}

% Coercion:
\alias{as.character,Promise-method}
\alias{as.integer,Promise-method}
\alias{as.logical,Promise-method}
\alias{as.numeric,Promise-method}
\alias{fulfill}
\alias{fulfill,Promise-method}
\alias{fulfill,ANY-method}

% Default implementations that force:
\alias{cbind2,ANY,Promise-method}
\alias{cbind2,Promise,ANY-method}
\alias{cbind2,Promise,Promise-method}
\alias{rbind2,ANY,Promise-method}
\alias{rbind2,Promise,ANY-method}
\alias{rbind2,Promise,Promise-method}

% Construction:
\alias{Promise}

% Show:

\title{Promises}
\description{
  The \code{Promise} class formally and abstractly represents the
  potential result of a deferred computation. 
}

\details{
  Lazy programming is useful in a number of contexts, including
  interaction with external/remote systems like databases, where we want
  the computation to occur within the external system, despite
  appearances to the contrary. Typically, the user constructs one or
  more promises referring to pre-existing objects. Operations on those
  objects produce new promises that encode the additional
  computations. Eventually, usually after some sort of restriction
  and/or aggregation, the promise is \dQuote{fulfilled} to yield a
  materialized, eager object, such as an R vector.

  \code{Promise} and its partial implementation \code{SimplePromise}
  provide a foundation for implementations that mostly helps with
  creating and fulfilling promises, while the implementation is
  responsible for deferring particular computations, which is
  language-dependent.
}

\section{Construction}{
  \describe{
    \item{}{
      \code{Promise(expr, context, ...)}: A generic constructor that
      dispatches on \code{expr} to construct a \code{Promise} object,
      the specific type of which corresponds to the language of
      \code{expr}. The \code{context} argument should be a
      \code{Context} object, in which \code{expr} will be evaluated when
      the promise is fulfilled. The \code{\dots} are passed to methods.
    }
  }
}

\section{Fulfillment}{
  \describe{
    \item{}{
      \code{fulfill(x)}: Fulfills the promise by evaluating the deferred
      computation and returning a materialized object.
    }
  }

  The basic coercion functions in R, like \code{as.vector} and
  \code{as.data.frame}, have methods for \code{Promise} that simply call
  \code{fulfill} on the promise, and then perform the coercion. Coercion
  is preferred to calling \code{fulfill} directly.
}

\author{ Michael Lawrence }

\keyword{methods}
\keyword{classes}
