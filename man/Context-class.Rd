\name{Context-class}
\docType{class}

\alias{class:Context}
\alias{Context-class}

% Translation:
\alias{symbolFactory}
\alias{symbolFactory,DelegateContext-method}
\alias{symbolFactory<-}
\alias{symbolFactory<-,DelegateContext-method}

% Coercion:

% Show:

\title{Evaluation Contexts}
\description{
  The \code{Context} class is for representing contexts in which
  expressions are evaluated. This might be an R environment, a database,
  or some other external system.
}

\section{Translation}{
  Contexts play an important role in translation. When extracting an
  object by name, the context can delegate to a
  \code{\linkS4class{SymbolFactory}} to create a
  \code{\linkS4class{Symbol}} object that is a lazy reference to the
  object. The reference is expressed in the target language. If there is
  no \code{SymbolFactory}, i.e., it has been set to \code{NULL}, then
  evaluation is eager.

  The intent is to decouple the type of the context from a particular
  language, since a context could support the evaluation of multiple
  languages. The accessors below effectively allow one to specify the
  desired target language.

  \describe{
    \item{}{
      \code{symbolFactory(x)}, \code{symbolFactory(x) <- value}: Get or
      set the current \code{SymbolFactory} (may be NULL).
    }
  }
}

\author{ Michael Lawrence }

\keyword{methods}
\keyword{classes}
