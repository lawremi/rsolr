\name{Expression-class}
\docType{class}

\alias{class:Expression}
\alias{Expression-class}

\alias{class:SimpleExpression}
\alias{SimpleExpression-class}

\alias{class:SimpleSymbol}
\alias{SimpleSymbol-class}

\alias{class:Symbol}
\alias{Symbol-class}

\alias{class:SymbolFactory}
\alias{SymbolFactory-class}

\alias{class:TranslationRequest}
\alias{TranslationRequest-class}

% Translation:
\alias{translate,ANY,Expression-method}
\alias{eval,ANY,DelegateContext-method}
\alias{translate}
\alias{SymbolFactory}

% Coercion:
\alias{as.character,TranslationRequest-method}

% Show:

\title{Expressions and Translation}
\description{
  Underlying rsolr is a simple, general framework for representing,
  manipulating and translating between expressions in arbitrary
  languages. The two foundational classes are \code{Expression} and
  \code{Symbol}, which are partially implemented by
  \code{SimpleExpression} and \code{SimpleSymbol}, respectively.
}

\section{Translation}{
  The \code{Expression} framework defines a translation strategy based
  on evaluating source language expressions, using promises to represent
  the objects, such that the result is a promise with its deferred
  computation expressed in the target language.

  The primary entry point is the \code{translate} generic, which has a
  default method that abstractly implements this strategy.  The first
  step is to obtain a \code{SymbolFactory} instance for the target
  expression type via a method on the \code{SymbolFactory} generic. The
  \code{SymbolFactory} (a simple R function) is set on the
  \code{Context}, which should define (perhaps through inheritance) all
  symbols referenced in the source expression. The translation happens
  when the source expression is \code{eval}uated in the context.  The
  context calls the factory to construct \code{Symbol} objects which are
  passed, along with the context, to the \code{Promise} generic, which
  wraps them in the appropriate type of promise. Typically, R is the
  source language, and the \code{eval} method evaluates the R expression
  on the promises. Each method for the specific type of promise will
  construct a new promise with an expression that encodes the
  computation, building on the existing expression. When evaluation is
  finished, we simply extract the expression from the returned promise.
  
  \describe{
    \item{}{
      \code{translate(x, target, context, ...)}: Translates the source
      expression \code{x} to the \code{target} \code{Expression}, where
      the symbols in the source expression are resolved in
      \code{context}, which is usually an R environment or some sort of
      database. The \dots are passed to \code{symbolFactory}.
    }
    \item{}{
      \code{symbolFactory(x)}: Gets the \code{SymbolFactory} object
      that will construct the appropriate type of symbol for the target
      expression \code{x}.
    }
  }
}

\section{Note on Laziness}{
  In general, translation requires access to the referenced data.  There
  may be certain operations that cannot be deferred, so evaluation is
  allowed to be eager, in the hope that the result can be embedded
  directly into the larger expression. Or, at the very least, the
  translation machinery needs to know whether the data actually exist,
  and whether the data are typed or have other constraints.  Since the
  data and schema are not always available when translation is
  requested, such as when building a database query that will be sent to
  by another module to an as-yet-unspecified endpoint, translation
  itself must be deferred. The \code{TranslationRequest} class provides
  a foundation for capturing translations and evaluating them later.
}

\author{ Michael Lawrence }

\keyword{methods}
\keyword{classes}
