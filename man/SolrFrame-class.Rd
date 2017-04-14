\name{SolrFrame-class}
\docType{class}

\alias{class:SolrFrame}
\alias{SolrFrame-class}

\alias{class:Solr}
\alias{Solr-class}

% Accessors:
\alias{dimnames,SolrFrame-method}
\alias{colnames,SolrFrame-method}
\alias{rownames,SolrFrame-method}
\alias{ROWNAMES,SolrFrame-method}
\alias{dim,SolrFrame-method}
\alias{nrow,SolrFrame-method}
\alias{NROW,SolrFrame-method}
\alias{ncol,SolrFrame-method}
\alias{NCOL,SolrFrame-method}
\alias{length,SolrFrame-method}
\alias{names,SolrFrame-method}
\alias{fieldNames}
\alias{fieldNames,SolrFrame-method}
\alias{fieldNames,Solr-method}
\alias{[<-,SolrFrame,ANY,ANY,ANY-method}
\alias{[[<-,SolrFrame-method}
\alias{[,SolrFrame-method}
\alias{[[,SolrFrame,ANY-method}
\alias{$,Solr-method}
\alias{$<-,Solr-method}
\alias{ndoc,Solr-method}
\alias{nfield,Solr-method}
\alias{core}
\alias{query}
\alias{schema,Solr-method}
\alias{facets,SolrSummary-method}
\alias{grouping,Solr-method}
\alias{ids,Solr-method}

% Constructor:
\alias{SolrFrame}

% Manipulation:
\alias{rename}
\alias{rename,Solr-class}
\alias{subset,Solr-method}
\alias{transform,Solr-method}
\alias{sort,Solr-method}
\alias{xtabs,Solr-method}
\alias{aggregate,formula-method}
\alias{unique,SolrFrame-method}
\alias{aggregate,Solr-method}
\alias{sort,Solr-method}
\alias{summary,SolrFrame-method}
\alias{unique,Solr-method}
\alias{head,Solr-method}
\alias{tail,Solr-method}
\alias{window,Solr-method}
\alias{group}
\alias{group,SolrFrame,NULL-method}
\alias{group,SolrFrame,formula-method}
\alias{ungroup,SolrFrame-method}
\alias{rename,Solr-method}

% Evaluation:
\alias{eval,SolrExpression,SolrFrame-method}
\alias{eval,SolrAggregateCall,SolrFrame-method}
\alias{eval,language,Solr-method}
\alias{with,Solr-method}
\alias{within,Solr-method}

% Laziness:
\alias{defer}
\alias{defer,Solr-method}
\alias{symbolFactory,Solr-method}
\alias{symbolFactory<-,Solr-method}

% Coercion:
\alias{as.data.frame.Solr}
\alias{as.data.frame,Solr-method}
\alias{coerce,Solr,data.frame-method}
\alias{as.list,Solr-method}
\alias{as.list.Solr}
\alias{coerce,Solr,environment-method}
\alias{as.data.frame,SolrFrame-method}
\alias{as.list,SolrFrame-method}
\alias{as.table,SolrSummary-method}

% Search:
\alias{searchDocs,Solr-method}

% Show:
\alias{show,SolrFrame-method}

\title{SolrFrame}
\description{
  The \code{SolrFrame} object makes Solr data accessible through a
  data.frame-like interface. This is the typical way an R user accesses
  data from a Solr core. Much of its methods are shared with
  \code{SolrList}, which has very similar behavior.
}

\details{
  A \code{SolrFrame} should more or less behave analogously to a data
  frame. It provides the same basic accessors (\code{\link{nrow}},
  \code{\link{ncol}}, \code{\link{length}}, \code{\link{rownames}},
  \code{\link{colnames}}, \code{\link{[}}, \code{\link{[<-}},
  \code{\link{[[}}, \code{\link{[[<-}}, \code{\link{$}},
  \code{\link{$<-}}, \code{\link{head}}, \code{\link{tail}}, etc) and
  can be coerced to an actual data frame via
  \code{\link{as.data.frame}}. Supported types of data manipulations
  include \code{\link{subset}}, \code{\link{transform}},
  \code{\link{sort}}, \code{\link{xtabs}}, \code{\link{aggregate}},
  \code{\link{unique}}, \code{\link{summary}}, etc.

  Mapping a collection of documents to a tablular data structure is not
  quite natural, as the document collection is ragged: a given document
  can have any arbitrary set of fields, out of a set that is essentially
  infinite. Unlike some other document stores, however, Solr constrains
  the type of every field through a schema. The schema achieves
  flexibility through \dQuote{dynamic} fields. The name of a dynamic
  field is a wildcard pattern, and any document field that matches the
  pattern is expected to obey the declared type and other
  constraints.

  When determining its set of columns, \code{SolrFrame} takes every
  actual field present in the collection, and (by default) adds all
  non-dynamic (static) fields, in the order specified by the
  schema. Note that is very likely that many columns will consist
  entirely or almost entirely of NAs.

  If a collection is extremly ragged, where few fields are shared
  between documents, it may make more sense to treat the data as a list,
  through \code{\linkS4class{SolrList}}, which shares almost all of the
  functionality of \code{SolrFrame} but in a different shape.

  The rownames are taken from the field declared in the schema to
  represent the unique document key. Schemas are not strictly required
  to declare such a field, so if there is no unique key, the rownames
  are \code{NULL}.
  
  Field restrictions passed to e.g. \code{[} or \code{subset(fields=)}
  may be specified by name, or wildcard pattern (glob). Similarly, a row
  index passed to \code{[} must be either a character vector of
  identifiers (of length <= 1024, NAs are not supported, and this
  requires a unique key in the schema) or a
  \code{\linkS4class{SolrPromise}}/\code{\linkS4class{SolrExpression}},
  but note that if it evaluates to NAs, the corresponding rows are
  excluded from the result, as with \code{subset}. Using a
  \code{SolrPromise} or \code{SolrExpression} is recommended, as
  filtering happens at the database.
  
  A special feature of \code{SolrFrame}, vs. an ordinary data frame, is
  that it can be \code{group}ed into a
  \code{\linkS4class{GroupedSolrFrame}}, where every column is modeled
  as a list, split by some combination of grouping factors. This is
  useful for aggregation and supports the implementation of the
  \code{aggregate} method, which is the recommended high-level
  interface.

  Another interesting feature is laziness. One can \code{defer} a
  \code{SolrFrame}, so that all column retrieval, e.g., via \code{$} or
  \code{eval}, returns a \code{\linkS4class{SolrPromise}} object. Many
  operations on promises are deferred, until they are finally
  \code{fulfill}ed by being shown or through explicit coercion to an R
  vector.

  A note for developers: \code{SolrList} and \code{SolrFrame} share
  common functionality through the base \code{Solr} class. Much of the
  functionality mentioned here is actually implemented as methods on the
  \code{Solr} class.
}

\section{Accessors}{
  These are some accessors that \code{SolrFrame} adds on top of the
  basic data frame accessors. Most of these are for advanced use only.
  
  \describe{
    \item{}{
      \code{ndoc(x)}: Gets the number of documents (rows); serves as an
      abstraction over \code{SolrFrame} and \code{SolrList}
    }
    \item{}{
      \code{nfield(x)}: Gets the number of fields (columns); serves as an
      abstraction over \code{SolrFrame} and \code{SolrList}
    }
    \item{}{
      \code{ids(x)}: Gets the document unique identifiers (may
      be \code{NULL}, treated as rownames); serves as an abstraction
      over \code{SolrFrame} and \code{SolrList}
    }
    \item{}{
      \code{fieldNames(x, includeStatic=TRUE, ...)}: Gets the name of
      each field represented by any document in the Solr core, with
      \dots being passed down to \code{fieldNames} on
      \code{\linkS4class{SolrCore}}. Fields must be indexed to be
      reported, with the exception that when \code{includeStatic} is
      \code{TRUE}, we ensure all static (non-dynamic) fields are present
      in the return value. Names are returned in an order consistent
      with the order in the schema. Note that two different
      \dQuote{instances} of the same dynamic field do not have a
      specified order in the schema, so we use the index order
      (lexicographical) for those cases.
    }
    \item{}{
      \code{core(x)}: Gets the \code{SolrCore} wrapped by \code{x}
    }
    \code{}{
      \code{query(x)}: Gets the query that is being constructed by
      \code{x}
    }
  }
}

\section{Extended API}{
  Most of the typical data frame accessors and data manipulation
  functions will work analogously on \code{SolrFrame} (see
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

        For global aggregation, simply pass the \code{SolrFrame} as
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
      \code{\linkS4class{GroupedSolrFrame}} that is grouped by the
      factors in \code{by}, typically a formula. To get back to
      \code{x}, call \code{ungroup(x)}.
    }
    \item{}{\code{grouping(x)}: Just returns \code{NULL}, since a
      \code{SolrFrame} is not grouped (unless extended to be groupable).
    }
    \item{}{\code{defer(x)}: Returns a \code{SolrFrame} that yields
      \code{\linkS4class{SolrPromise}} objects instead of vectors
      whenever a field is retrieved
    }
    \item{}{\code{searchDocs(x, q)}: Performs a conventional document
      search using the query string \code{q}. The main difference to
      filtering is that (by default) Solr will order the result by
      score, i.e., how well each document matches the query.
    }
  }
}

\section{Constructor}{
  \describe{
    \item{}{
      \code{SolrFrame(uri)}: Constructs a new \code{SolrFrame} instance,
      representing a Solr core located at \code{uri}, which should be a
      string or a \code{\link[restfulr]{RestUri}} object. The \dots are
      passed to the \code{SolrQuery} constructor.
    }
  }
}


\section{Evaluation}{
  \describe{
    \item{}{
      \code{eval(expr, envir, enclos)}: Evaluates \code{expr} in the
      \code{SolrFrame} \code{envir}, using \code{enclos} as the
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

\seealso{
  \code{\linkS4class{SolrList}} for representing a Solr collection as a
  list instead of a table
}

\examples{
     schema <- deriveSolrSchema(mtcars)
     solr <- TestSolr(schema)
     sr <- SolrFrame(solr$uri)
     sr[] <- mtcars
     dim(sr)
     head(sr)
     subset(sr, mpg > 20 & cyl == 4)
     solr$kill()
     ## see the vignette for more
}

\keyword{methods}
\keyword{classes}
