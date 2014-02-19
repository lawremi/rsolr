#include <Rinternals.h>

/*
 * --- .Call ENTRY POINT ---
 * Gets the top environment associated with a (nested) promise.
 */
SEXP top_prenv(SEXP nm, SEXP env)
{
  SEXP promise = findVar(nm, env);
  while(TYPEOF(promise) == PROMSXP) {
    env = PRENV(promise);
    promise = PREXPR(promise);
  }
  return env;
}
