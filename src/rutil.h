#include <Rcpp.h>
#include <map>

#pragma once

#define THROW_EXCEPTION( exp, msg ) \
{ \
    std::ostringstream err; \
    err << msg; \
    throw std::invalid_argument( err.str() ); \
}

inline bool r_is_na( int rtype, Rcpp::GenericVector::const_Proxy value )
{
    switch ( rtype ) {
    case STRSXP: {
        // @TODO: SEXP comparison, not NA comparison
        return Rcpp::as<std::string>( value ) == "NA";
    }
    case INTSXP:  return Rcpp::traits::is_na<INTSXP>( Rcpp::as<int>( value ) );
    case LGLSXP:  return Rcpp::traits::is_na<LGLSXP>( Rcpp::as<bool>( value ) );
    case REALSXP: return Rcpp::traits::is_na<REALSXP>( Rcpp::as<double>( value ) );
    default: THROW_EXCEPTION( std::invalid_argument, "Don't know how to is.na(): RTYPE=" << rtype );
    }
}

inline int r_vector_sxp_type( const Rcpp::GenericVector& v )
{
	return ( v.size() > 0 ? Rcpp::RObject( v[0] ).sexp_type() : NILSXP );
}

typedef std::map<std::string, int> column_map_t;

inline column_map_t column_names( const Rcpp::DataFrame& data )
{
    column_map_t res;

    Rcpp::CharacterVector columnNames = data.names();
    for ( int i = 0; i < columnNames.size(); i++ ) {
        std::string colName = (std::string)Rcpp::String(columnNames[i]);
        res[ colName ] = i;
    }
    return ( res );
}
