#include <sstream>
#include <set>
#include <map>
#include <Rcpp.h>
#include <Rcpp/iostream/Rostream.h>

#ifndef NDEBUG
//#define DYNLOAD_DEBUG
#endif

#define THROW_EXCEPTION( exp, msg ) \
{ \
    std::ostringstream err; \
    err << msg; \
    throw std::invalid_argument( err.str() ); \
}

std::string r_to_graphml_type( int rtype )
{
    switch ( rtype ) {
    case STRSXP:  return "string";
    case INTSXP:  return "integer";
    case LGLSXP:  return "boolean";
    case REALSXP: return "double";
    default: THROW_EXCEPTION( std::invalid_argument, "RTYPE " << rtype << " is not supported by RGraphML" );
    }
}

bool r_is_na( int rtype, Rcpp::GenericVector::const_Proxy value )
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

void write_comment(
    std::ostringstream&         out,
    const char*                 comment
){
    out << "<!-- " << comment << " -->\n";
}

struct Attribute {
    Rcpp::GenericVector values;
    int             id;
    std::string     exportName;
    int             rtype;
    bool            isNodeAttr;

    Attribute( const Rcpp::GenericVector& values,
               int id, const std::string& exportName,
               bool isNodeAttr
    ) : values( values )
      , id( id )
      , exportName( exportName )
      , rtype( values.size() > 0 ? Rcpp::RObject( values[0] ).sexp_type() : NILSXP )
      , isNodeAttr( isNodeAttr )
    {
    }

    void write_definition( std::ostringstream& out ) const
    {
        out << "<key for=\"" << ( isNodeAttr ? "node" : "edge" ) << "\""
            << " id=\"" << ( isNodeAttr ? "n" : "e" ) << id << "\""
            << " attr.name=\"" << exportName << "\""
            << " attr.type=\"" << r_to_graphml_type( rtype ) << "\"/>\n";
    }

    void write_value( std::ostringstream& out, int ix ) const
    {
        //Rcpp::Rcerr << "Writing attribute " << exportName << " for row " << ix << "\n";
        if ( ix >= values.size() ) throw std::runtime_error( "Row index out of bounds" );
        if ( !r_is_na( rtype, values[ix] ) ) {
            out << "    <data key=\"" << ( isNodeAttr ? "n" : "e" ) << id << "\">";
            switch ( rtype ) {
            case LGLSXP:
                out << ( !Rcpp::as<bool>( values[ix] ) ? "false" : "true" );
                break;
            case STRSXP:
                out << Rcpp::as<std::string>( values[ix] );
                break;
            case INTSXP:
                out << Rcpp::as<int>( values[ix] );
                break;
            case REALSXP:
                out << Rcpp::as<double>( values[ix] );
                break;
            default:
                THROW_EXCEPTION( std::invalid_argument, "RTYPE " << rtype << " is not supported by RGraphML" );
            }
            out << "</data>\n";
        }
    }
};

typedef std::map<std::string, int> column_map_t;

column_map_t column_names( const Rcpp::DataFrame& data )
{
    column_map_t res;

    Rcpp::CharacterVector columnNames = data.names();
    for ( int i = 0; i < columnNames.size(); i++ ) {
        std::string colName = (std::string)Rcpp::String(columnNames[i]);
        res[ colName ] = i;
    }
    return ( res );
}

std::vector<Attribute> process_attributes(
    const Rcpp::DataFrame&      data,
    const Rcpp::CharacterVector& attrsExported,
    bool                        isNodeAttrs
){
    Rcpp::Rcerr << attrsExported.size() << " "
                << ( isNodeAttrs ? "node" : "edge" )
                << " attribute(s) to export\n";
    std::vector<Attribute> attrs;

    Rcpp::CharacterVector exportNames;
    Rcpp::RObject attrNames = attrsExported.names();
    if ( !attrNames.isNULL() ) {
        exportNames = attrNames;
    }

    column_map_t cols = column_names( data );
    for ( int i = 0; i < attrsExported.size(); i++ ) {
        std::string attrName = Rcpp::String( attrsExported[i] );
        //Rcpp::Rcerr << "Processing " << i << " attribute '" << attrName << "'\n";
        column_map_t::const_iterator colIt = cols.find( attrName );
        if ( colIt != cols.end() ) {
            // use exported name, if provided, otherwise use the column name
            std::string exportName;
            if ( i < exportNames.size() ) exportName = (std::string)Rcpp::String( exportNames[i] );
            if ( exportName.empty() ) exportName = attrName;

            //Rcpp::Rcerr << "Processing " << exportName << " attribute\n";
            Attribute attr = Attribute( data[colIt->second], attrs.size() + 1, exportName, isNodeAttrs );
            attrs.push_back( attr );
            Rcpp::Rcerr << "Attribute '" << attrName << "'"
                        << " exported as " << attr.exportName << ", type " << r_to_graphml_type( attr.rtype )
                        << ", key " << attr.id << "\n";
        } else {
            Rcpp::Rcerr << "Skipping attribute '" << attrName << "': not found in the data.frame\n";
        }
    }
    Rcpp::Rcerr << attrs.size() << " " 
                << ( isNodeAttrs ? "node" : "edge" ) << " attribute(s) would be exported\n";
    return ( attrs );
}

typedef std::string node_id_t;

struct Node {
    std::string id;
    std::string parentId;
    int         rowIx;
};

typedef std::pair<node_id_t, node_id_t> edge_id_t;

struct Edge {
    std::string sourceId;
    std::string targetId;
    int         rowIx;
};

struct Graph {
    typedef std::set<node_id_t> node_set_t; 
    typedef std::map<node_id_t, Node> node_map_t;
    typedef std::multimap<node_id_t, node_id_t> parent_map_t;
    typedef std::pair<parent_map_t::const_iterator, parent_map_t::const_iterator> const_node_range_t;
    typedef std::map<edge_id_t, Edge> edge_map_t;

    Rcpp::DataFrame  nodes;
    Rcpp::DataFrame  edges;
    std::string      nodeIdCol;
    std::string      parentIdCol;
    std::string      sourceCol;
    std::string      targetCol;
    Rcpp::CharacterVector nodeAttrsExported;
    Rcpp::CharacterVector edgeAttrsExported;
    bool                    isDirected;

    std::vector<Attribute> nodeAttrs;
    std::vector<Attribute> edgeAttrs;

    node_map_t      nodeMap;
    parent_map_t    parentMap;

    edge_map_t      edgeMap;

    Graph(
        const Rcpp::DataFrame&  nodes,
        const Rcpp::DataFrame&  edges,
        const std::string&      nodeIdCol,
        const std::string&      parentIdCol,
        const std::string&      sourceCol,
        const std::string&      targetCol,
        const Rcpp::CharacterVector& nodeAttrsExported,
        const Rcpp::CharacterVector& edgeAttrsExported,
        bool                    isDirected
    );

    void init();
    void read_nodes();
    void read_edges();

    void write( std::ostringstream& out ) const;
    void write_node_subset( std::ostringstream& out, const const_node_range_t& nodeSubset, node_set_t& processedNodes ) const;
    void write_edges( std::ostringstream& out ) const;
};

Graph::Graph(
    const Rcpp::DataFrame&  nodes,
    const Rcpp::DataFrame&  edges,
    const std::string&      nodeIdCol,
    const std::string&      parentIdCol,
    const std::string&      sourceCol,
    const std::string&      targetCol,
    const Rcpp::CharacterVector& nodeAttrsExported,
    const Rcpp::CharacterVector& edgeAttrsExported,
    bool                    isDirected
) : nodes( nodes ), edges( edges )
  , nodeIdCol( nodeIdCol ), parentIdCol( parentIdCol )
  , sourceCol( sourceCol ), targetCol( targetCol )
  , isDirected( isDirected )
{
    // mapping of attributes from R to GraphML
    Rcpp::Rcerr << "Reading node attributes...\n";
    nodeAttrs = process_attributes( nodes, nodeAttrsExported, true );
    Rcpp::Rcerr << "Reading edge attributes...\n";
    edgeAttrs = process_attributes( edges, edgeAttrsExported, false );
}

void Graph::init()
{
    Rcpp::Rcerr << "Reading nodes...\n";
    read_nodes();
    Rcpp::Rcerr << nodeMap.size() << " node(s) read\n";

    Rcpp::Rcerr << "Reading edges...\n";
    read_edges();
    Rcpp::Rcerr << edgeMap.size() << " edge(s) read\n";
}

void Graph::read_nodes()
{
    Rcpp::CharacterVector nodeIds = nodes[ nodeIdCol ];
    Rcpp::CharacterVector parentIds;
    if ( !parentIdCol.empty() ) parentIds = nodes[ parentIdCol ];
    for ( int i = 0; i < nodes.nrows(); i++ ) {
        Node node;
        node.id = nodeIds[ i ];
        if ( i < parentIds.size() && !(parentIds[i] == NA_STRING) ) node.parentId = parentIds[i];
        node.rowIx = i;
        node_map_t::iterator nIt = nodeMap.find( node.id );
        if ( nIt == nodeMap.end() ) {
            nodeMap.insert( nIt, std::make_pair( node.id, node ) );
        }
        else {
            THROW_EXCEPTION( std::runtime_error,
                             "Duplicate node '" << node.id
                             << "' at row " << node.rowIx );
        }
        parentMap.insert( std::make_pair( node.parentId, node.id ) );
    }
}

void Graph::read_edges()
{
    Rcpp::CharacterVector sourceIds = edges[ sourceCol ];
    Rcpp::CharacterVector targetIds = edges[ targetCol ];
    for ( int i = 0; i < edges.nrows(); i++ ) {
        Edge edge;
        edge.sourceId = sourceIds[ i ];
        edge.targetId = targetIds[ i ];
        edge.rowIx = i;
        edge_id_t edgeId( edge.sourceId, edge.targetId );
        edge_map_t::iterator eIt = edgeMap.find( edgeId );
        if ( eIt == edgeMap.end() ) {
            edgeMap.insert( eIt, std::make_pair( edgeId, edge ) );
        }
        else {
            THROW_EXCEPTION( std::runtime_error,
                             "Duplicate edge '" << edge.sourceId << "'-'" << edge.targetId << "'"
                             << "' at row " << edge.rowIx );
        }
    }
}

void Graph::write( std::ostringstream& out ) const
{
    out << "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
        << "<graphml xmlns=\"http://graphml.graphdrawing.org/xmlns\""
        << " xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
        << " xmlns:y=\"http://www.yworks.com/xml/graphml\""
        << " xmlns:yed=\"http://www.yworks.com/xml/yed/3\"\n"
        << " xsi:schemaLocation=\"http://graphml.graphdrawing.org/xmlns"
        <<  " http://www.yworks.com/xml/schema/graphml/1.1/ygraphml.xsd\">\n";
    write_comment( out, "Generated by RGraphML, Alexey Stukalov @ CeMM" );

    write_comment( out, "Node attributes definitions" );
    for ( std::size_t i = 0; i < nodeAttrs.size(); i++ ) {
        nodeAttrs[i].write_definition( out );
    }

    write_comment( out, "Edge attributes definitions" );
    for ( std::size_t i = 0; i < edgeAttrs.size(); i++ ) {
        edgeAttrs[i].write_definition( out );
    }

    write_comment( out, "Graph" );
    out << "<graph id=\"root\" edgedefault=\"" << ( isDirected ? "" : "un" ) << "directed\">\n";
    Rcpp::Rcerr << "Generating nodes...\n";
    write_comment( out, "Nodes" );
    node_set_t processedNodes;
    write_node_subset( out, parentMap.equal_range( std::string() ), processedNodes );
    if ( processedNodes.size() < nodeMap.size() ) {
        Rcpp::Rcerr << "Only " << processedNodes.size() 
                    << " of " << nodeMap.size() << " node(s) written\n";
    }
    
    if ( !edges.isNULL() && edges.nrows() > 0 ) {
        Rcpp::Rcerr << "Generating edges...\n";
        write_comment( out, "Edges" );
        write_edges( out );
    }
    out << "</graph>\n";

    out << "</graphml>\n";
}

void Graph::write_node_subset(
    std::ostringstream&         out,
    const const_node_range_t&   nodes,
    node_set_t&                 processedNodes
) const {
    for ( parent_map_t::const_iterator nIt = nodes.first; nIt != nodes.second; ++nIt )
    {
        node_map_t::const_iterator n2It = nodeMap.find( nIt->second );
        if ( n2It == nodeMap.end() ) {
            THROW_EXCEPTION( std::runtime_error,
                             "Internal error: node '" << nIt->second << "' not found in the nodes map" );
        }
        const Node& node = n2It->second;
        if ( processedNodes.count( node.id ) ) {
            THROW_EXCEPTION( std::runtime_error,
                             "Node '" << node.id << "' already written to GraphML. Potential parent-child circular reference" );
        }
        processedNodes.insert( node.id );
        out << "  <node id=\"" << node.id << "\">\n";
        for ( std::size_t attrIx = 0; attrIx < nodeAttrs.size(); ++attrIx ) {
            nodeAttrs[attrIx].write_value( out, node.rowIx );
        }
        // check for subgraph
        const_node_range_t subnodes = parentMap.equal_range( node.id );
        if ( subnodes.first != subnodes.second ) {
            Rcpp::Rcerr << "Writing subgraph of node \"" << node.id << "\"...";
            out << "  <graph id=\"" << node.id << "_subgraph\" edgedefault=\"" 
                << ( isDirected ? "" : "un" ) << "directed\">\n";
            write_node_subset( out, subnodes, processedNodes );
            out << "  </graph>\n";
        }
        out << "  </node>\n";
    }
}

void Graph::write_edges(
    std::ostringstream& out    
) const {
    for ( edge_map_t::const_iterator eit = edgeMap.begin(); eit != edgeMap.end(); ++eit )
    {
        const Edge& edge = eit->second;
        out << "  <edge id=\"" << edge.sourceId << "_" << edge.targetId << "\""
            << " source=\"" << edge.sourceId << "\""
            << " target=\"" << edge.targetId << "\">\n";
        for ( std::size_t attrIx = 0; attrIx < edgeAttrs.size(); ++attrIx ) {
            edgeAttrs[attrIx].write_value( out, edge.rowIx );
        }
        out << "  </edge>\n";
    }
}

//✬ The length of a string (in characters).
//✬
//✬ @param str input character vector
//✬ @return characters in each element of the vector
//* @TODO: fix to use proper NA for parentIdCol
// [[Rcpp::export]]
std::string DataFrameToGraphML(
    const Rcpp::DataFrame&  nodes,
    const Rcpp::DataFrame&  edges,
    const std::string&      nodeIdCol = "id",
    const std::string&      parentIdCol = "NA",
    const std::string&      sourceCol = "source",
    const std::string&      targetCol = "target",
    const Rcpp::CharacterVector& nodeAttrs = Rcpp::CharacterVector::create(),
    const Rcpp::CharacterVector& edgeAttrs = Rcpp::CharacterVector::create(),
    bool                    isDirected = false
){
    Rcpp::Rcerr << "Initializing GraphML export...\n";
    Graph graph( nodes, edges, nodeIdCol,
                 parentIdCol == "NA" ? std::string() : parentIdCol,
                 sourceCol, targetCol,
                 nodeAttrs, edgeAttrs,
                 isDirected );
    graph.init();

    Rcpp::Rcerr << "Generating GraphML...\n";
    std::ostringstream out;
    graph.write( out );

    return ( out.str() );
}
