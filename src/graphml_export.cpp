#include <sstream>
#include <set>
#include <map>
#include <deque>
#include <Rcpp.h>

#include "rutil.h"

#ifndef NDEBUG
//#define DYNLOAD_DEBUG
#endif

std::string r_to_graphml_type( int rtype )
{
    switch ( rtype ) {
    case STRSXP:  return "string";
    case INTSXP:  return "int";
    case LGLSXP:  return "boolean";
    case REALSXP: return "double";
    default: THROW_EXCEPTION( std::invalid_argument, "RTYPE " << rtype << " is not supported by RGraphML" );
    }
}

void write_comment(
    std::ostringstream&         out,
    const char*                 comment
){
    out << "<!-- " << comment << " -->\n";
}

typedef std::string attr_name_t;

struct Attribute {
    Rcpp::GenericVector values;
    int             id;
    attr_name_t     exportName;
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
            case REALSXP: {
                double val = Rcpp::as<double>( values[ix] );
                if ( std::isnan( val ) ) out << "NaN";
                else if ( val == std::numeric_limits<double>::infinity() ) out << "Infinity";
                else if ( val == -std::numeric_limits<double>::infinity() ) out << "-Infinity";
                else out << val;
            }
                break;
            default:
                THROW_EXCEPTION( std::invalid_argument, "RTYPE " << rtype << " is not supported by RGraphML" );
            }
            out << "</data>\n";
        }
    }
};

typedef std::vector<Attribute> attr_map_t;

attr_map_t process_attributes(
    const Rcpp::DataFrame&      data,
    const Rcpp::CharacterVector& attrsExported,
    bool                        isNodeAttrs
){
    Rcpp::Rcerr << attrsExported.size() << " "
                << ( isNodeAttrs ? "node" : "edge" )
                << " attribute(s) to export\n";
    attr_map_t attrs;

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
typedef std::deque<node_id_t> node_id_deque_t;

struct Node {
    node_id_t           id;
    node_id_deque_t     parentIds; // a list of parent node Ids, innermost are on top
    int                 rowIx;
};

typedef std::pair<node_id_t, node_id_t> edge_id_t;

struct Edge {
    node_id_t   sourceId;
    node_id_t   targetId;
    int         rowIx;
    node_id_t   parentId;
};

struct Graph {
    typedef std::set<node_id_t> node_set_t;
    typedef std::map<node_id_t, Node> node_map_t;
    typedef std::multimap<node_id_t, node_id_t> parent_map_t;
    typedef std::pair<parent_map_t::const_iterator, parent_map_t::const_iterator> const_node_range_t;
    typedef std::map<edge_id_t, Edge> edge_map_t;
    typedef std::multimap<node_id_t, edge_id_t> edge_parent_map_t;
    typedef std::pair<edge_parent_map_t::const_iterator, edge_parent_map_t::const_iterator> const_edge_range_t;

    Rcpp::DataFrame  nodes;
    Rcpp::DataFrame  edges;
    std::string      nodeIdCol;
    std::string      parentIdCol;
    std::string      sourceCol;
    std::string      targetCol;
    Rcpp::CharacterVector nodeAttrsExported;
    Rcpp::CharacterVector edgeAttrsExported;
    bool                    isDirected;

    attr_map_t nodeAttrs;
    attr_map_t edgeAttrs;

    node_map_t      nodeMap;
    parent_map_t    parentMap;

    edge_map_t      edgeMap;
    edge_parent_map_t   edgeParentMap;

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

    void set_nodes_parents( const node_id_deque_t& parentIds = node_id_deque_t( 1, node_id_t() ) );
    node_id_t innermost_parent_node( const node_id_t& a, const node_id_t& b ) const;

    void write( std::ostringstream& out ) const;
    void write_subgraph( std::ostringstream& out, const node_id_t& parentId, node_set_t& processedNodes ) const;
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
    // check ID columns existence
    column_map_t nodeCols = column_names( nodes );
    if ( nodeCols.count( nodeIdCol ) == 0 ) {
        THROW_EXCEPTION( std::invalid_argument, "Node ID column '" << nodeIdCol
                         << "' not found in the nodes data.frame" );
    }
    if ( !parentIdCol.empty() && nodeCols.count( parentIdCol ) == 0 ) {
        THROW_EXCEPTION( std::invalid_argument, "Parent node ID column '" << parentIdCol
                         << "' not found in the nodes data.frame" );
    }
    column_map_t edgeCols = column_names( edges );
    if ( edgeCols.count( sourceCol ) == 0 ) {
        THROW_EXCEPTION( std::invalid_argument, "Edge source ID column '"
                         << sourceCol << "' not found in the edges data.frame" );
    }
    if ( edgeCols.count( targetCol ) == 0 ) {
        THROW_EXCEPTION( std::invalid_argument, "Edge target ID column '"
                         << targetCol << "' not found in the edges data.frame" );
    }

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

    Rcpp::Rcerr << "Setting node parents...\n";
    set_nodes_parents();

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
        node_id_t parentId;
        if ( i < parentIds.size() && !Rcpp::traits::is_na<STRSXP>( parentIds[i] ) ) parentId = parentIds[i];
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
        parentMap.insert( std::make_pair( parentId, node.id ) );
    }
}

/// sets the parentIds property of all the nodes that are the children of parentIds.front()
void Graph::set_nodes_parents( const node_id_deque_t& parentIds )
{
    const_node_range_t subnodes = parentMap.equal_range( parentIds.front() );
    node_id_deque_t subParentIds( parentIds );
    subParentIds.push_front( node_id_t() );
    for ( parent_map_t::const_iterator nIt = subnodes.first; nIt != subnodes.second; ++nIt ) {
        node_map_t::iterator subNodeIt = nodeMap.find( nIt->second );
        subNodeIt->second.parentIds = parentIds;
        subParentIds.front() = nIt->second;
        set_nodes_parents( subParentIds );
    }
}

// Finds the innermost common parent node of nodes a and b
node_id_t Graph::innermost_parent_node(
    const node_id_t& aId,
    const node_id_t& bId
) const {
    const node_map_t::const_iterator aIt = nodeMap.find( aId );
    const node_map_t::const_iterator bIt = nodeMap.find( bId );
    if ( aIt == nodeMap.end() || bIt == nodeMap.end() ) return ( node_id_t() );

    node_id_deque_t::const_reverse_iterator rAit = aIt->second.parentIds.rbegin();
    node_id_deque_t::const_reverse_iterator rBit = bIt->second.parentIds.rbegin();
    node_id_t res;
    while ( rAit != aIt->second.parentIds.rend() && rBit != bIt->second.parentIds.rend() && *rAit == *rBit ) {
        res = *rAit;
        rAit++;
        rBit++;
    }
    return ( res );
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
        edge.parentId = innermost_parent_node( edge.sourceId, edge.targetId );

        edge_map_t::iterator eIt = edgeMap.find( edgeId );
        if ( eIt == edgeMap.end() ) {
            edgeMap.insert( eIt, std::make_pair( edgeId, edge ) );
        }
        else {
            THROW_EXCEPTION( std::runtime_error,
                             "Duplicate edge '" << edge.sourceId << "'-'" << edge.targetId << "'"
                             << "' at row " << edge.rowIx );
        }
        edgeParentMap.insert( std::make_pair( edge.parentId, edgeId ) );
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
    for ( attr_map_t::const_iterator attrIt = nodeAttrs.begin(); attrIt != nodeAttrs.end(); ++attrIt ) {
        attrIt->write_definition( out );
    }

    write_comment( out, "Edge attributes definitions" );
    for ( attr_map_t::const_iterator attrIt = edgeAttrs.begin(); attrIt != edgeAttrs.end(); ++attrIt ) {
        attrIt->write_definition( out );
    }

    write_comment( out, "Graph" );
    Rcpp::Rcerr << "Writing nodes and edges...\n";
    node_set_t processedNodes;
    write_subgraph( out, node_id_t(), processedNodes );
    if ( processedNodes.size() < nodeMap.size() ) {
        Rcpp::Rcerr << "Only " << processedNodes.size() 
                    << " of " << nodeMap.size() << " node(s) have been written\n";
    }
    
    out << "</graphml>\n";
}

void Graph::write_subgraph(
    std::ostringstream&         out,
    const node_id_t&            parentId,
    node_set_t&                 processedNodes
) const {
    out << "<graph id=\"" << ( parentId == node_id_t() ? parentId : "root" ) << "\""
        << " edgedefault=\"" << ( isDirected ? "" : "un" ) << "directed\">\n";

    write_comment( out, "Nodes" );
    const_node_range_t nodes = parentMap.equal_range( parentId );
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
        if ( parentMap.count( node.id ) ) {
            Rcpp::Rcerr << "Writing subgraph of node \"" << node.id << "\"...\n";
            write_subgraph( out, node.id, processedNodes );
        }
        out << "  </node>\n";
    }

    write_comment( out, "Edges" );
    const_edge_range_t edges = edgeParentMap.equal_range( parentId );
    for ( edge_parent_map_t::const_iterator eIt = edges.first; eIt != edges.second; ++eIt )
    {
        const Edge& edge = edgeMap.at( eIt->second );
        out << "  <edge id=\"" << edge.sourceId << "_" << edge.targetId << "\""
            << " source=\"" << edge.sourceId << "\""
            << " target=\"" << edge.targetId << "\">\n";
        for ( std::size_t attrIx = 0; attrIx < edgeAttrs.size(); ++attrIx ) {
            edgeAttrs[attrIx].write_value( out, edge.rowIx );
        }
        out << "  </edge>\n";
    }
    out << "</graph>\n";
}

//??? The length of a string (in characters).
//???
//??? @param str input character vector
//??? @return characters in each element of the vector
// [[Rcpp::export]]
std::string DataFrameToGraphML(
    const Rcpp::DataFrame&  nodes,
    const Rcpp::DataFrame&  edges,
    const std::string&      nodeIdCol = "id",
    const Rcpp::String&     parentIdCol = NA_STRING,
    const std::string&      sourceCol = "source",
    const std::string&      targetCol = "target",
    const Rcpp::CharacterVector& nodeAttrs = Rcpp::CharacterVector::create(),
    const Rcpp::CharacterVector& edgeAttrs = Rcpp::CharacterVector::create(),
    bool                    isDirected = false
){
    Rcpp::Rcerr << "Initializing GraphML export...\n";
    Graph graph( nodes, edges, nodeIdCol,
                 parentIdCol.get_sexp() != NA_STRING ? (std::string)parentIdCol : std::string(),
                 sourceCol, targetCol,
                 nodeAttrs, edgeAttrs,
                 isDirected );
    graph.init();

    Rcpp::Rcerr << "Generating GraphML...\n";
    std::ostringstream out;
    graph.write( out );

    return ( out.str() );
}
