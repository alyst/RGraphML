#include <sstream>
#include <set>
#include <map>
#include <deque>
#include <Rcpp.h>

#include "rutil.h"

#ifndef NDEBUG
//#define DYNLOAD_DEBUG
#endif

#define ROOT_GRAPH_ID 0

namespace Tulip {

std::string r_to_tulip_type( int rtype )
{
	switch ( rtype ) {
	case LGLSXP:
		return "bool";
	case STRSXP:
		return "string";
	case INTSXP:
		return "int";
	case REALSXP:
		return "double";
	default:
		THROW_EXCEPTION( std::invalid_argument, "RTYPE " << rtype << " is not supported by Tulip export" );
	}
}

void write_comment(
    std::ostringstream&         out,
    const char*                 comment
){
    out << "  ; " << comment << "\n";
}

struct AttributeDescriptor {
    typedef std::map<std::string, int> field_map_t;

	bool        is_composite;
    std::string type;
    field_map_t fields;

    AttributeDescriptor( bool is_composite, const std::string& type )
    : is_composite( is_composite ), type( type )
    {}

    AttributeDescriptor()
	: is_composite( false )
    {}

    bool empty() const {
		return fields.empty();
	}

	int first_column_index() const {
		return !fields.empty() ? fields.begin()->second : 0;
	}

    static void init_composite_types();

    static const std::string& TypeFromField( const std::string& field ) {
        if ( FieldToType.empty() ) init_composite_types();
        field_to_type_t::const_iterator it = FieldToType.find( field );
        return ( it != FieldToType.end() ? it->second : std::string() );
    }

    struct Field {
        std::string name;
        bool        required;
        double      def_value;
        
        Field( const std::string& name, bool required = true, double def_value = NA_REAL )
        : name( name ), required( required ), def_value( def_value )
        {}
        Field()
        : required( false ), def_value( NA_REAL )
        {}
    };
    typedef std::vector<Field> fields_t;

	static const fields_t CompositeFields( const std::string& type ) {
        if ( CompositeTypes.empty() ) init_composite_types();
        type_to_fields_t::const_iterator it = CompositeTypes.find( type );
        return ( it != CompositeTypes.end() ? it->second : fields_t() );
    }

protected:
    typedef std::map<std::string, fields_t> type_to_fields_t;
    typedef std::map<std::string, std::string> field_to_type_t;

    static type_to_fields_t CompositeTypes;
    static field_to_type_t  FieldToType;

    static void register_field( const std::string& type, const std::string& field, bool required = true, double def_value = NA_REAL )
    {
        FieldToType.insert( std::make_pair( field, type ) );
        type_to_fields_t::iterator it = CompositeTypes.find( type );
        if ( it == CompositeTypes.end() ) {
            it = CompositeTypes.insert( it, std::make_pair( type, fields_t() ) );
        }
        it->second.push_back( Field( field, required, def_value ) );
    }
};

void AttributeDescriptor::init_composite_types()
{
    register_field( "layout", "x", true );
    register_field( "layout", "y", true );
    register_field( "layout", "z", false, 0.0 );

    register_field( "size", "w", true );
    register_field( "size", "h", true );
    register_field( "size", "d", false, 1.0 );

    register_field( "color", "r", true );
    register_field( "color", "g", true );
    register_field( "color", "b", true );
    register_field( "color", "a", false );
}

AttributeDescriptor::type_to_fields_t AttributeDescriptor::CompositeTypes;
AttributeDescriptor::field_to_type_t AttributeDescriptor::FieldToType;

typedef std::string attr_name_t;

typedef std::map<attr_name_t, AttributeDescriptor> attr_desc_map_t;

attr_desc_map_t process_attributes(
    const Rcpp::DataFrame&      data,
    const Rcpp::CharacterVector& attrsExported,
    bool                        isNodeAttrs
){
    Rcpp::Rcerr << attrsExported.size() << " "
                << ( isNodeAttrs ? "node" : "edge" )
                << " attribute(s) to export\n";
    attr_desc_map_t attrs;

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
            attr_name_t exportName;
            if ( i < exportNames.size() ) exportName = (std::string)Rcpp::String( exportNames[i] );
            if ( exportName.empty() ) exportName = attrName;

            std::string type;
			std::string field;
			std::size_t bracketPos = exportName.find( '[' );
            bool is_composite = false;
			if ( bracketPos != std::string::npos ) {
				std::size_t bracketEnd = exportName.find( ']' );
				if ( bracketEnd != std::string::npos ) {
					field = exportName.substr( bracketPos + 1, bracketEnd - bracketPos - 1 );
                    type = AttributeDescriptor::TypeFromField( field );
                    if ( type.empty() ) {
                        // treat as ordinary attribute
                        Rcpp::Rcerr << "Bad composite attribute field '" << field << "'\n";
                        field = std::string();
                    } else {
        				exportName = exportName.substr( 0, bracketPos );
                        is_composite = true;
                    }
				} else {
                    Rcpp::Rcerr << "Bad composite attribute name format '" << exportName << "'\n";
				}
			}
            if ( !is_composite ) {
                type = r_to_tulip_type( r_vector_sxp_type( data[colIt->second] ) );
			}
			attr_desc_map_t::iterator attrIt = attrs.find( exportName );
			if ( attrIt == attrs.end() ) {
				attrIt = attrs.insert( attrIt, std::make_pair( exportName, AttributeDescriptor( is_composite, type ) ) );
			}
            else if ( attrIt->second.type != type ) {
                Rcpp::Rcerr << "Attribute '" << attrIt->first << "' type mismatch:"
                            << attrIt->second.type << " and " << type << "\n";
			}
			AttributeDescriptor::field_map_t::iterator fieldIt = attrIt->second.fields.find( field );
			if ( fieldIt != attrIt->second.fields.end() ) {
                Rcpp::Rcerr << "Skipping duplicate attribute field '" << attrName
                            << " (" << exportName << "[" << field << "])\n";
			}
			attrIt->second.fields.insert( fieldIt, std::make_pair( field, colIt->second ) );
            Rcpp::Rcerr << "Attribute '" << attrName << "'"
                        << " exported as '" << exportName << "'";
            if ( !field.empty() ) {
				Rcpp::Rcerr << '[' << field << "]";
			}
            Rcpp::Rcerr << ", type " << attrIt->second.type
                        << ", pos " << colIt->second << "\n";
        } else {
            Rcpp::Rcerr << "Skipping attribute '" << attrName << "': not found in the data.frame\n";
        }
    }
    Rcpp::Rcerr << attrs.size() << " " 
                << ( isNodeAttrs ? "node" : "edge" ) << " attribute(s) would be exported\n";
    return ( attrs );
}

struct AttributeBase {
    int             id;
    attr_name_t     exportName;

    AttributeBase( int id, const attr_name_t& exportName )
	: id( id )
    , exportName( exportName )
    {
    }
    virtual ~AttributeBase()
    {}

    virtual const std::string& type() const = 0;

    void write_header( std::ostringstream& out ) const
    {
        out << "(property " << 0 // 0 is the main graph
            << ' ' << type()
		    << ' ' << '\"' << exportName << "\"\n";
    }
    void write_footer( std::ostringstream& out ) const
    {
        out << ")\n";
    }

    virtual bool has_data( bool is_node, int value_ix ) const = 0;

    void write_element_value( std::ostringstream& out, bool is_node, std::size_t elm_id, int value_ix ) const {
        if ( !has_data( is_node, value_ix ) ) return;

		out << "  (" << ( is_node ? "node" : "edge" ) << ' ' << elm_id << " \"";
		write_value( out, is_node, value_ix );
		out << "\")\n";
	}

protected:
    virtual void write_value( std::ostringstream& out, bool is_node, int ix ) const = 0;
};

struct SimpleDataContainer {
	Rcpp::GenericVector values;
	int                 rtype;

	SimpleDataContainer( const AttributeDescriptor& attr,
					     const Rcpp::DataFrame& data )
	: values( !attr.empty() ? data[attr.first_column_index()] : Rcpp::GenericVector() )
	{
		rtype = r_vector_sxp_type( values );
	}

	bool empty() const {
		return ( values.size() == 0 );
	}

	bool has_data( int ix ) const {
		return ( !r_is_na( rtype, values[ix] ) );
	}

	void write_value( std::ostringstream& out, int ix ) const
	{
		//Rcpp::Rcerr << "Writing attribute " << exportName << " for row " << ix << "\n";
		if ( ix >= values.size() ) {
			THROW_EXCEPTION( std::invalid_argument,
								"Row index (" << ix << ") out of bounds (" << values.size() << ")" );
		}
		if ( has_data( ix ) ) {
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
				THROW_EXCEPTION( std::invalid_argument, "RTYPE " << rtype << " is not supported by Tulip export" );
			}
		}
	}
};

struct CompositeDataContainer {
	std::vector<Rcpp::NumericVector> values;

	CompositeDataContainer( const AttributeDescriptor& attr,
						    const Rcpp::DataFrame& data )
	{
		if ( attr.empty() ) return;

		const AttributeDescriptor::fields_t& fields = AttributeDescriptor::CompositeFields( attr.type );
		if ( fields.size() != 3 ) {
			THROW_EXCEPTION( std::runtime_error, "Expected 3 fields defined for composite type" );
		}
		for ( std::size_t i = 0; i < fields.size(); ++i ) {
        	int colIx;
			AttributeDescriptor::field_map_t::const_iterator fIt = attr.fields.find( fields[i].name );
			colIx = fIt != attr.fields.end() ? fIt->second : -1;
            if ( colIx >= 0 ) {
                values.push_back( data[colIx] );
            } else if ( fields[i].required ){
        		THROW_EXCEPTION( std::invalid_argument,
    								"No data for required field '" << fields[i].name << "'"
    								<< " of attribute type " << attr.type );
            } else if ( R_IsNA( fields[i].def_value ) ) {
                Rcpp::Rcerr << "Skipping optional field '" << fields[i].name
                            << "' of attribute type " << attr.type << "\n";
            } else {
                values.push_back( Rcpp::NumericVector( data.nrows(), fields[i].def_value ) );
                Rcpp::Rcerr << "Setting optional field '" << fields[i].name
                            << "' of attribute type " << attr.type
                            << " to the default value (" << fields[i].def_value << ")\n";
            }
		}
	}

	bool empty() const {
		return ( values.empty() );
	}

    int size() const {
        return ( !empty() ? values.front().size() : 0 );
    }

	bool has_data( int ix ) const {
		return ( !empty() && !R_IsNA( values.front()[ix] ) );
	}

	void write_value( std::ostringstream& out, int ix ) const
	{
		//Rcpp::Rcerr << "Writing attribute " << exportName << " for row " << ix << "\n";
		if ( ix >= size() ) throw std::runtime_error( "Row index out of bounds" );
        out << '(';
        for ( std::size_t i = 0; i < values.size(); i++ ) {
            if ( i > 0 ) out << ',';
            const Rcpp::NumericVector& values_i = values[i];
            out << values_i[ix];
        }
        out << ')';
	}
};

template<typename T>
struct Attribute: public AttributeBase {
	typedef T container_type;
    std::string type_;
	container_type nodeValues;
	container_type edgeValues;

    Attribute( int id, const attr_name_t& exportName,
						const AttributeDescriptor& nodeAttr,
						const AttributeDescriptor& edgeAttr,
						const Rcpp::DataFrame& nodeData,
						const Rcpp::DataFrame& edgeData
    ) : AttributeBase( id, exportName )
	  , type_( !nodeAttr.empty() ? nodeAttr.type : edgeAttr.type )
      , nodeValues( nodeAttr, nodeData )
      , edgeValues( edgeAttr, edgeData )
    {
		if ( !nodeAttr.empty() && !edgeAttr.empty() ) {
			if ( nodeAttr.type != edgeAttr.type ) {
				THROW_EXCEPTION( std::invalid_argument, "Property '" << exportName
			                     << "' node (" <<  nodeAttr.type <<  ")/edge("
						         << edgeAttr.type << ") type mismatch" );
			}
		}
    }

    virtual const std::string& type() const {
		return ( type_ );
	}
	
    virtual bool has_data( bool is_node, int value_ix ) const {
        const container_type& data = ( is_node ? nodeValues : edgeValues );
        return ( !data.empty() && data.has_data( value_ix ) );
    }

    virtual void write_value( std::ostringstream& out, bool is_node, int ix ) const
    {
		( is_node ? nodeValues : edgeValues ).write_value( out, ix );
    }
};

typedef std::vector<AttributeBase*> attr_map_t;

attr_map_t create_attributes(
	const attr_desc_map_t& nodeAttrs,
	const attr_desc_map_t& edgeAttrs,
	const Rcpp::DataFrame& nodeData,
	const Rcpp::DataFrame& edgeData
){
	attr_map_t res;

	// process node (and matching edge) attributes
	for ( attr_desc_map_t::const_iterator nIt = nodeAttrs.begin();
		  nIt != nodeAttrs.end(); ++nIt
	){
		attr_desc_map_t::const_iterator eIt = edgeAttrs.find( nIt->first );
		if ( eIt != edgeAttrs.end() ) {
			if ( eIt->second.type != nIt->second.type ) {
				Rcpp::Rcerr << "'" << nIt->first << "' attribute: types of node (" << nIt->second.type << ") and edge (" << eIt->second.type
				            << ") attributes don't match, ignoring the edge attribute\n";
				eIt = edgeAttrs.end();
			}
		}
		AttributeBase* newAttr = !nIt->second.is_composite
							? (AttributeBase*)new Attribute<SimpleDataContainer>( res.size()+1, nIt->first, nIt->second,
													eIt != edgeAttrs.end() ? eIt->second : AttributeDescriptor(),
													nodeData, edgeData )
							: (AttributeBase*)new Attribute<CompositeDataContainer>( res.size()+1, nIt->first, nIt->second,
														eIt != edgeAttrs.end() ? eIt->second : AttributeDescriptor(),
														nodeData, edgeData );
		res.push_back( newAttr );
	}
	// process non-matching edge attributes
	for ( attr_desc_map_t::const_iterator eIt = edgeAttrs.begin();
		  eIt != edgeAttrs.end(); ++eIt
	){
		attr_desc_map_t::const_iterator nIt = edgeAttrs.find( eIt->first );
		if ( eIt == edgeAttrs.end() ) {
			AttributeBase* newAttr = !eIt->second.is_composite
			                   ? (AttributeBase*)new Attribute<SimpleDataContainer>( res.size()+1, eIt->first,
													  AttributeDescriptor(), eIt->second,
													  nodeData, edgeData )
							   : (AttributeBase*)new Attribute<CompositeDataContainer>( res.size()+1, eIt->first,
								                         AttributeDescriptor(), eIt->second,
													     nodeData, edgeData );
			res.push_back( newAttr );
		}
	}
	return ( res );
}

typedef std::size_t node_id_t;
typedef std::deque<node_id_t> node_id_deque_t;

struct Node {
    node_id_t           id;
    node_id_deque_t     parentIds; // a list of parent node Ids, innermost are on top
    int                 rowIx;
};

typedef std::pair<node_id_t, node_id_t> edge_key_t;

typedef std::size_t edge_id_t;

struct Edge {
    edge_id_t   id;
    node_id_t   sourceId;
    node_id_t   targetId;
    int         rowIx;
    node_id_t   parentId;
};

struct Graph {
    typedef std::map<node_id_t, node_id_t> child_map_t;
    typedef std::map<node_id_t, Node> node_map_t;
    typedef std::multimap<node_id_t, node_id_t> parent_map_t;
    typedef std::pair<parent_map_t::const_iterator, parent_map_t::const_iterator> const_node_range_t;
    typedef std::map<edge_key_t, Edge> edge_map_t;
    typedef std::multimap<node_id_t, edge_key_t> edge_parent_map_t;
    typedef std::pair<edge_parent_map_t::const_iterator, edge_parent_map_t::const_iterator> const_edge_range_t;

    Rcpp::DataFrame  nodes;
    Rcpp::DataFrame  edges;
    std::string      nodeIdCol;
    std::string      parentIdCol;

    std::string      edgeIdCol;
    std::string      sourceCol;
    std::string      targetCol;

    Rcpp::CharacterVector nodeAttrsExported;
    Rcpp::CharacterVector edgeAttrsExported;
    bool                    isDirected;

    attr_map_t      attrs;

    node_map_t      nodeMap;
    parent_map_t    parentMap;

    edge_map_t      edgeMap;
    edge_parent_map_t   edgeParentMap;

    Graph(
        const Rcpp::DataFrame&  nodes,
        const Rcpp::DataFrame&  edges,
        const std::string&      nodeIdCol,
        const std::string&      parentIdCol,
        const std::string&      edgeIdCol,
        const std::string&      sourceCol,
        const std::string&      targetCol,
        const Rcpp::CharacterVector& nodeAttrsExported,
        const Rcpp::CharacterVector& edgeAttrsExported,
        bool                    isDirected
    );
    ~Graph();

    void init();
    void read_nodes();
    void read_edges();

    void set_nodes_parents( const node_id_deque_t& parentIds = node_id_deque_t( 1, node_id_t() ) );
    node_id_t innermost_parent_node( const node_id_t& a, const node_id_t& b ) const;

    void write( std::ostringstream& out ) const;
    void write_subgraph( std::ostringstream& out, node_id_t parentId, child_map_t& nodeToParent ) const;
    void write_metagraph_property( std::ostringstream& out ) const;
};

Graph::Graph(
    const Rcpp::DataFrame&  nodes,
    const Rcpp::DataFrame&  edges,
    const std::string&      nodeIdCol,
    const std::string&      parentIdCol,
    const std::string&      edgeIdCol,
    const std::string&      sourceCol,
    const std::string&      targetCol,
    const Rcpp::CharacterVector& nodeAttrsExported,
    const Rcpp::CharacterVector& edgeAttrsExported,
    bool                    isDirected
) : nodes( nodes ), edges( edges )
  , nodeIdCol( nodeIdCol ), parentIdCol( parentIdCol )
  , edgeIdCol( edgeIdCol ), sourceCol( sourceCol ), targetCol( targetCol )
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
    if ( edgeCols.count( edgeIdCol ) == 0 ) {
        THROW_EXCEPTION( std::invalid_argument, "Edge ID column '"
                         << edgeIdCol << "' not found in the edges data.frame" );
    }
    if ( edgeCols.count( sourceCol ) == 0 ) {
        THROW_EXCEPTION( std::invalid_argument, "Edge source ID column '"
                         << sourceCol << "' not found in the edges data.frame" );
    }
    if ( edgeCols.count( targetCol ) == 0 ) {
        THROW_EXCEPTION( std::invalid_argument, "Edge target ID column '"
                         << targetCol << "' not found in the edges data.frame" );
    }

    // mapping of attributes from R to Tulip
    Rcpp::Rcerr << "Reading node attributes...\n";
    attr_desc_map_t nodeAttrsMap = process_attributes( nodes, nodeAttrsExported, true );
    Rcpp::Rcerr << "Reading edge attributes...\n";
    attr_desc_map_t edgeAttrsMap = process_attributes( edges, edgeAttrsExported, false );
    Rcpp::Rcerr << "Binding attributes to data...\n";
    attrs = create_attributes( nodeAttrsMap, edgeAttrsMap, nodes, edges );
}

Graph::~Graph()
{
    for ( std::size_t i = 0; i < attrs.size(); i++ ) {
        delete attrs[i];
    }
    attrs.clear();
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
    Rcpp::IntegerVector nodeIds = nodes[ nodeIdCol ];
    Rcpp::IntegerVector parentIds;
    if ( !parentIdCol.empty() ) parentIds = nodes[ parentIdCol ];
    for ( int i = 0; i < nodes.nrows(); i++ ) {
        Node node;
        node.id = nodeIds[ i ];
        node_id_t parentId = ROOT_GRAPH_ID;
        if ( i < parentIds.size() && !Rcpp::traits::is_na<INTSXP>( parentIds[i] ) ) parentId = parentIds[i];
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
    Rcpp::IntegerVector edgeIds = edges[ edgeIdCol ];
    Rcpp::IntegerVector sourceIds = edges[ sourceCol ];
    Rcpp::IntegerVector targetIds = edges[ targetCol ];
    for ( int i = 0; i < edges.nrows(); i++ ) {
        Edge edge;
        edge.id = edgeIds[ i ];
        edge.sourceId = sourceIds[ i ];
        edge.targetId = targetIds[ i ];
        edge.rowIx = i;
        edge_key_t edgeKey( edge.sourceId, edge.targetId );
        edge.parentId = innermost_parent_node( edge.sourceId, edge.targetId );

        edge_map_t::iterator eIt = edgeMap.find( edgeKey );
        if ( eIt == edgeMap.end() ) {
            edgeMap.insert( eIt, std::make_pair( edgeKey, edge ) );
        }
        else {
            THROW_EXCEPTION( std::runtime_error,
                             "Duplicate edge '" << edge.sourceId << "'-'" << edge.targetId << "'"
                             << "' at row " << edge.rowIx
                             << " previous is defined at row " << eIt->second.rowIx );
        }
        edgeParentMap.insert( std::make_pair( edge.parentId, edgeKey ) );
    }
}

void Graph::write( std::ostringstream& out ) const
{
    out << "(tlp \"2.0\"\n"
        << "  (comments \"Generated by RGraphML, Alexey Stukalov @ CeMM\")\n";
        // @TODO date author

    write_comment( out, "Graph" );
    Rcpp::Rcerr << "Writing nodes...\n";
    out << "  (nodes";
    for ( node_map_t::const_iterator nIt = nodeMap.begin(); nIt != nodeMap.end(); ++nIt )
    {
        out << ' ' << nIt->first;
    }
    out << ")\n";
    Rcpp::Rcerr << "Writing edges...\n";
    for ( edge_map_t::const_iterator eIt = edgeMap.begin(); eIt != edgeMap.end(); ++eIt )
    {
        const Edge& e = eIt->second;
        out << "  (edge " << e.id << ' ' << e.sourceId << ' ' << e.targetId << ")\n";
    }
    
    Rcpp::Rcerr << "Writing clusters...\n";
    out << "\n";
    write_comment( out, "Node groups" );
    child_map_t nodeToParent;
    write_subgraph( out, ROOT_GRAPH_ID, nodeToParent );
    if ( nodeToParent.size() < nodeMap.size() ) {
        Rcpp::Rcerr << "Only " << nodeToParent.size() 
                    << " of " << nodeMap.size() << " node(s) have been written\n";
    }
    
    Rcpp::Rcerr << "Writing properties...\n";
    out << "\n";
    write_comment( out, "Properties" );
    for ( std::size_t i = 0; i < attrs.size(); ++i ) {
        const AttributeBase* attr = attrs[i];
        Rcpp::Rcerr << "Writing '" << attr->exportName << "'...\n";
        attr->write_header( out );
        for ( node_map_t::const_iterator nIt = nodeMap.begin(); nIt != nodeMap.end(); ++nIt )
        {
            attr->write_element_value( out, true, nIt->second.id, nIt->second.rowIx );
        }
        for ( edge_map_t::const_iterator eIt = edgeMap.begin(); eIt != edgeMap.end(); ++eIt )
        {
            attr->write_element_value( out, false, eIt->second.id, eIt->second.rowIx );
        }
        attr->write_footer( out );
    }
    write_comment( out, "Mapping of node groups to containing nodes" );
    write_metagraph_property( out );
    
    // name of the root graph
    write_comment( out, "Graph attributes" );
    out << "(graph_attributes 0\n"
    << "  (string \"name\" \"<root>\")\n"
    << ")\n";
    out << ")\n";
}

void Graph::write_subgraph(
    std::ostringstream&         out,
    node_id_t                   parentId,
    child_map_t&                nodeToParent
) const {
    Rcpp::Rcerr << "finding children of node " << parentId << "...\n";
    const_node_range_t nodes = parentMap.equal_range( parentId );
    bool hasClusterBlock = parentId != ROOT_GRAPH_ID || !parentMap.empty();
    if ( nodes.first == nodes.second ) {
        // no child nodes, exit
        return;
    }
    if ( hasClusterBlock ) {
        //Rcpp::Rcerr << "defining cluster " << parentId << "...\n";
        out << "(cluster " << (parentId+1) << " \"" << parentId << "\"\n"
            << "  (nodes ";
        for ( parent_map_t::const_iterator nIt = nodes.first; nIt != nodes.second; ++nIt )
        {
            out << ' ' << nIt->second;
        }
        out << ")\n";
    
        const_edge_range_t edges = edgeParentMap.equal_range( parentId );
        if ( edges.first != edges.second ) {
            out << "  (edges";
            for ( edge_parent_map_t::const_iterator eIt = edges.first; eIt != edges.second; ++eIt )
            {
                const Edge& edge = edgeMap.at( eIt->second );
                out << ' ' << edge.id;
            }
            out << ")\n";
        }
        out << ")\n";
    }
    // process subgraphs
    for ( parent_map_t::const_iterator nIt = nodes.first; nIt != nodes.second; ++nIt )
    {
        node_map_t::const_iterator n2It = nodeMap.find( nIt->second );
        if ( n2It == nodeMap.end() ) {
            THROW_EXCEPTION( std::runtime_error,
                             "Internal error: node '" << nIt->second << "' not found in the nodes map" );
        }
        const Node& node = n2It->second;
        child_map_t::iterator chIt = nodeToParent.find( node.id );
        if ( chIt != nodeToParent.end() ) {
            THROW_EXCEPTION( std::runtime_error,
                             "Node '" << node.id << "' already has a parent. Potential parent-child circular reference" );
        }
        nodeToParent.insert( chIt, std::make_pair( node.id, parentId ) );
        write_subgraph( out, node.id, nodeToParent );
    }
}

void Graph::write_metagraph_property(
    std::ostringstream&         out
) const {
    out << "(property 0 graph \"viewMetaGraph\"\n"
        << "  (default \"\" \"()\")\n";
    node_id_t prevParentId = ROOT_GRAPH_ID;
    for ( parent_map_t::const_iterator nIt = parentMap.begin(); nIt != parentMap.end(); ++nIt )
    {
        if ( nIt->first != prevParentId && nIt->first != ROOT_GRAPH_ID ) {
            // bind node to the cluster (they have same ids)
            out << "  (node " << nIt->first << " \"" << (nIt->first+1) << "\")\n";
            prevParentId = nIt->first;
        }
    }
    out << ")\n";
}

}

//??? The length of a string (in characters).
//???
//??? @param str input character vector
//??? @return characters in each element of the vector
// [[Rcpp::export]]
std::string DataFrameToTulip(
    const Rcpp::DataFrame&  nodes,
    const Rcpp::DataFrame&  edges,
    const std::string&      nodeIdCol = "id",
    const Rcpp::String&     parentIdCol = NA_STRING,
    const std::string&      edgeIdCol = "id",
    const std::string&      sourceCol = "source",
    const std::string&      targetCol = "target",
	const Rcpp::CharacterVector& nodeAttrs = Rcpp::CharacterVector::create(),
    const Rcpp::CharacterVector& edgeAttrs = Rcpp::CharacterVector::create(),
    bool                    isDirected = false
){
    Rcpp::Rcerr << "Initializing Tulip export...\n";
    Tulip::Graph graph( nodes, edges, nodeIdCol,
                 parentIdCol.get_sexp() != NA_STRING ? (std::string)parentIdCol : std::string(),
                 edgeIdCol, sourceCol, targetCol,
                 nodeAttrs, edgeAttrs,
                 isDirected );
    graph.init();

    Rcpp::Rcerr << "Generating Tulip graph definition...\n";
    std::ostringstream out;
    graph.write( out );

    return ( out.str() );
}
