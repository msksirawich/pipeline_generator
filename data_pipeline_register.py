import yaml
import os
import re
import datetime
import pandas as pd
import tempfile

# Set page configuration
st.set_page_config(
    page_title="Data Pipeline Configuration Generator",
    page_icon="🔄",
    layout="wide"
)

# Global constants
SUPPORTED_DATA_TYPES = [
    "string", "int", "bigint", "smallint", "decimal(18, 4)", 
    "timestamp", "date", "boolean", "float", "double"
]

# Helper functions
def sanitize_name(name):
    """Convert a name to a valid identifier"""
    return re.sub(r'\W+', '_', name).lower()

def create_yaml_file(config_data, filename, output_dir):
    """Create a YAML file with the provided configuration data"""
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    file_path = os.path.join(output_dir, filename)
    
    # Write the YAML file
    with open(file_path, 'w') as f:
        yaml.dump(config_data, f, default_flow_style=False, sort_keys=False)
    
    return file_path

def load_existing_config(file_path):
    """Load an existing YAML configuration file"""
    try:
        with open(file_path, 'r') as f:
            return yaml.safe_load(f)
    except Exception as e:
        st.error(f"Error loading configuration file: {str(e)}")
        return None

# Initialize session state
if 'column_mappings' not in st.session_state:
    st.session_state.column_mappings = []

if 'source_config' not in st.session_state:
    st.session_state.source_config = {}

if 'validator_config' not in st.session_state:
    st.session_state.validator_config = {
        'tier1': {'control_file_flag': 'y', 'data_quality_rules': []},
        'tier2': {'data_quality_rules': []}
    }

if 'table_config' not in st.session_state:
    st.session_state.table_config = {
        'metadata': {
            'tier1': {
                'technical_columns': [],
                'partition_columns': []
            },
            'tier2': {
                'technical_columns': [],
                'partition_columns': [],
                'primary_keys': [],
                'historical_load_columns': []
            }
        },
        'columns': []
    }

# UI Components
def render_header():
    """Render the header section"""
    st.title("Data Pipeline Configuration Generator")
    st.markdown("""
    Generate YAML configuration files for data pipelines. This tool helps you create 
    standardized configurations that can be stored in Git and used by your data pipeline framework.
    """)

def render_sidebar():
    """Render the sidebar with navigation and options"""
    st.sidebar.title("Configuration Sections")
    
    # Navigation
    section = st.sidebar.radio(
        "Navigate to section:",
        ["Source Configuration", "Validator Configuration", "Table Configuration", "Column Mappings", "Generate YAML"]
    )
    
    # Template options
    st.sidebar.header("Templates")
    
    template_option = st.sidebar.selectbox(
        "Start from template:",
        ["None", "POS Branch Template", "Sales Transaction Template", "Customer Master Template"]
    )
    
    if st.sidebar.button("Load Template"):
        if template_option == "POS Branch Template":
            # Load the POS Branch template
            load_pos_branch_template()
            st.sidebar.success("POS Branch template loaded!")
        elif template_option == "Sales Transaction Template":
            # Load the Sales Transaction template
            load_sales_transaction_template()
            st.sidebar.success("Sales Transaction template loaded!")
        elif template_option != "None":
            st.sidebar.info(f"{template_option} not implemented yet")
    
    # Load from file
    st.sidebar.header("Load Existing Configuration")
    uploaded_file = st.sidebar.file_uploader("Upload YAML file", type=['yml', 'yaml'])
    
    if uploaded_file is not None:
        # Save the uploaded file to a temporary location
        with tempfile.NamedTemporaryFile(delete=False, suffix='.yml') as tmp_file:
            tmp_file.write(uploaded_file.getvalue())
            tmp_path = tmp_file.name
        
        # Load the configuration
        config = load_existing_config(tmp_path)
        if config:
            load_config_into_session(config)
            st.sidebar.success("Configuration loaded successfully!")
        
        # Clean up the temporary file
        os.unlink(tmp_path)
    
    # Output directory setting
    st.sidebar.header("Output Settings")
    output_dir = st.sidebar.text_input("Output Directory", value="./pipeline_configs")
    
    return section, output_dir

def load_sales_transaction_template():
    """Load the Sales Transaction template into session state"""
    # Set values based on the provided t2_pos-txn_sales_order_header.yml
    st.session_state.source_config = {
        'source_system': 'pos',
        'catalog': '${catalog}',
        'delay_day': '1',
        'active_flag': 'y',
        'landing_bucket': '${tier0_bucket}',
        'persisted_bucket': '${tier1_bucket}',
        'data_timezone': 'UTC',
        'control_file_landing_location': '/ctl/pos/df_sohdr/${data_date}/',
        'data_file_landing_location': '/data/pos/df_sohdr/${data_date}/',
        'control_file_regex': 'df_sohdr*.ctl',
        'data_file_regex': 'df_sohdr*.csv',
        'control_file_format': {
            'header': False,
            'delimiter': '|'
        },
        'data_file_format': {
            'header': True,
            'delimiter': '|',
            'quote': '"',
            'escape': '"',
            'charset': 'utf-8'
        }
    }
    
    st.session_state.validator_config = {
        'tier1': {
            'control_file_flag': 'y',
            'data_quality_rules': [
                {'rule': 'check_duplicate', 'column': ['branch_no', 'shopping_card', 'order_no']},
                {'rule': 'check_null', 'column': ['branch_no', 'shopping_card', 'order_no']}
            ]
        },
        'tier2': {
            'data_quality_rules': [
                {'rule': 'check_duplicate', 'column': ['branch_code', 'shopping_card_id', 'order_id']},
                {'rule': 'check_null', 'column': ['branch_code', 'shopping_card_id', 'order_id']}
            ]
        }
    }
    
    st.session_state.table_config = {
        'metadata': {
            'tier1': {
                'load_type': 'full_dump',
                'schema': 't1_pos',
                'table': 'df_sohdr',
                'partition_columns': ['source', 'dp_data_dt'],
                'technical_columns': [
                    {'name': 'source', 'data_type': 'string'},
                    {'name': 'dp_load_ts', 'data_type': 'timestamp'},
                    {'name': 'dp_data_dt', 'data_type': 'date'}
                ]
            },
            'tier2': {
                'load_type': 'scd1',
                'schema': 't2_pos',
                'table': 'txn_sales_order_header',
                'primary_keys': ['branch_code', 'shopping_card_id', 'order_id'],
                'partition_columns': ['source'],
                'technical_columns': [
                    {'name': 'source', 'data_type': 'string'},
                    {'name': 'dp_load_ts', 'data_type': 'timestamp'},
                    {'name': 'dp_data_dt', 'data_type': 'date'}
                ]
            }
        },
        'columns': [
            {'tier_1': 'branch_no', 'tier_2': 'branch_code', 'data_type': 'string'},
            {'tier_1': 'shopping_card', 'tier_2': 'shopping_card_id', 'data_type': 'string'},
            {'tier_1': 'order_no', 'tier_2': 'order_id', 'data_type': 'string'},
            {'tier_1': 'order_date', 'tier_2': 'order_timestamp', 'data_type': 'timestamp'},
            {'tier_1': 'tour_code', 'tier_2': 'tour_code', 'data_type': 'string'},
            {'tier_1': 'cust_type_code', 'tier_2': 'customer_type_code', 'data_type': 'string'},
            {'tier_1': 'airline_code', 'tier_2': 'airline_code', 'data_type': 'string'},
            {'tier_1': 'flight_code', 'tier_2': 'flight_code', 'data_type': 'string'},
            {'tier_1': 'flight_date', 'tier_2': 'flight_date', 'data_type': 'date'},
            {'tier_1': 'flight_time', 'tier_2': 'flight_time', 'data_type': 'string'},
            {'tier_1': 'country_code', 'tier_2': 'country_code', 'data_type': 'string'},
            {'tier_1': 'order_status', 'tier_2': 'order_status_type', 'data_type': 'string'},
            {'tier_1': 'posid', 'tier_2': 'pos_id', 'data_type': 'string'},
            {'tier_1': 'cashier_mac', 'tier_2': 'cashier_machine_id', 'data_type': 'string'},
            {'tier_1': 'cashier_code', 'tier_2': 'cashier_code', 'data_type': 'string'},
            {'tier_1': 'update_date_sale', 'tier_2': 'sale_update_timestamp', 'data_type': 'timestamp'},
            {'tier_1': 'add_datetime', 'tier_2': 'add_timestamp', 'data_type': 'timestamp'},
            {'tier_1': 'update_datetime', 'tier_2': 'update_timestamp', 'data_type': 'timestamp'},
            {'tier_1': 'user_add', 'tier_2': 'user_add_id', 'data_type': 'string'},
            {'tier_1': 'user_update', 'tier_2': 'user_update_id', 'data_type': 'string'},
            {'tier_1': 'time_stamp', 'tier_2': 'timestamp_code', 'data_type': 'string'},
            {'tier_1': 'lock_address', 'tier_2': 'lock_address_code', 'data_type': 'string'},
            {'tier_1': 'cancel_to_order', 'tier_2': 'cancel_to_order_id', 'data_type': 'int'},
            {'tier_1': 'hotel_code', 'tier_2': 'hotel_code', 'data_type': 'string'},
            {'tier_1': 'hotel_source', 'tier_2': 'hotel_source_code', 'data_type': 'string'},
            {'tier_1': 'shop_ref', 'tier_2': 'reference_shopping_card_id', 'data_type': 'string'},
            {'tier_1': 'machine_tax', 'tier_2': 'machine_tax_id', 'data_type': 'string'},
            {'tier_1': 'ref_doc', 'tier_2': 'reference_document_id', 'data_type': 'string'},
            {'tier_1': 'cardtypecode', 'tier_2': 'card_type_code', 'data_type': 'string'},
            {'tier_1': 'embossid', 'tier_2': 'emboss_id', 'data_type': 'string'},
            {'tier_1': 'cardtypeid', 'tier_2': 'card_type_id', 'data_type': 'string'},
            {'tier_1': 'lvheaderkey', 'tier_2': 'lvheader_key', 'data_type': 'decimal(13, 2)'},
            {'tier_1': 'alipaysession', 'tier_2': 'alipay_session_number', 'data_type': 'string'},
            {'tier_1': 'paid_guid', 'tier_2': 'paid_guid', 'data_type': 'string'},
            {'tier_1': 'site', 'tier_2': 'site_name', 'data_type': 'string'}
        ]
    }


def load_pos_branch_template():
    """Load the POS Branch template into session state"""
    st.session_state.source_config = {
        'source_system': 'pos',
        'catalog': '${catalog}',
        'delay_day': '1',
        'active_flag': 'y',
        'landing_bucket': '${tier0_bucket}',
        'persisted_bucket': '${tier1_bucket}',
        'data_timezone': 'UTC',
        'control_file_landing_location': '/ctl/pos/df_branch/${data_date}/',
        'data_file_landing_location': '/data/pos/df_branch/${data_date}/',
        'control_file_regex': 'df_branch*.ctl',
        'data_file_regex': 'df_branch*.csv',
        'control_file_format': {
            'header': False,
            'delimiter': '|'
        },
        'data_file_format': {
            'header': True,
            'delimiter': '|',
            'quote': '"',
            'escape': '"',
            'charset': 'utf-8'
        }
    }
    
    st.session_state.validator_config = {
        'tier1': {
            'control_file_flag': 'y',
            'data_quality_rules': [
                {'rule': 'check_duplicate', 'column': ['branch_no', 'plant', 'site']},
                {'rule': 'check_null', 'column': ['branch_no', 'plant', 'site']}
            ]
        },
        'tier2': {
            'data_quality_rules': [
                {'rule': 'check_duplicate', 'column': ['branch_code', 'plant_code', 'site_name']},
                {'rule': 'check_null', 'column': ['branch_code', 'plant_code', 'site_name']}
            ]
        }
    }
    
    st.session_state.table_config = {
        'metadata': {
            'tier1': {
                'load_type': 'full_dump',
                'schema': 't1_pos',
                'table': 'df_branch',
                'partition_columns': ['source', 'dp_data_dt'],
                'technical_columns': [
                    {'name': 'source', 'data_type': 'string'},
                    {'name': 'dp_load_ts', 'data_type': 'timestamp'},
                    {'name': 'dp_data_dt', 'data_type': 'date'}
                ]
            },
            'tier2': {
                'load_type': 'scd2',
                'schema': 't2_pos',
                'table': 'mst_branch',
                'primary_keys': ['branch_code', 'plant_code', 'site_name'],
                'partition_columns': ['is_current', 'source'],
                'technical_columns': [
                    {'name': 'source', 'data_type': 'string'},
                    {'name': 'dp_load_ts', 'data_type': 'timestamp'},
                    {'name': 'dp_data_dt', 'data_type': 'date'}
                ],
                'historical_load_columns': [
                    {'name': 'is_current', 'data_type': 'string'},
                    {'name': 'effective_start_date', 'data_type': 'date'},
                    {'name': 'effective_end_date', 'data_type': 'date'}
                ]
            }
        },
        'columns': [
            {'tier_1': 'branch_no', 'tier_2': 'branch_code', 'data_type': 'string'},
            {'tier_1': 'name', 'tier_2': 'branch_name', 'data_type': 'string'},
            {'tier_1': 'plant', 'tier_2': 'plant_code', 'data_type': 'string'},
            {'tier_1': 'site', 'tier_2': 'site_name', 'data_type': 'string'},
            {'tier_1': 'address1', 'tier_2': 'address_1_name', 'data_type': 'string'}
        ]
    }
    
    st.session_state.validator_config = {
        'tier1': {
            'control_file_flag': 'y',
            'data_quality_rules': [
                {'rule': 'check_duplicate', 'column': ['branch_no', 'shopping_card', 'order_no']},
                {'rule': 'check_null', 'column': ['branch_no', 'shopping_card', 'order_no']}
            ]
        },
        'tier2': {
            'data_quality_rules': [
                {'rule': 'check_duplicate', 'column': ['branch_code', 'shopping_card_id', 'order_id']},
                {'rule': 'check_null', 'column': ['branch_code', 'shopping_card_id', 'order_id']}
            ]
        }
    }
    
    st.session_state.table_config = {
        'metadata': {
            'tier1': {
                'load_type': 'full_dump',
                'schema': 't1_pos',
                'table': 'df_sohdr',
                'partition_columns': ['source', 'dp_data_dt'],
                'technical_columns': [
                    {'name': 'source', 'data_type': 'string'},
                    {'name': 'dp_load_ts', 'data_type': 'timestamp'},
                    {'name': 'dp_data_dt', 'data_type': 'date'}
                ]
            },
            'tier2': {
                'load_type': 'scd1',
                'schema': 't2_pos',
                'table': 'txn_sales_order_header',
                'primary_keys': ['branch_code', 'shopping_card_id', 'order_id'],
                'partition_columns': ['source'],
                'technical_columns': [
                    {'name': 'source', 'data_type': 'string'},
                    {'name': 'dp_load_ts', 'data_type': 'timestamp'},
                    {'name': 'dp_data_dt', 'data_type': 'date'}
                ]
            }
        },
        'columns': [
            {'tier_1': 'branch_no', 'tier_2': 'branch_code', 'data_type': 'string'},
            {'tier_1': 'shopping_card', 'tier_2': 'shopping_card_id', 'data_type': 'string'},
            {'tier_1': 'order_no', 'tier_2': 'order_id', 'data_type': 'string'},
            {'tier_1': 'order_date', 'tier_2': 'order_timestamp', 'data_type': 'timestamp'},
            {'tier_1': 'tour_code', 'tier_2': 'tour_code', 'data_type': 'string'},
            {'tier_1': 'cust_type_code', 'tier_2': 'customer_type_code', 'data_type': 'string'},
            {'tier_1': 'airline_code', 'tier_2': 'airline_code', 'data_type': 'string'},
            {'tier_1': 'flight_code', 'tier_2': 'flight_code', 'data_type': 'string'},
            {'tier_1': 'flight_date', 'tier_2': 'flight_date', 'data_type': 'date'},
            {'tier_1': 'flight_time', 'tier_2': 'flight_time', 'data_type': 'string'},
            {'tier_1': 'country_code', 'tier_2': 'country_code', 'data_type': 'string'},
            {'tier_1': 'order_status', 'tier_2': 'order_status_type', 'data_type': 'string'},
            {'tier_1': 'posid', 'tier_2': 'pos_id', 'data_type': 'string'},
            {'tier_1': 'cashier_mac', 'tier_2': 'cashier_machine_id', 'data_type': 'string'},
            {'tier_1': 'cashier_code', 'tier_2': 'cashier_code', 'data_type': 'string'},
            {'tier_1': 'update_date_sale', 'tier_2': 'sale_update_timestamp', 'data_type': 'timestamp'},
            {'tier_1': 'add_datetime', 'tier_2': 'add_timestamp', 'data_type': 'timestamp'},
            {'tier_1': 'update_datetime', 'tier_2': 'update_timestamp', 'data_type': 'timestamp'},
            {'tier_1': 'user_add', 'tier_2': 'user_add_id', 'data_type': 'string'},
            {'tier_1': 'user_update', 'tier_2': 'user_update_id', 'data_type': 'string'},
            {'tier_1': 'time_stamp', 'tier_2': 'timestamp_code', 'data_type': 'string'},
            {'tier_1': 'lock_address', 'tier_2': 'lock_address_code', 'data_type': 'string'},
            {'tier_1': 'cancel_to_order', 'tier_2': 'cancel_to_order_id', 'data_type': 'int'},
            {'tier_1': 'hotel_code', 'tier_2': 'hotel_code', 'data_type': 'string'},
            {'tier_1': 'hotel_source', 'tier_2': 'hotel_source_code', 'data_type': 'string'},
            {'tier_1': 'shop_ref', 'tier_2': 'reference_shopping_card_id', 'data_type': 'string'},
            {'tier_1': 'machine_tax', 'tier_2': 'machine_tax_id', 'data_type': 'string'},
            {'tier_1': 'ref_doc', 'tier_2': 'reference_document_id', 'data_type': 'string'},
            {'tier_1': 'cardtypecode', 'tier_2': 'card_type_code', 'data_type': 'string'},
            {'tier_1': 'embossid', 'tier_2': 'emboss_id', 'data_type': 'string'},
            {'tier_1': 'cardtypeid', 'tier_2': 'card_type_id', 'data_type': 'string'},
            {'tier_1': 'lvheaderkey', 'tier_2': 'lvheader_key', 'data_type': 'decimal(13, 2)'},
            {'tier_1': 'alipaysession', 'tier_2': 'alipay_session_number', 'data_type': 'string'},
            {'tier_1': 'paid_guid', 'tier_2': 'paid_guid', 'data_type': 'string'},
            {'tier_1': 'site', 'tier_2': 'site_name', 'data_type': 'string'}
        ]
    }

def load_config_into_session(config):
    """Load a configuration dictionary into session state"""
    if 'source_config' in config:
        st.session_state.source_config = config['source_config']
    
    if 'validator_config' in config:
        st.session_state.validator_config = config['validator_config']
    
    if 'table_config' in config:
        st.session_state.table_config = config['table_config']
        
        # Ensure columns are properly loaded
        if 'columns' in config['table_config']:
            st.session_state.column_mappings = config['table_config']['columns']

def render_source_config():
    """Render the source configuration section"""
    st.header("Source Configuration")
    st.markdown("Define the source system and file format details.")
    
    source_config = st.session_state.source_config
    
    col1, col2 = st.columns(2)
    
    with col1:
        source_config['source_system'] = st.text_input(
            "Source System", 
            value=source_config.get('source_system', ''),
            help="The system or application that generates the source data"
        )
        
        source_config['catalog'] = st.text_input(
            "Catalog", 
            value=source_config.get('catalog', '${catalog}'),
            help="Catalog identifier, can use variables like ${catalog}"
        )
        
        source_config['delay_day'] = st.text_input(
            "Delay Day", 
            value=source_config.get('delay_day', '1'),
            help="Number of days delay for data processing"
        )
        
        source_config['active_flag'] = st.selectbox(
            "Active Flag", 
            options=['y', 'n'],
            index=0 if source_config.get('active_flag', 'y') == 'y' else 1,
            help="Whether this pipeline is active"
        )
    
    with col2:
        source_config['landing_bucket'] = st.text_input(
            "Landing Bucket", 
            value=source_config.get('landing_bucket', '${tier0_bucket}'),
            help="Bucket for landing raw data"
        )
        
        source_config['persisted_bucket'] = st.text_input(
            "Persisted Bucket", 
            value=source_config.get('persisted_bucket', '${tier1_bucket}'),
            help="Bucket for persisted data"
        )
        
        source_config['data_timezone'] = st.text_input(
            "Data Timezone", 
            value=source_config.get('data_timezone', 'UTC'),
            help="Timezone of the source data"
        )
    
    st.subheader("File Locations")
    
    col1, col2 = st.columns(2)
    
    with col1:
        source_config['control_file_landing_location'] = st.text_input(
            "Control File Landing Location", 
            value=source_config.get('control_file_landing_location', '/ctl/${source_system}/${table}/${data_date}/'),
            help="Path pattern for control files"
        )
        
        source_config['data_file_landing_location'] = st.text_input(
            "Data File Landing Location", 
            value=source_config.get('data_file_landing_location', '/data/${source_system}/${table}/${data_date}/'),
            help="Path pattern for data files"
        )
    
    with col2:
        source_config['control_file_regex'] = st.text_input(
            "Control File Regex", 
            value=source_config.get('control_file_regex', '*.ctl'),
            help="Regular expression to match control files"
        )
        
        source_config['data_file_regex'] = st.text_input(
            "Data File Regex", 
            value=source_config.get('data_file_regex', '*.csv'),
            help="Regular expression to match data files"
        )
    
    st.subheader("File Formats")
    
    # Control file format
    st.write("Control File Format")
    
    if 'control_file_format' not in source_config:
        source_config['control_file_format'] = {'header': False, 'delimiter': '|'}
    
    control_format = source_config['control_file_format']
    
    col1, col2 = st.columns(2)
    
    with col1:
        control_format['header'] = st.checkbox(
            "Control File Has Header", 
            value=control_format.get('header', False)
        )
    
    with col2:
        control_format['delimiter'] = st.text_input(
            "Control File Delimiter", 
            value=control_format.get('delimiter', '|')
        )
    
    # Data file format
    st.write("Data File Format")
    
    if 'data_file_format' not in source_config:
        source_config['data_file_format'] = {
            'header': True, 
            'delimiter': ',', 
            'quote': '"', 
            'escape': '"',
            'charset': 'utf-8'
        }
    
    data_format = source_config['data_file_format']
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        data_format['header'] = st.checkbox(
            "Data File Has Header", 
            value=data_format.get('header', True)
        )
        
        data_format['delimiter'] = st.text_input(
            "Data File Delimiter", 
            value=data_format.get('delimiter', ',')
        )
    
    with col2:
        data_format['quote'] = st.text_input(
            "Quote Character", 
            value=data_format.get('quote', '"')
        )
        
        data_format['escape'] = st.text_input(
            "Escape Character", 
            value=data_format.get('escape', '"')
        )
    
    with col3:
        data_format['charset'] = st.text_input(
            "Character Set", 
            value=data_format.get('charset', 'utf-8')
        )
    
    # Update session state
    st.session_state.source_config = source_config

def render_validator_config():
    """Render the validator configuration section"""
    st.header("Validator Configuration")
    st.markdown("Define data quality rules for validation.")
    
    validator_config = st.session_state.validator_config
    
    # Tier 1 validation
    st.subheader("Tier 1 Validation")
    
    if 'tier1' not in validator_config:
        validator_config['tier1'] = {'control_file_flag': 'y', 'data_quality_rules': []}
    
    tier1_config = validator_config['tier1']
    
    tier1_config['control_file_flag'] = st.selectbox(
        "Control File Flag", 
        options=['y', 'n'],
        index=0 if tier1_config.get('control_file_flag', 'y') == 'y' else 1,
        help="Whether to validate against control file"
    )
    
    st.write("Tier 1 Data Quality Rules")
    
    # Convert existing rules to DataFrame for editing
    tier1_rules = []
    for rule in tier1_config.get('data_quality_rules', []):
        rule_type = rule.get('rule', '')
        columns = rule.get('column', [])
        columns_str = ', '.join(columns) if isinstance(columns, list) else columns
        tier1_rules.append({'rule': rule_type, 'columns': columns_str})
    
    # Create DataFrame editor
    tier1_rules_df = pd.DataFrame(tier1_rules) if tier1_rules else pd.DataFrame(columns=['rule', 'columns'])
    edited_tier1_rules = st.data_editor(
        tier1_rules_df, 
        num_rows="dynamic",
        key="tier1_rules_editor",  # Added unique key
        use_container_width=True
    )
    
    # Update tier1 rules
    tier1_config['data_quality_rules'] = []
    for _, row in edited_tier1_rules.iterrows():
        if row['rule'] and row['columns']:
            columns = [col.strip() for col in row['columns'].split(',')]
            tier1_config['data_quality_rules'].append({
                'rule': row['rule'],
                'column': columns
            })
    
    # Tier 2 validation
    st.subheader("Tier 2 Validation")
    
    if 'tier2' not in validator_config:
        validator_config['tier2'] = {'data_quality_rules': []}
    
    tier2_config = validator_config['tier2']
    
    st.write("Tier 2 Data Quality Rules")
    
    # Convert existing rules to DataFrame for editing
    tier2_rules = []
    for rule in tier2_config.get('data_quality_rules', []):
        rule_type = rule.get('rule', '')
        columns = rule.get('column', [])
        columns_str = ', '.join(columns) if isinstance(columns, list) else columns
        tier2_rules.append({'rule': rule_type, 'columns': columns_str})
    
    # Create DataFrame editor
    tier2_rules_df = pd.DataFrame(tier2_rules) if tier2_rules else pd.DataFrame(columns=['rule', 'columns'])
    edited_tier2_rules = st.data_editor(
        tier2_rules_df, 
        num_rows="dynamic",
        key="tier2_rules_editor",  # Added unique key
        use_container_width=True
    )
    
    # Update tier2 rules
    tier2_config['data_quality_rules'] = []
    for _, row in edited_tier2_rules.iterrows():
        if row['rule'] and row['columns']:
            columns = [col.strip() for col in row['columns'].split(',')]
            tier2_config['data_quality_rules'].append({
                'rule': row['rule'],
                'column': columns
            })
    
    # Update session state
    st.session_state.validator_config = validator_config

def render_table_config():
    """Render the table configuration section"""
    st.header("Table Configuration")
    st.markdown("Define table metadata and structure.")
    
    table_config = st.session_state.table_config
    
    if 'metadata' not in table_config:
        table_config['metadata'] = {
            'tier1': {},
            'tier2': {}
        }
    
    metadata = table_config['metadata']
    
    # Tier 1 table configuration
    st.subheader("Tier 1 Table Configuration")
    
    if 'tier1' not in metadata:
        metadata['tier1'] = {}
    
    tier1_meta = metadata['tier1']
    
    col1, col2 = st.columns(2)
    
    with col1:
        tier1_meta['load_type'] = st.selectbox(
            "Tier 1 Load Type", 
            options=['full_dump', 'incremental'],
            index=0 if tier1_meta.get('load_type', 'full_dump') == 'full_dump' else 1,
            help="The loading strategy for tier 1"
        )
        
        tier1_meta['schema'] = st.text_input(
            "Tier 1 Schema", 
            value=tier1_meta.get('schema', 't1_'),
            help="Database schema for tier 1 table"
        )
    
    with col2:
        tier1_meta['table'] = st.text_input(
            "Tier 1 Table Name", 
            value=tier1_meta.get('table', ''),
            help="Table name for tier 1"
        )
    
    # Tier 1 partition columns
    st.write("Tier 1 Partition Columns")
    
    partition_cols_str = ', '.join(tier1_meta.get('partition_columns', []))
    new_partition_cols = st.text_input(
        "Partition Columns (comma-separated)", 
        value=partition_cols_str
    )
    tier1_meta['partition_columns'] = [col.strip() for col in new_partition_cols.split(',')] if new_partition_cols else []
    
    # Tier 1 technical columns
    st.write("Tier 1 Technical Columns")
    
    if 'technical_columns' not in tier1_meta:
        tier1_meta['technical_columns'] = []
    
    # Convert existing technical columns to DataFrame for editing
    tech_cols = []
    for col in tier1_meta.get('technical_columns', []):
        tech_cols.append({
            'name': col.get('name', ''),
            'data_type': col.get('data_type', '')
        })
    
    # Create DataFrame editor
    tech_cols_df = pd.DataFrame(tech_cols) if tech_cols else pd.DataFrame(columns=['name', 'data_type'])
    edited_tech_cols = st.data_editor(
        tech_cols_df, 
        num_rows="dynamic",
        key="tier1_tech_columns_editor",  # Added unique key
        column_config={
            "data_type": st.column_config.SelectboxColumn(
                "Data Type",
                options=SUPPORTED_DATA_TYPES,
                required=True
            )
        },
        use_container_width=True
    )
    
    # Update technical columns
    tier1_meta['technical_columns'] = []
    for _, row in edited_tech_cols.iterrows():
        if row['name'] and row['data_type']:
            tier1_meta['technical_columns'].append({
                'name': row['name'],
                'data_type': row['data_type']
            })
    
    # Tier 2 table configuration
    st.subheader("Tier 2 Table Configuration")
    
    if 'tier2' not in metadata:
        metadata['tier2'] = {}
    
    tier2_meta = metadata['tier2']
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Function to handle load type changes
        def update_load_type():
            # If switching to SCD2, initialize the historical columns if they don't exist
            if tier2_meta['load_type'] == 'scd2' and ('historical_load_columns' not in tier2_meta or not tier2_meta['historical_load_columns']):
                tier2_meta['historical_load_columns'] = [
                    {'name': 'is_current', 'data_type': 'string'},
                    {'name': 'effective_start_date', 'data_type': 'date'},
                    {'name': 'effective_end_date', 'data_type': 'date'}
                ]
            # If switching away from SCD2, remove historical columns
            elif tier2_meta['load_type'] != 'scd2' and 'historical_load_columns' in tier2_meta:
                del tier2_meta['historical_load_columns']
    
    col1, col2 = st.columns(2)
    
    with col1:
        previous_load_type = tier2_meta.get('load_type', '')
        tier2_meta['load_type'] = st.selectbox(
            "Tier 2 Load Type", 
            options=['scd1', 'scd2'],
            index=1 if previous_load_type == 'scd2' else 0,
            help="The loading strategy for tier 2"
        )
        
        # Check if load type changed and update accordingly
        if previous_load_type != tier2_meta['load_type']:
            update_load_type()
        
        tier2_meta['schema'] = st.text_input(
            "Tier 2 Schema", 
            value=tier2_meta.get('schema', 't2_'),
            help="Database schema for tier 2 table"
        )
    
    with col2:
        tier2_meta['table'] = st.text_input(
            "Tier 2 Table Name", 
            value=tier2_meta.get('table', ''),
            help="Table name for tier 2"
        )
    
    # Tier 2 primary keys
    st.write("Tier 2 Primary Keys")
    
    primary_keys_str = ', '.join(tier2_meta.get('primary_keys', []))
    new_primary_keys = st.text_input(
        "Primary Keys (comma-separated)", 
        value=primary_keys_str
    )
    tier2_meta['primary_keys'] = [key.strip() for key in new_primary_keys.split(',')] if new_primary_keys else []
    
    # Tier 2 partition columns
    st.write("Tier 2 Partition Columns")
    
    t2_partition_cols_str = ', '.join(tier2_meta.get('partition_columns', []))
    new_t2_partition_cols = st.text_input(
        "Tier 2 Partition Columns (comma-separated)", 
        value=t2_partition_cols_str
    )
    tier2_meta['partition_columns'] = [col.strip() for col in new_t2_partition_cols.split(',')] if new_t2_partition_cols else []
    
    # Tier 2 technical columns
    st.write("Tier 2 Technical Columns")
    
    if 'technical_columns' not in tier2_meta:
        tier2_meta['technical_columns'] = []
    
    # Convert existing technical columns to DataFrame for editing
    t2_tech_cols = []
    for col in tier2_meta.get('technical_columns', []):
        t2_tech_cols.append({
            'name': col.get('name', ''),
            'data_type': col.get('data_type', '')
        })
    
    # Create DataFrame editor
    t2_tech_cols_df = pd.DataFrame(t2_tech_cols) if t2_tech_cols else pd.DataFrame(columns=['name', 'data_type'])
    edited_t2_tech_cols = st.data_editor(
        t2_tech_cols_df, 
        num_rows="dynamic",
        key="tier2_tech_columns_editor",  # Added unique key parameter
        column_config={
            "data_type": st.column_config.SelectboxColumn(
                "Data Type",
                options=SUPPORTED_DATA_TYPES,
                required=True
            )
        },
        use_container_width=True
    )
    
    # Update technical columns
    tier2_meta['technical_columns'] = []
    for _, row in edited_t2_tech_cols.iterrows():
        if row['name'] and row['data_type']:
            tier2_meta['technical_columns'].append({
                'name': row['name'],
                'data_type': row['data_type']
            })
    
        # Historical load columns (for SCD2 only)
    if tier2_meta['load_type'] == 'scd2':
        st.write("Historical Load Columns (SCD2)")
        
        if 'historical_load_columns' not in tier2_meta:
            tier2_meta['historical_load_columns'] = []
        
        # Convert existing historical columns to DataFrame for editing
        hist_cols = []
        for col in tier2_meta.get('historical_load_columns', []):
            hist_cols.append({
                'name': col.get('name', ''),
                'data_type': col.get('data_type', '')
            })
        
        # Create DataFrame editor
        hist_cols_df = pd.DataFrame(hist_cols) if hist_cols else pd.DataFrame(columns=['name', 'data_type'])
        edited_hist_cols = st.data_editor(
            hist_cols_df, 
            num_rows="dynamic",
            key="historical_columns_editor",  # Added unique key
            column_config={
                "data_type": st.column_config.SelectboxColumn(
                    "Data Type",
                    options=SUPPORTED_DATA_TYPES,
                    required=True
                )
            },
            use_container_width=True
        )
        
        # Update historical columns
        tier2_meta['historical_load_columns'] = []
        for _, row in edited_hist_cols.iterrows():
            if row['name'] and row['data_type']:
                tier2_meta['historical_load_columns'].append({
                    'name': row['name'],
                    'data_type': row['data_type']
                })
    else:
        # Remove historical_load_columns if load type is not SCD2
        if 'historical_load_columns' in tier2_meta:
            del tier2_meta['historical_load_columns']
    
    # Update session state
    table_config['metadata'] = metadata
    st.session_state.table_config = table_config

def render_column_mappings():
    """Render the column mappings section"""
    st.header("Column Mappings")
    st.markdown("Define mappings between tier 1 and tier 2 columns.")
    
    # Get current mappings from session state
    columns = st.session_state.table_config.get('columns', [])
    
    # Convert to DataFrame for editing
    column_data = []
    for col in columns:
        column_data.append({
            'tier_1': col.get('tier_1', ''),
            'tier_2': col.get('tier_2', ''),
            'data_type': col.get('data_type', '')
        })
    
    # Create DataFrame editor
    df = pd.DataFrame(column_data) if column_data else pd.DataFrame(columns=['tier_1', 'tier_2', 'data_type'])
    edited_df = st.data_editor(
        df, 
        num_rows="dynamic",
        key="column_mappings_editor",  # Added unique key
        column_config={
            "data_type": st.column_config.SelectboxColumn(
                "Data Type",
                options=SUPPORTED_DATA_TYPES,
                required=True
            )
        },
        use_container_width=True
    )
    
    # Update mappings in session state
    updated_columns = []
    for _, row in edited_df.iterrows():
        if row['tier_1'] and row['tier_2'] and row['data_type']:
            updated_columns.append({
                'tier_1': row['tier_1'],
                'tier_2': row['tier_2'],
                'data_type': row['data_type']
            })
    
    st.session_state.table_config['columns'] = updated_columns
    
    # Bulk upload option
    st.subheader("Bulk Upload Column Mappings")
    st.markdown("Upload a CSV file with column mappings. Format: tier_1,tier_2,data_type")
    
    uploaded_csv = st.file_uploader("Upload column mappings CSV", type=['csv'])
    
    if uploaded_csv is not None:
        try:
            df_upload = pd.read_csv(uploaded_csv)
            
            # Validate columns
            required_cols = ['tier_1', 'tier_2', 'data_type']
            missing_cols = [col for col in required_cols if col not in df_upload.columns]
            
            if missing_cols:
                st.error(f"Missing required columns: {', '.join(missing_cols)}")
            else:
                # Validate data types
                invalid_types = df_upload[~df_upload['data_type'].isin(SUPPORTED_DATA_TYPES)]
                
                if not invalid_types.empty:
                    st.error(f"Found unsupported data types: {', '.join(invalid_types['data_type'].unique())}")
                else:
                    # Add to existing mappings
                    bulk_columns = []
                    for _, row in df_upload.iterrows():
                        bulk_columns.append({
                            'tier_1': row['tier_1'],
                            'tier_2': row['tier_2'],
                            'data_type': row['data_type']
                        })
                    
                    # Confirm before adding
                    if st.button("Add these mappings"):
                        st.session_state.table_config['columns'].extend(bulk_columns)
                        st.success(f"Added {len(bulk_columns)} column mappings!")
                        st.experimental_rerun()
        
        except Exception as e:
            st.error(f"Error processing CSV: {str(e)}")

def render_generate_yaml():
    """Render the YAML generation section"""
    st.header("Generate YAML Configuration")
    st.markdown("Review and generate the final YAML configuration file.")
    
    # Get all configs from session state
    source_config = st.session_state.source_config
    validator_config = st.session_state.validator_config
    table_config = st.session_state.table_config
    
    # Check for required fields
    has_required_fields = True
    error_messages = []
    
    # Source config validation
    if not source_config.get('source_system'):
        has_required_fields = False
        error_messages.append("Source system is required")
    
    # Table config validation
    tier1_meta = table_config.get('metadata', {}).get('tier1', {})
    tier2_meta = table_config.get('metadata', {}).get('tier2', {})
    
    if not tier1_meta.get('table'):
        has_required_fields = False
        error_messages.append("Tier 1 table name is required")
    
    if not tier2_meta.get('table'):
        has_required_fields = False
        error_messages.append("Tier 2 table name is required")
    
    if not table_config.get('columns'):
        has_required_fields = False
        error_messages.append("At least one column mapping is required")
    
    # Display errors if any
    if not has_required_fields:
        st.error("Please fix the following errors:")
        for msg in error_messages:
            st.warning(msg)
    
    # Combine all configurations
    combined_config = {
        'source_config': source_config,
        'validator_config': validator_config,
        'table_config': table_config
    }
    
    # Generate YAML
    yaml_config = yaml.dump(combined_config, default_flow_style=False, sort_keys=False)
    
    # Display YAML preview
    st.subheader("YAML Preview")
    st.code(yaml_config, language="yaml")
    
    # File name input
    source_system = source_config.get('source_system', '')
    tier2_table = table_config.get('metadata', {}).get('tier2', {}).get('table', '')
    
    default_filename = f"{source_system}_{tier2_table}.yml" if source_system and tier2_table else "pipeline_config.yml"
    filename = st.text_input("Configuration File Name", value=default_filename)
    
    # Output directory (from sidebar)
    output_dir = st.session_state.get('output_dir', "./pipeline_configs")
    
    # Save options
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("Download YAML"):
            st.download_button(
                label="Download Configuration",
                data=yaml_config,
                file_name=filename,
                mime="text/yaml"
            )
    
    with col2:
        if st.button("Save to File"):
            try:
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                
                file_path = create_yaml_file(combined_config, filename, output_dir)
                st.success(f"Configuration saved to {file_path}")
            except Exception as e:
                st.error(f"Error saving file: {str(e)}")

def main():
    """Main application entry point"""
    render_header()
    
    # Render sidebar and get selected section
    section, output_dir = render_sidebar()
    
    # Store output directory in session state
    st.session_state.output_dir = output_dir
    
    # Render the selected section
    if section == "Source Configuration":
        render_source_config()
    elif section == "Validator Configuration":
        render_validator_config()
    elif section == "Table Configuration":
        render_table_config()
    elif section == "Column Mappings":
        render_column_mappings()
    elif section == "Generate YAML":
        render_generate_yaml()

if __name__ == "__main__":
    main()