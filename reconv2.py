import streamlit as st
import pandas as pd
import json
import io
import zipfile
import re
import logging
from datetime import datetime, timedelta
from streamlit.components.v1 import html
from functools import lru_cache
import tempfile
import os
from openpyxl import Workbook
import unicodedata
import itertools
import uuid
import numpy as np
import plotly.express as px

# Set page config
st.set_page_config(page_title="Reconciliation Dashboard", layout="wide")

# --- UI Styling ---
st.markdown("""
<style>
    /* Target the 'Browse files' buttons for a uniform look */
    section[data-testid="stFileUploadDropzone"] button[data-baseweb="button"] {
        width: 120px !important;
    }
    /* Target the main 'Process Files' button */
    div[data-testid="stButton"] > button[data-baseweb="button"] {
        width: 100% !important;
    }
</style>
""", unsafe_allow_html=True)

# Set up detailed logging
log_stream = io.StringIO()
log_handler = logging.StreamHandler(log_stream)
log_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s'))
logger = logging.getLogger(__name__)
logger.handlers = [log_handler]
logger.setLevel(logging.INFO)

# Global lists and sets
audit_entries, wsr_error_entries, resolution_logs = [], [], []
matched_bank_refs, matched_settlement_nums = set(), set()

# Session state initialization
if "processed" not in st.session_state:
    st.session_state.processed = False
    st.session_state.recon_df, st.session_state.summary_df, st.session_state.wsr_recon_df = None, None, None
    st.session_state.audit_df, st.session_state.wsr_error_df = pd.DataFrame(), pd.DataFrame()
    st.session_state.missing_weeks_df = pd.DataFrame()
    st.session_state.coverage_summary_df = pd.DataFrame()
    st.session_state.no_data_stores = []
    st.session_state.log_content, st.session_state.error_message, st.session_state.excel_file = "", None, None
    st.session_state.diagnostic_df = pd.DataFrame()
    st.session_state.master_ar_df = pd.DataFrame()  # For audit reporting

# --- Constants ---
DATE_WINDOW_DAYS = 5
FINAL_PASS_DATE_WINDOW_DAYS = 3
TOLERANCE = 0.02
FINAL_PASS_TOLERANCE = 0.10
MIN_FEE_RATE = 0.015
MAX_FEE_RATE = 0.05
TYPICAL_AMEX_FEE_RATE = 0.0275

# --- Audit Report Functions ---
def create_wsr_audit_report(master_ar_df, diagnostic_df):
    """
    Create a comprehensive audit report showing AR counts by store and date
    """
    # Group by Store and WSR_Date to get AR counts
    ar_counts = master_ar_df.groupby(['Store', 'WSR_Date']).agg({
        'Amount': ['count', 'sum', 'mean', 'std'],
        'Channel': lambda x: len(x.unique()),
        'Card_Type': lambda x: len(x.unique()),
        'WSR_File': lambda x: '; '.join(x.unique())
    }).reset_index()
    
    # Flatten column names
    ar_counts.columns = ['Store', 'WSR_Date', 'AR_Count', 'Total_Amount', 
                         'Avg_Amount', 'Std_Amount', 'Unique_Channels', 
                         'Unique_Card_Types', 'WSR_Files']
    
    # Calculate week-over-week variance for each store
    ar_counts = ar_counts.sort_values(['Store', 'WSR_Date'])
    
    # Calculate statistics per store
    store_stats = []
    for store in ar_counts['Store'].unique():
        store_data = ar_counts[ar_counts['Store'] == store].copy()
        
        if len(store_data) > 1:
            # Calculate variance metrics
            ar_count_mean = store_data['AR_Count'].mean()
            ar_count_std = store_data['AR_Count'].std()
            ar_count_cv = (ar_count_std / ar_count_mean * 100) if ar_count_mean > 0 else 0
            
            # Calculate week-to-week changes
            store_data['AR_Count_Change'] = store_data['AR_Count'].diff()
            store_data['AR_Count_Pct_Change'] = store_data['AR_Count'].pct_change() * 100
            
            # Flag unusual variations (>20% change or >2 std deviations)
            store_data['Variance_Flag'] = np.where(
                (abs(store_data['AR_Count_Pct_Change']) > 20) | 
                (abs(store_data['AR_Count'] - ar_count_mean) > 2 * ar_count_std),
                '⚠️ HIGH VARIANCE', ''
            )
            
            # Add store-level statistics
            store_data['Store_AR_Mean'] = ar_count_mean
            store_data['Store_AR_Std'] = ar_count_std
            store_data['Store_CV'] = ar_count_cv
            
            store_stats.append(store_data)
    
    # Combine all store data
    if store_stats:
        audit_df = pd.concat(store_stats, ignore_index=True)
    else:
        audit_df = ar_counts.copy()
        audit_df['AR_Count_Change'] = 0
        audit_df['AR_Count_Pct_Change'] = 0
        audit_df['Variance_Flag'] = ''
        audit_df['Store_AR_Mean'] = audit_df['AR_Count']
        audit_df['Store_AR_Std'] = 0
        audit_df['Store_CV'] = 0
    
    # Create summary statistics
    summary_stats = audit_df.groupby('Store').agg({
        'AR_Count': ['mean', 'std', 'min', 'max'],
        'Total_Amount': ['mean', 'sum'],
        'Variance_Flag': lambda x: (x != '').sum()
    }).round(2)
    
    summary_stats.columns = ['AR_Count_Mean', 'AR_Count_Std', 'AR_Count_Min', 
                             'AR_Count_Max', 'Avg_Weekly_Amount', 'Total_Amount', 
                             'High_Variance_Weeks']
    
    # Add coefficient of variation
    summary_stats['CV_Percent'] = (summary_stats['AR_Count_Std'] / 
                                   summary_stats['AR_Count_Mean'] * 100).round(1)
    
    # Flag stores with high overall variance (CV > 15%)
    summary_stats['Store_Health'] = np.where(
        summary_stats['CV_Percent'] > 15,
        '🔴 High Variance',
        np.where(summary_stats['CV_Percent'] > 10,
                '🟡 Moderate Variance',
                '🟢 Consistent')
    )
    
    # Create detailed variance analysis for specific stores
    variance_analysis = audit_df[audit_df['Variance_Flag'] != ''][
        ['Store', 'WSR_Date', 'AR_Count', 'AR_Count_Change', 
         'AR_Count_Pct_Change', 'Store_AR_Mean', 'Variance_Flag']
    ].copy()
    
    # Add diagnostic information if available
    if diagnostic_df is not None and not diagnostic_df.empty:
        # Merge with diagnostic data for additional insights
        diag_summary = diagnostic_df.groupby('store').agg({
            'status': lambda x: (x != 'success').sum(),
            'issues': lambda x: '; '.join([str(i) for i in x if i])
        }).reset_index()
        diag_summary.columns = ['Store', 'Failed_Processes', 'Processing_Issues']
        
        # Convert Store to string for merging
        diag_summary['Store'] = diag_summary['Store'].astype(str)
        summary_stats = summary_stats.reset_index()
        summary_stats['Store'] = summary_stats['Store'].astype(str)
        summary_stats = summary_stats.merge(diag_summary, on='Store', how='left')
        summary_stats = summary_stats.fillna({'Failed_Processes': 0, 'Processing_Issues': ''})
    else:
        summary_stats = summary_stats.reset_index()
    
    return audit_df, summary_stats, variance_analysis

# UI Elements
st.title("Reconciliation Dashboard")
st.markdown("Upload files to perform reconciliation using bank statement processing.")
st.subheader("Select Reconciliation Month")
recon_month = st.date_input("Select Month (day will be ignored)", value=datetime(2024, 12, 1), format="YYYY/MM/DD")
recon_start, recon_end = pd.to_datetime(recon_month.replace(day=1)), pd.to_datetime((recon_month.replace(day=1) + timedelta(days=31)).replace(day=1) - timedelta(days=1))

st.subheader("Upload Input Files")
col1, col2 = st.columns(2)
with col1:
    st.markdown("**Required Files:**")
    merchant_file = st.file_uploader("Merchant Numbers (xlsx)", type=["xlsx"])
    wsr_zip_file = st.file_uploader("WSR Files (zip)", type=["zip"])
with col2:
    st.markdown("**Bank Processing Files:**")
    bank_file = st.file_uploader("Bank Statement (csv)", type=["csv"])
    settlement_file = st.file_uploader("Amex Settlements (csv)", type=["csv"])

# --- Functions ---
@lru_cache(maxsize=10000)
def extract_wsr_info(filename):
    match = re.match(r'(?:#)?(\d+)(?:[_\s]WSR)?[_\s]?(\d{2}-\d{2}-\d{2,4})?\.(xls|xlsx)', 
                     os.path.basename(filename), re.IGNORECASE)
    if match:
        store_num = match.group(1).lstrip('0')
        date_str = match.group(2) if len(match.groups()) > 1 and match.group(2) else None
        return store_num, date_str
    return None, None

def process_wsr_file(wsr_file, store_number, processed_files, file_content, recon_year):
    file_base = os.path.basename(wsr_file).lower()
    diagnostics = {'file': file_base, 'store': store_number, 'status': 'processing', 'issues': []}
    
    if file_base in processed_files: 
        diagnostics['status'] = 'skipped_duplicate'
        return None, diagnostics
    
    processed_files.add(file_base)
    
    try:
        engine = 'openpyxl' if wsr_file.lower().endswith('.xlsx') else 'xlrd'
        
        try:
            all_sheets = pd.ExcelFile(io.BytesIO(file_content), engine=engine).sheet_names
            diagnostics['available_sheets'] = all_sheets
            logger.info(f"Available sheets in {wsr_file}: {all_sheets}")
            
            weekly_sheet = None
            for sheet in all_sheets:
                if 'weekly' in sheet.lower() and 'sales' in sheet.lower():
                    weekly_sheet = sheet
                    break
            
            if not weekly_sheet and all_sheets:
                weekly_sheet = all_sheets[0]
                diagnostics['issues'].append(f"No 'Weekly Sales' sheet found, using '{weekly_sheet}'")
                logger.warning(f"No 'Weekly Sales' sheet in {wsr_file}, using first sheet: {weekly_sheet}")
            
            if not weekly_sheet:
                diagnostics['status'] = 'failed_no_sheets'
                diagnostics['issues'].append("No sheets found in file")
                return None, diagnostics
                
            wsr_data = pd.read_excel(io.BytesIO(file_content), sheet_name=weekly_sheet, header=None, engine=engine)
                
        except Exception as e:
            diagnostics['status'] = 'failed_read_error'
            diagnostics['issues'].append(f"Error reading file: {str(e)}")
            logger.error(f"Error reading {wsr_file}: {e}")
            return None, diagnostics
        
        try:
            file_store_num = str(wsr_data.iloc[3, 2]).lstrip('0').replace('#', '').strip()
            diagnostics['file_store_num'] = file_store_num
            
            if file_store_num != store_number:
                alternative_locations = [(3, 1), (3, 3), (2, 2), (4, 2), (0, 0), (0, 1), (1, 0), (1, 1)]
                
                found_match = False
                for row, col in alternative_locations:
                    try:
                        if row < wsr_data.shape[0] and col < wsr_data.shape[1]:
                            alt_store = str(wsr_data.iloc[row, col]).lstrip('0').replace('#', '').strip()
                            if alt_store == store_number:
                                file_store_num = alt_store
                                found_match = True
                                diagnostics['issues'].append(f"Store number found at alternative location ({row},{col})")
                                logger.info(f"Found store number at ({row},{col}) in {wsr_file}")
                                break
                    except:
                        continue
                
                if not found_match:
                    diagnostics['issues'].append(f"Store mismatch: file has '{file_store_num}', expected '{store_number}'. Using filename store.")
                    logger.warning(f"Store number mismatch in {wsr_file}: expected {store_number}, found {file_store_num}. Continuing with filename store.")
        except Exception as e:
            diagnostics['issues'].append(f"Could not extract store number: {str(e)}")
            logger.warning(f"Could not extract store number from {wsr_file}: {e}")
        
        dates, date_cols = [], []
        # Extend search range to ensure we get all dates AND their PM columns
        max_col_search = min(20, wsr_data.shape[1])  
        for col_idx in range(3, max_col_search):
            try:
                if 8 < wsr_data.shape[0]:
                    date_str = wsr_data.iloc[8, col_idx]
                    if pd.notna(date_str):
                        date_str = str(date_str).strip()
                        if not re.search(r'[\s/,-]\d{2,4}\s*$', date_str):
                            date_str = f"{date_str}/{recon_year}"
                        
                        for fmt in ['%m-%d-%Y', '%m/%d/%Y', '%m-%d-%y', '%m/%d/%y', '%Y-%m-%d']:
                            try:
                                date = pd.to_datetime(date_str, format=fmt)
                                if not pd.isna(date):
                                    date_formatted = date.strftime('%Y-%m-%d')
                                    if date_formatted not in dates:
                                        dates.append(date_formatted)
                                        date_cols.append(col_idx)
                                    break
                            except:
                                continue
            except Exception as e:
                continue
        
        if not dates:
            diagnostics['status'] = 'failed_no_dates'
            diagnostics['issues'].append("No valid dates found in expected location")
            logger.warning(f"No dates found in {wsr_file}")
            return None, diagnostics
        
        diagnostics['dates_found'] = dates
        
        # Create shift map - ensure we can access PM columns for all dates
        shift_map = {}
        for date, col in zip(dates, date_cols):
            shift_map[col] = {'date': date, 'shift': 'AM'}
            # Ensure PM column (col+1) is within bounds
            if col + 1 < wsr_data.shape[1]:
                shift_map[col + 1] = {'date': date, 'shift': 'PM'}
            else:
                diagnostics['issues'].append(f"PM column for date {date} would be out of bounds")
                logger.warning(f"PM column for date {date} in {wsr_file} would be out of bounds (col {col+1} >= {wsr_data.shape[1]})")
        
        ar_data = []
        channel_summary = {'INSHOP': 0, 'MOTO': 0, 'ONLINE': 0, 'UNKNOWN': 0}  # Track channels
        card_summary = {'Visa': 0, 'MC': 0, 'Discover': 0, 'Amex': 0}  # Track cards
        
        for row_idx in range(wsr_data.shape[0]):
            try:
                label_raw = wsr_data.iloc[row_idx, 0]
                if not isinstance(label_raw, str): 
                    continue
                    
                label = unicodedata.normalize("NFKD", label_raw).strip()
                
                # Check if this is an A/R row
                is_ar_row = False
                if ("A/R" in label or "AR" in label) and ("Due" in label or "DUE" in label):
                    if "CC" in label.upper():
                        is_ar_row = True
                    # Also check for variations
                    elif any(term in label.upper() for term in ['CREDIT', 'CARD']):
                        is_ar_row = True
                        logger.info(f"Alternative AR pattern found: '{label}'")
                
                if is_ar_row:
                    # Try multiple channel detection patterns
                    channel_match = re.search(r'\((InShop|MOTO|ONLINE|Inshop|Moto|Online|INSHOP)\)', label, re.IGNORECASE)
                    current_channel = None
                    
                    if channel_match:
                        current_channel = channel_match.group(1).upper()
                    else:
                        # Try alternative patterns for channel detection
                        patterns = [
                            (r'INSHOP', 'INSHOP'),
                            (r'IN\s*SHOP', 'INSHOP'),
                            (r'IN-SHOP', 'INSHOP'),
                            (r'MOTO', 'MOTO'),
                            (r'ONLINE', 'ONLINE'),
                            (r'ON\s*LINE', 'ONLINE'),
                            (r'ON-LINE', 'ONLINE')
                        ]
                        
                        for pattern, channel_name in patterns:
                            if re.search(pattern, label.upper()):
                                current_channel = channel_name
                                break
                        
                        if not current_channel:
                            channel_summary['UNKNOWN'] += 1
                            continue
                    
                    channel_summary[current_channel] += 1
                    
                    # Enhanced card type detection
                    card_patterns = [
                        (r'\b(Visa|VISA|visa)\b', 'Visa'),
                        (r'\b(Amex|AMEX|amex|American\s*Express)\b', 'Amex'),
                        (r'\b(MC|mc|MasterCard|Mastercard|Master\s*Card|MASTERCARD)\b', 'MC'),
                        (r'\b(Discover|DISCOVER|discover)\b', 'Discover'),
                        # Additional patterns for variations
                        (r'\b(V|v)\b', 'Visa'),  # Sometimes just 'V' for Visa
                        (r'\b(M|m)\b', 'MC'),     # Sometimes just 'M' for MasterCard
                        (r'\b(D|d)\b', 'Discover'), # Sometimes just 'D' for Discover
                        (r'\b(A|a)\b', 'Amex'),     # Sometimes just 'A' for Amex
                    ]
                    
                    card_type = None
                    for pattern, card_name in card_patterns:
                        if re.search(pattern, label):
                            card_type = card_name
                            break
                    
                    if not card_type:
                        continue
                    
                    card_summary[card_type] += 1
                    
                    # Extract amounts for each date/shift
                    for col, shift in shift_map.items():
                        if col < wsr_data.shape[1]:
                            amount = wsr_data.iloc[row_idx, col]
                            if pd.notna(amount):
                                try:
                                    amount_float = float(amount)
                                    if amount_float != 0:
                                        ar_data.append({
                                            'Store': store_number,
                                            'Date': shift['date'],
                                            'Channel': current_channel,
                                            'Card_Type': card_type,
                                            'Amount': amount_float,
                                            'WSR_File': file_base,
                                            'WSR_Label': label
                                        })
                                        
                                except (ValueError, TypeError) as e:
                                    diagnostics['issues'].append(f"Could not convert amount '{amount}': {e}")
                                    continue
                        
            except Exception as e:
                continue
        
        if ar_data:
            diagnostics['status'] = 'success'
            diagnostics['ar_records'] = len(ar_data)
            diagnostics['channel_breakdown'] = channel_summary
            diagnostics['card_breakdown'] = card_summary
            return pd.DataFrame(ar_data), diagnostics
        else:
            diagnostics['status'] = 'failed_no_ar_data'
            diagnostics['issues'].append("No A/R data extracted")
            return None, diagnostics
            
    except Exception as e:
        diagnostics['status'] = 'failed_exception'
        diagnostics['issues'].append(f"Unexpected error: {str(e)}")
        logger.error(f"Error processing WSR file {wsr_file}: {e}")
        return None, diagnostics

def load_merchant_numbers(file_content):
    try:
        try:
            sheets = pd.read_excel(io.BytesIO(file_content), sheet_name=['Merchant Key', 'Amex'], dtype=str)
            general_sheet_name = 'Merchant Key'
        except ValueError:
            logger.warning("Worksheet 'Merchant Key' not found. Trying 'Merchant Numbers'.")
            sheets = pd.read_excel(io.BytesIO(file_content), sheet_name=['Merchant Numbers', 'Amex'], dtype=str)
            general_sheet_name = 'Merchant Numbers'

        general, amex = sheets[general_sheet_name], sheets['Amex']
        general['Store'], amex['Store'] = general['Store'].str.lstrip('0'), amex['Store'].str.lstrip('0')
        
        # Note: We're still loading designations but all stores will use Statement processing
        general['Designation'], amex['Designation'] = general['Designation'].str.strip(), amex['Designation'].str.strip()
        designation_map = pd.concat([general[['Store', 'Designation']], amex[['Store', 'Designation']]]).dropna(subset=['Store', 'Designation']).drop_duplicates('Store')
        store_to_designation = dict(zip(designation_map['Store'], designation_map['Designation']))
        
        # Log the designations found (for reference, but all will process as Statement)
        logger.info(f"Store designations loaded (all will use Statement processing): {store_to_designation}")
        
        amex['Merchant Number'], general['Merchant Number'] = amex['Merchant Number'].str.strip(), general['Merchant Number'].str.strip().str.zfill(13)
        amex_map = dict(zip(amex.dropna(subset=['Merchant Number'])['Merchant Number'], amex['Store']))
        general_map = dict(zip(general.dropna(subset=['Merchant Number'])['Merchant Number'], general['Store']))
        return store_to_designation, general, amex, amex_map, general_map
    except Exception as e:
        logger.error(f"Error loading Merchant Numbers: {e}")
        raise

def load_bank_statement(file_content):
    """
    Load bank statement with flexible column name handling
    """
    try:
        # First, read without parsing dates to see what columns we have
        df = pd.read_csv(io.StringIO(file_content.decode('utf-8')), dtype=str)
        
        # Log the columns found
        logger.info(f"Bank statement columns found: {df.columns.tolist()}")
        
        # Find the date column - try different common names
        date_col = None
        possible_date_cols = ['As Of', 'Date', 'Post Date', 'Posted Date', 'Transaction Date', 
                              'As of', 'as of', 'DATE', 'date']
        for col in possible_date_cols:
            if col in df.columns:
                date_col = col
                break
        
        if not date_col:
            # Try to find any column with 'date' in the name
            for col in df.columns:
                if 'date' in col.lower():
                    date_col = col
                    break
        
        if not date_col:
            raise ValueError(f"Could not find date column. Available columns: {df.columns.tolist()}")
        
        # Parse the date column
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        # Rename to standard name for consistency
        df = df.rename(columns={date_col: 'As Of'})
        
        # Find the data type column - try different common names
        data_type_col = None
        possible_type_cols = ['Data Type', 'Transaction Type', 'Type', 'Trans Type', 
                             'data type', 'DATA TYPE']
        for col in possible_type_cols:
            if col in df.columns:
                data_type_col = col
                break
        
        # If no data type column, check if there's a Debit/Credit column structure
        if not data_type_col:
            if 'Credit' in df.columns or 'Credits' in df.columns:
                # This is likely a different format where credits/debits are separate columns
                logger.info("Bank statement appears to use separate Credit/Debit columns")
                # Keep only rows with credit amounts
                if 'Credit' in df.columns:
                    df = df[df['Credit'].notna()]
                    df['Amount'] = pd.to_numeric(df['Credit'], errors='coerce')
                elif 'Credits' in df.columns:
                    df = df[df['Credits'].notna()]
                    df['Amount'] = pd.to_numeric(df['Credits'], errors='coerce')
            else:
                # No data type column found, assume all are credits or look for amount > 0
                logger.warning("No Data Type column found, will filter by positive amounts")
                if 'Amount' in df.columns:
                    df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
                    df = df[df['Amount'] > 0]
        else:
            # Filter for credits using the data type column
            df = df[df[data_type_col].str.contains('Credit', case=False, na=False)]
            df = df.rename(columns={data_type_col: 'Data Type'})
        
        # Ensure Amount column exists and is numeric
        if 'Amount' not in df.columns:
            # Try to find amount column
            possible_amount_cols = ['Amount', 'Credit', 'Credits', 'Credit Amount', 'Deposit', 
                                   'amount', 'AMOUNT']
            for col in possible_amount_cols:
                if col in df.columns:
                    df['Amount'] = pd.to_numeric(df[col], errors='coerce')
                    break
        else:
            df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')
        
        # Ensure Bank Reference column exists
        if 'Bank Reference' not in df.columns:
            # Try to find reference column
            possible_ref_cols = ['Bank Reference', 'Reference', 'Reference Number', 'Ref', 
                                'Transaction ID', 'Trans ID', 'ID']
            for col in possible_ref_cols:
                if col in df.columns:
                    df['Bank Reference'] = df[col].astype(str)
                    break
            
            # If still no reference, create one from index
            if 'Bank Reference' not in df.columns:
                df['Bank Reference'] = df.index.astype(str)
        
        # Ensure Text/Description column exists
        if 'Text' not in df.columns:
            # Try to find description column
            possible_text_cols = ['Text', 'Description', 'Memo', 'Details', 'Transaction Description',
                                 'text', 'TEXT', 'Payee', 'Name']
            for col in possible_text_cols:
                if col in df.columns:
                    df['Text'] = df[col].astype(str)
                    break
            
            # If still no text column, create empty one
            if 'Text' not in df.columns:
                df['Text'] = ''
        
        # Remove duplicates
        df = df.drop_duplicates(['Bank Reference', 'Amount'])
        
        # UPDATED: Only keep Fifth Third Bank and Amex merchant deposits
        # Pattern 1: Fifth Third Bank (5/3 BANKCARD)
        # Pattern 2: American Express settlements
        merchant_patterns = [
            r'5/3\s+BANKCARD',  # Fifth Third Bank card deposits
            r'AMERICAN\s+EXPRESS.*JIMMY\s+JOHNS?'  # Amex settlements
        ]
        
        # Create combined pattern
        pattern = '|'.join(merchant_patterns)
        
        # Keep ONLY transactions matching our merchant patterns
        df = df[df['Text'].str.contains(pattern, case=False, na=False, regex=True)]
        
        logger.info(f"Successfully loaded {len(df)} merchant deposit transactions")
        
        return df
        
    except Exception as e:
        logger.error(f"Error loading bank statement: {e}")
        logger.error(f"Please ensure your CSV has columns for: Date, Amount/Credit, and Description/Text")
        raise

def load_amex_settlement(file_content):
    try:
        lines = file_content.decode('utf-8').splitlines()
        skip = next((i for i, line in enumerate(lines) if 'Settlement Date' in line), -1)
        if skip == -1: 
            raise ValueError("Could not find 'Settlement Date' header.")
        df = pd.read_csv(io.StringIO('\n'.join(lines[skip:])), engine='python')
        df.columns = [c.strip() for c in df.columns]
        numeric = ['Total Charges', 'Settlement Amount', 'Discount Amount', 'Fees & Incentives', 'Chargebacks', 'Adjustments', 'Held Funds']
        for col in numeric:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col].astype(str).str.replace(r'[$,()]', '', regex=True).replace('-', '0'), errors='coerce').fillna(0)
        df['Settlement_Date'] = pd.to_datetime(df['Settlement Date'], errors='coerce')
        for col in ['Chargebacks', 'Adjustments']:
            if col in df.columns: 
                df[col] = -abs(df[col])
        df['Total_Fees'] = df[numeric[2:]].sum(axis=1)
        df = df.rename(columns={'Payee Merchant ID': 'Merchant_Number', 'Settlement Amount': 'Settlement_Amount', 'Settlement Number': 'Settlement_Number'})
        df['Merchant_Number'] = df['Merchant_Number'].astype(str).str.strip()
        return df
    except Exception as e:
        logger.error(f"Error loading Amex settlement: {e}")
        raise

def extract_merchant_and_store(text, amex_map, general_map, amex_channel_map):
    if not isinstance(text, str): 
        return None, None, None, None
    text_upper = text.upper()
    if 'AMERICAN EXPRESS' in text_upper:
        m = re.search(r'INDN\s*:\s*(?:JIMMY\s*JOHNS\s*)?(\d{9,10})', text, re.IGNORECASE)
        if m:
            merch_num = m.group(1)
            store_num = amex_map.get(merch_num)
            channel = 'ALL'
            return merch_num, store_num, 'Amex', channel
    else:
        m = re.search(r'ID:(\d{12,13})', text)
        if m:
            merch_num, store_num = m.group(1), general_map.get(m.group(1))
            channel_match = re.search(r'-(EC|M|MO)\s*(?:CO\s*ID:|$)', text_upper)
            channel = {'EC': 'ONLINE', 'M': 'INSHOP', 'MO': 'MOTO'}.get(channel_match.group(1), 'UNKNOWN') if channel_match else 'UNKNOWN'
            return merch_num, store_num, 'Non-Amex', channel
    return None, None, None, None

def create_ar_totals(ar_df, store_num, general_merchants, amex_merchants):
    logger.info(f"Creating AR totals for store {store_num}")
    if ar_df.empty: 
        return pd.DataFrame()
    
    def agg_labels(x): 
        return '; '.join(x.astype(str).unique())
    agg_dict = {'Amount': 'sum', 'WSR_File': agg_labels, 'WSR_Label': agg_labels}
    ar_df = ar_df.copy()  
    ar_df['WSR_Date'] = pd.to_datetime(ar_df['WSR_Date'])
    ar_groups, amex_df, non_amex_df = [], ar_df[ar_df['Card_Type'] == 'Amex'].copy(), ar_df[ar_df['Card_Type'] != 'Amex'].copy()
    
    if not non_amex_df.empty:
        non_amex_grouped = non_amex_df.groupby(['WSR_Date', 'Channel']).agg(agg_dict).reset_index()
        
        for _, row in non_amex_grouped.iterrows():
            merch_row = general_merchants[(general_merchants['Store'] == store_num) & (general_merchants['Channel'] == row['Channel'])]
            
            ar_groups.append({'WSR_Date': row['WSR_Date'], 'Store': store_num, 'Channel': row['Channel'], 'Card_Type': 'Non-Amex', 'Merchant_Type': 'Non-Amex', 'Merchant_Number': merch_row.iloc[0]['Merchant Number'] if not merch_row.empty else None, 'AR_Amount': row['Amount'], 'WSR_File': row['WSR_File'], 'WSR_Label': row['WSR_Label']})
    
    if not amex_df.empty:
        all_channel_merch_row = amex_merchants[(amex_merchants['Store'] == store_num) & (amex_merchants['Channel'] == 'ALL')]
        if not all_channel_merch_row.empty:
            logger.info(f"Store {store_num}: 'ALL' Amex channel configured. Summing all Amex A/R to daily totals.")
            daily_amex_sum = amex_df.groupby('WSR_Date')['Amount'].sum().reset_index()
            for _, sum_row in daily_amex_sum.iterrows():
                wsr_date, day_df = sum_row['WSR_Date'], amex_df[amex_df['WSR_Date'] == sum_row['WSR_Date']]
                wsr_files, wsr_labels = '; '.join(day_df['WSR_File'].unique()), '; '.join(day_df['WSR_Label'].unique())
                ar_groups.append({'WSR_Date': wsr_date, 'Store': store_num, 'Channel': 'ALL', 'Card_Type': 'Amex', 'Merchant_Type': 'Amex', 'Merchant_Number': all_channel_merch_row.iloc[0]['Merchant Number'], 'AR_Amount': sum_row['Amount'], 'WSR_File': wsr_files, 'WSR_Label': wsr_labels})
        else:
            logger.info(f"Store {store_num}: No 'ALL' Amex channel. Processing Amex channels individually.")
            amex_grouped = amex_df.groupby(['WSR_Date', 'Channel']).agg(agg_dict).reset_index()
            for _, row in amex_grouped.iterrows():
                merch_row = amex_merchants[(amex_merchants['Store'] == store_num) & (amex_merchants['Channel'] == row['Channel'])]
                if not merch_row.empty:
                    ar_groups.append({'WSR_Date': row['WSR_Date'], 'Store': store_num, 'Channel': row['Channel'], 'Card_Type': 'Amex', 'Merchant_Type': 'Amex', 'Merchant_Number': merch_row.iloc[0]['Merchant Number'], 'AR_Amount': row['Amount'], 'WSR_File': row['WSR_File'], 'WSR_Label': row['WSR_Label']})
    
    return pd.DataFrame(ar_groups)

def reconcile_store_statement(daily_ar, bank_df_store, settle_df, store_num, matched_bank_refs, matched_settle_nums):
    recon, temp_b, temp_s, matched_ar = [], set(), set(), set()
    
    logger.info(f"=== RECONCILIATION START for Store {store_num} ===")
    logger.info(f"Total AR records to reconcile: {len(daily_ar)}")
    
    amex_ar = daily_ar[daily_ar['Merchant_Type'] == 'Amex'].copy()
    non_amex_ar = daily_ar[daily_ar['Merchant_Type'] != 'Amex'].copy()
    reversal_cols = ['Chargebacks', 'Adjustments']

    for idx, ar_row in amex_ar.iterrows():
        if idx in matched_ar: continue
        candidate_settles = settle_df[(settle_df['Merchant_Number'] == ar_row['Merchant_Number']) & (settle_df['Settlement_Date'].between(ar_row['WSR_Date'], ar_row['WSR_Date'] + timedelta(days=DATE_WINDOW_DAYS*2)))]
        for _, settle_row in candidate_settles.iterrows():
            for col in reversal_cols:
                if col in settle_row and np.isclose(settle_row[col], -ar_row['AR_Amount']):
                    logger.info(f"Found reversal for AR {idx} (Amount: {ar_row['AR_Amount']}) in settlement row. Status set to Reversed.")
                    ar_dict = ar_row.to_dict()
                    ar_dict.update({'Bank_Date': settle_row['Settlement_Date'], 'Bank_Amount': settle_row[col], 'Settlement_Number': settle_row['Settlement_Number'], 'Status': 'Reversed', 'Source': 'Statement', 'Bank_Description': f"Full Reversal via {col}"})
                    recon.append(ar_dict)
                    matched_ar.add(idx)
                    temp_s.add(settle_row['Settlement_Number'])
                    break
            if idx in matched_ar: break

    for idx, ar_row in amex_ar[~amex_ar.index.isin(matched_ar)].iterrows():
        settles = settle_df[(settle_df['Settlement_Date'].between(ar_row['WSR_Date'] - timedelta(days=DATE_WINDOW_DAYS), ar_row['WSR_Date'] + timedelta(days=DATE_WINDOW_DAYS))) & (settle_df['Merchant_Number'] == ar_row['Merchant_Number'])]
        exact_settle = settles[abs(settles['Total Charges'] - ar_row['AR_Amount']) <= TOLERANCE]

        if not exact_settle.empty:
            settle_row = exact_settle.iloc[0]
            banks = bank_df_store[~bank_df_store['Bank Reference'].isin(temp_b)]
            candidate_banks = banks[(banks['As Of'].between(settle_row['Settlement_Date'] - timedelta(days=DATE_WINDOW_DAYS), settle_row['Settlement_Date'] + timedelta(days=DATE_WINDOW_DAYS))) & (banks['Merchant_Number'] == ar_row['Merchant_Number'])]
            
            net_bank_match = candidate_banks[abs(candidate_banks['Amount'] - settle_row['Settlement_Amount']) <= TOLERANCE]
            gross_bank_match = candidate_banks[abs(candidate_banks['Amount'] - settle_row['Total Charges']) <= TOLERANCE]
            exact_bank = net_bank_match if not net_bank_match.empty else gross_bank_match
            
            if not exact_bank.empty:
                bank_row = exact_bank.iloc[0]
                logger.info(f"[[[DEPOSIT CLAIMED]]] Bank Ref: {bank_row['Bank Reference']} (Amount: {bank_row['Amount']:.2f}) was claimed by A/R Index: {idx} (AR Amount: {ar_row['AR_Amount']:.2f}) via Settlement.")
                ar_dict = ar_row.to_dict()
                ar_dict.update({'Bank_Date': bank_row['As Of'], 'Bank_Amount': bank_row['Amount'], 'Settlement_Amount': settle_row['Settlement_Amount'], 'Settlement_Number': settle_row['Settlement_Number'], 'Status': 'Matched with Settlement', 'Source': 'Statement', 'Bank_Reference': bank_row['Bank Reference'], 'Bank_Description': bank_row['Text']})
                recon.append(ar_dict)
                matched_ar.add(idx)
                temp_b.add(bank_row['Bank Reference'])
                temp_s.add(settle_row['Settlement_Number'])

    for idx, ar_row in amex_ar[~amex_ar.index.isin(matched_ar)].iterrows():
        available_banks = bank_df_store[~bank_df_store['Bank Reference'].isin(temp_b)]
        candidate_banks = available_banks[(available_banks['As Of'].between(ar_row['WSR_Date'] - timedelta(days=DATE_WINDOW_DAYS), ar_row['WSR_Date'] + timedelta(days=DATE_WINDOW_DAYS))) & (available_banks['Merchant_Number'] == ar_row['Merchant_Number'])]
        
        plausible_deposits = candidate_banks[(ar_row['AR_Amount'] > candidate_banks['Amount']) & (((ar_row['AR_Amount'] - candidate_banks['Amount']) / ar_row['AR_Amount']).between(MIN_FEE_RATE, MAX_FEE_RATE))].copy()
        
        if not plausible_deposits.empty:
            plausible_deposits['Fee_Diff'] = abs(((ar_row['AR_Amount'] - plausible_deposits['Amount']) / ar_row['AR_Amount']) - TYPICAL_AMEX_FEE_RATE)
            best_match = plausible_deposits.loc[plausible_deposits['Fee_Diff'].idxmin()]
            
            logger.info(f"[[[DEPOSIT CLAIMED (Fallback)]]] Bank Ref: {best_match['Bank Reference']} (Amount: {best_match['Amount']:.2f}) was claimed by A/R Index: {idx} (AR Amount: {ar_row['AR_Amount']:.2f}) via Net-of-Fees match.")
            ar_dict = ar_row.to_dict()
            ar_dict.update({'Bank_Date': best_match['As Of'], 'Bank_Amount': best_match['Amount'], 'Status': 'Matched (Net of Fees)', 'Source': 'Statement', 'Bank_Reference': best_match['Bank Reference'], 'Bank_Description': best_match['Text']})
            recon.append(ar_dict)
            matched_ar.add(idx)
            temp_b.add(best_match['Bank Reference'])

    remaining_ar = pd.concat([non_amex_ar, amex_ar[~amex_ar.index.isin(matched_ar)]])
    
    logger.info(f"Processing remaining AR records (Non-Amex + unmatched Amex): {len(remaining_ar)} records")
    
    for idx, ar_row in remaining_ar.iterrows():
        available_banks = bank_df_store[~bank_df_store['Bank Reference'].isin(temp_b)]
        matches = available_banks[(available_banks['As Of'].between(ar_row['WSR_Date'] - timedelta(days=DATE_WINDOW_DAYS), ar_row['WSR_Date'] + timedelta(days=DATE_WINDOW_DAYS))) & (available_banks['Merchant_Number'] == ar_row['Merchant_Number'])]
        
        exact = matches[abs(matches['Amount'] - ar_row['AR_Amount']) <= TOLERANCE]
        if not exact.empty:
            bank = exact.iloc[0]
            logger.info(f"[[[DEPOSIT CLAIMED]]] Bank Ref: {bank['Bank Reference']} (Amount: {bank['Amount']:.2f}) was claimed by A/R Index: {idx} (AR Amount: {ar_row['AR_Amount']:.2f}) via Exact Match.")
            ar_dict = ar_row.to_dict()
            ar_dict.update({'Bank_Date': bank['As Of'], 'Bank_Amount': bank['Amount'], 'Status': 'Matched (Exact)', 'Source': 'Statement', 'Bank_Reference': bank['Bank Reference'], 'Bank_Description': bank['Text']})
            recon.append(ar_dict)
            matched_ar.add(idx)
            temp_b.add(bank['Bank Reference'])
                    
    logger.info(f"After Non-Amex/remaining Amex matching: {len(matched_ar)} total matched")

    for idx, ar_row in amex_ar[~amex_ar.index.isin(matched_ar)].iterrows():
        available_settles = settle_df[~settle_df['Settlement_Number'].isin(temp_s)]
        settles = available_settles[(available_settles['Settlement_Date'].between(ar_row['WSR_Date'] - timedelta(days=DATE_WINDOW_DAYS), ar_row['WSR_Date'] + timedelta(days=DATE_WINDOW_DAYS))) & (available_settles['Merchant_Number'] == ar_row['Merchant_Number'])]
        exact_settle = settles[abs(settles['Total Charges'] - ar_row['AR_Amount']) <= TOLERANCE]
        if not exact_settle.empty:
            settle = exact_settle.iloc[0]
            ar_dict = ar_row.to_dict()
            ar_dict.update({'Settlement_Amount': settle['Settlement_Amount'], 'Settlement_Number': settle['Settlement_Number'], 'Status': 'Settlement Match, No Bank', 'Source': 'Statement'})
            recon.append(ar_dict)
            matched_ar.add(idx)
            temp_s.add(settle['Settlement_Number'])
            
    # FIXED: Check if ALL channel record exists before skipping Non-Amex records
    logger.info(f"Adding unmatched AR records: {len(daily_ar[~daily_ar.index.isin(matched_ar)])} records")
    
    for idx in daily_ar[~daily_ar.index.isin(matched_ar)].index:
        ar = daily_ar.loc[idx]
        # FIXED LOGIC: Only skip Amex records that are part of an ALL channel sum
        is_part_of_sum = (
            ar['Channel'] != 'ALL' and 
            ar['Card_Type'] == 'Amex' and  # Only check Amex records
            any(r['WSR_Date'] == ar['WSR_Date'] and r['Channel'] == 'ALL' and r['Card_Type'] == 'Amex' for r in recon)
        )
        
        if not is_part_of_sum:
            ar_dict = ar.to_dict()
            ar_dict.update({'Status': 'No Bank Match', 'Source': 'Statement'})
            recon.append(ar_dict)
    
    logger.info(f"=== RECONCILIATION END for Store {store_num} ===")
    logger.info(f"Total recon records: {len(recon)}")
    
    # Add missing columns if they don't exist
    recon_df = pd.DataFrame(recon)
    if 'Bank_Reference' not in recon_df.columns:
        recon_df['Bank_Reference'] = None
    if 'Bank_Description' not in recon_df.columns:
        recon_df['Bank_Description'] = None
    if 'Settlement_Amount' not in recon_df.columns:
        recon_df['Settlement_Amount'] = np.nan
    
    return recon_df, temp_b, temp_s

def final_cleanup_pass(wsr_df, unmatched_source_df):
    logger.info("Starting final cleanup pass...")
    unmatched_ar_all = wsr_df[wsr_df['Status'].str.contains("No .* Match|Settlement Match, No Bank", na=False)].copy()
    protected_ar_indices = set(unmatched_ar_all[unmatched_ar_all['Status'] == 'Settlement Match, No Bank'].index)
    unmatched_ar = unmatched_ar_all[~unmatched_ar_all.index.isin(protected_ar_indices)]
    if unmatched_ar.empty or unmatched_source_df.empty: 
        return {}, unmatched_source_df
    updated_indices = {}
    all_possible_matches = []
    for source_idx, deposit_row in unmatched_source_df.iterrows():
        store, source, merch_num = deposit_row.get('Store'), deposit_row.get('Source'), deposit_row.get('Merchant_Number')
        candidate_ars = unmatched_ar[(unmatched_ar['Store'] == store) & (unmatched_ar['Source'] == source) & (unmatched_ar['Merchant_Number'] == merch_num if source == 'Statement' else True) & (unmatched_ar['WSR_Date'].between(pd.to_datetime(deposit_row['Bank_Date']) - timedelta(days=DATE_WINDOW_DAYS), pd.to_datetime(deposit_row['Bank_Date']) + timedelta(days=DATE_WINDOW_DAYS)))]
        if candidate_ars.empty: 
            continue
        for r in range(1, min(len(candidate_ars), 8) + 1):
            for combo_indices in itertools.combinations(candidate_ars.index, r):
                combo_sum, diff = candidate_ars.loc[list(combo_indices), 'AR_Amount'].sum(), abs(candidate_ars.loc[list(combo_indices), 'AR_Amount'].sum() - deposit_row['Bank_Amount'])
                all_possible_matches.append((diff, list(combo_indices), source_idx))
    all_possible_matches.sort(key=lambda x: x[0])
    ar_used, source_used = set(), set()
    for diff, ar_indices, source_idx in all_possible_matches:
        if source_idx in source_used or any(idx in ar_used for idx in ar_indices): 
            continue
        deposit_row = unmatched_source_df.loc[source_idx]
        current_tolerance = max(TOLERANCE, deposit_row['Bank_Amount'] * FINAL_PASS_TOLERANCE)
        if diff <= current_tolerance:
            status = 'Matched (by Sum)' if len(ar_indices) > 1 else 'Matched (Best Fit)'
            if diff > TOLERANCE: 
                status = 'Matched (Last Resort)'
            logger.info(f"Cleanup Match: Deposit {deposit_row['Bank_Amount']:.2f} matched with WSRs {ar_indices} ({status}). Diff: {diff:.2f}")
            match_data = deposit_row.to_dict()
            if len(ar_indices) > 1: 
                match_data['Match_Group_ID'] = f"SUM-{uuid.uuid4()}"
            for idx in ar_indices: 
                updated_indices[idx] = (match_data, status)
                ar_used.add(idx)
            source_used.add(source_idx)
    return updated_indices, unmatched_source_df[~unmatched_source_df.index.isin(source_used)]

def find_two_by_two_matches(unmatched_ar, unmatched_source):
    logger.info("Starting Final Cleanup Pass 2 (2-to-2)...")
    if unmatched_ar.empty or unmatched_source.empty or len(unmatched_ar) < 2 or len(unmatched_source) < 2: 
        return {}, set(), set()
    updates, used_ar, used_source, groups = {}, set(), set(), []
    statement_ars, statement_deps = unmatched_ar[unmatched_ar['Source'] == 'Statement'], unmatched_source[unmatched_source['Source'] == 'Statement']
    if not statement_ars.empty and not statement_deps.empty:
        for key, ar_group in statement_ars.groupby(['Store', 'Channel', 'Merchant_Type']):
            dep_group = statement_deps[(statement_deps['Store'] == key[0]) & (statement_deps['Channel'] == key[1]) & (statement_deps['Merchant_Type'] == key[2])]
            if len(ar_group) >= 2 and len(dep_group) >= 2: 
                groups.append((ar_group, dep_group))
    
    for ar_group, source_group in groups:
        ar_pairs, source_pairs = list(itertools.combinations(ar_group.index, 2)), list(itertools.combinations(source_group.index, 2))
        for ar_idx1, ar_idx2 in ar_pairs:
            if ar_idx1 in used_ar or ar_idx2 in used_ar: 
                continue
            ar1, ar2 = ar_group.loc[ar_idx1], ar_group.loc[ar_idx2]
            ar_sum = ar1['AR_Amount'] + ar2['AR_Amount']
            found_match_for_ar_pair = False
            for src_idx1, src_idx2 in source_pairs:
                if src_idx1 in used_source or src_idx2 in used_source: 
                    continue
                src1, src2 = source_group.loc[src_idx1], source_group.loc[src_idx2]
                all_dates = [ar1['WSR_Date'], ar2['WSR_Date'], src1['Bank_Date'], src2['Bank_Date']]
                if (max(all_dates) - min(all_dates)).days > FINAL_PASS_DATE_WINDOW_DAYS: 
                    continue
                source_sum = src1['Bank_Amount'] + src2['Bank_Amount']
                if abs(ar_sum - source_sum) <= TOLERANCE:
                    logger.info(f"Cleanup Match (2-to-2): ARs {ar_idx1},{ar_idx2} (sum {ar_sum:.2f}) match Deps {src_idx1},{src_idx2} (sum {source_sum:.2f})")
                    match_id, status = f"SUM2x2-{uuid.uuid4()}", "Matched (2 AR to 2 Dep)"
                    combo_dep_data = {'Bank_Date': min(src1['Bank_Date'], src2['Bank_Date']), 'Bank_Amount': source_sum, 'Bank_Reference': f"{src1['Bank_Reference']}, {src2['Bank_Reference']}", 'Bank_Description': f"{src1.get('Bank_Description', '')}; {src2.get('Bank_Description', '')}", 'Match_Group_ID': match_id}
                    updates[ar_idx1], updates[ar_idx2] = (combo_dep_data, status), (combo_dep_data, status)
                    used_ar.update([ar_idx1, ar_idx2])
                    used_source.update([src_idx1, src_idx2])
                    found_match_for_ar_pair = True
                    break
            if found_match_for_ar_pair: 
                continue
    return updates, used_ar, used_source

def process_files(recon_month_obj, merchant_file, wsr_zip_file, bank_file, settlement_file):
    global audit_entries, wsr_error_entries, matched_bank_refs, matched_settlement_nums
    all_recons, audit_entries, wsr_error_entries = [], [], []
    matched_bank_refs, matched_settlement_nums = set(), set()
    st.session_state.missing_weeks_df = pd.DataFrame()
    diagnostic_records = []
    
    try:
        if not all([merchant_file, wsr_zip_file]): 
            raise ValueError("Merchant Numbers and WSR ZIP files are required.")
        designation_map, general_merch, amex_merch, amex_map, general_map = load_merchant_numbers(merchant_file.read())
        bank_df = load_bank_statement(bank_file.read()) if bank_file else None
        settle_df = load_amex_settlement(settlement_file.read()) if settlement_file else None
        
        if bank_df is not None:
            amex_channel_map = amex_merch.dropna(subset=['Merchant Number', 'Channel']).set_index('Merchant Number')['Channel'].to_dict()
            bank_df[['Merchant_Number', 'Store', 'Merchant_Type', 'Channel']] = bank_df['Text'].apply(lambda x: pd.Series(extract_merchant_and_store(x, amex_map, general_map, amex_channel_map)))
            non_amex_channel_map = general_merch.dropna(subset=['Merchant Number', 'Channel']).set_index('Merchant Number')['Channel'].to_dict()
            unknown_mask = (bank_df['Channel'] == 'UNKNOWN') & (bank_df['Merchant_Number'].notna())
            bank_df.loc[unknown_mask, 'Channel'] = bank_df.loc[unknown_mask, 'Merchant_Number'].map(non_amex_channel_map)
        
        processed_files, recon_year, submitted_weeks_by_store, all_found_week_dates = set(), recon_month_obj.year, {}, set()
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(io.BytesIO(wsr_zip_file.read()), 'r') as zf_outer: 
                zf_outer.extractall(temp_dir)
            while True:
                nested_zips = [os.path.join(r, f) for r, _, fs in os.walk(temp_dir) for f in fs if f.lower().endswith('.zip')]
                if not nested_zips: 
                    break
                for zip_path in nested_zips:
                    try:
                        with zipfile.ZipFile(zip_path, 'r') as zf_inner: 
                            zf_inner.extractall(os.path.dirname(zip_path))
                        os.remove(zip_path)
                    except Exception as e: 
                        logger.warning(f"Could not extract or remove {zip_path}: {e}")
            wsr_files = [os.path.join(r, f) for r, _, fs in os.walk(temp_dir) for f in fs if f.lower().endswith(('.xls', '.xlsx'))]
            for fpath in wsr_files:
                store_num, date_str = extract_wsr_info(fpath)
                if store_num and date_str:
                    try:
                        week_date = pd.to_datetime(date_str, errors='coerce', dayfirst=False, yearfirst=False).strftime('%Y-%m-%d')
                        all_found_week_dates.add(week_date)
                        if store_num not in submitted_weeks_by_store: 
                            submitted_weeks_by_store[store_num] = set()
                        submitted_weeks_by_store[store_num].add(week_date)
                    except (ValueError, TypeError): 
                        logger.warning(f"Could not parse date '{date_str}' from filename {os.path.basename(fpath)}")
            progress = st.progress(0, "Processing WSR files...")
            all_wsr_ar = []
            for i, fpath in enumerate(wsr_files):
                store_num_from_name, _ = extract_wsr_info(fpath)
                if not store_num_from_name:
                    logger.warning(f"Could not extract store number from filename: {os.path.basename(fpath)}")
                    continue
                designation = designation_map.get(store_num_from_name)
                if not designation:
                    logger.warning(f"Store {store_num_from_name} has no designation in merchant file")
                    diagnostic_records.append({'file': os.path.basename(fpath), 'store': store_num_from_name, 'status': 'no_designation', 'issues': 'Store not found in merchant designation file'})
                    continue
                progress.progress((i + 1) / len(wsr_files), f"Store {store_num_from_name}")
                with open(fpath, 'rb') as f:
                    ar_df, diagnostics = process_wsr_file(fpath, store_num_from_name, processed_files, f.read(), recon_year)
                    diagnostic_records.append(diagnostics)
                if ar_df is not None and not ar_df.empty:
                    ar_df_filtered = ar_df[pd.to_datetime(ar_df['Date']).between(recon_start, recon_end)]
                    if not ar_df_filtered.empty: 
                        all_wsr_ar.append(ar_df_filtered)
                        logger.info(f"Successfully processed {len(ar_df_filtered)} records from store {store_num_from_name}")
            st.session_state.diagnostic_df = pd.DataFrame(diagnostic_records)
            if not all_wsr_ar: 
                raise ValueError("No valid A/R data was extracted from any WSR file.")
            master_ar_df = pd.concat(all_wsr_ar, ignore_index=True)
            master_ar_df.rename(columns={'Date': 'WSR_Date'}, inplace=True)
            
            # Store master_ar_df for audit reporting
            st.session_state.master_ar_df = master_ar_df.copy()
            
            processed_stores_with_data = set(master_ar_df['Store'].unique())
            
            # Enhanced missing weeks tracking
            def get_expected_week_endings(start_date, end_date):
                """Get all Tuesday week-ending dates in the date range"""
                week_endings = []
                current = start_date
                # Find the first Tuesday on or after start_date
                while current.weekday() != 1:  # 1 = Tuesday
                    current += timedelta(days=1)
                # Collect all Tuesdays in the range
                while current <= end_date:
                    week_endings.append(current)
                    current += timedelta(weeks=1)
                return week_endings
            
            expected_weeks = get_expected_week_endings(recon_start, recon_end)
            expected_week_strings = {week.strftime('%Y-%m-%d') for week in expected_weeks}
            
            logger.info(f"Expected week-ending dates for {recon_month.strftime('%B %Y')}: {sorted(expected_week_strings)}")
            logger.info(f"Actually found week-ending dates: {sorted(all_found_week_dates)}")
            
            # Create comprehensive missing weeks report
            missing_weeks_records = []
            coverage_summary = []
            
            # Check each store in the designation map (all stores that should report)
            all_stores = set(designation_map.keys())
            
            for store in sorted(all_stores):
                submitted_weeks = submitted_weeks_by_store.get(store, set())
                missing_weeks = expected_week_strings - submitted_weeks
                
                # Add detail records for missing weeks
                for week in sorted(missing_weeks):
                    missing_weeks_records.append({
                        'Store': store,
                        'Missing_WSR_Week_Ending': week,
                        'Designation': designation_map.get(store, 'Unknown')
                    })
                
                # Add coverage summary
                coverage_pct = (len(submitted_weeks) / len(expected_week_strings) * 100) if expected_week_strings else 0
                coverage_summary.append({
                    'Store': store,
                    'Expected_Weeks': len(expected_week_strings),
                    'Submitted_Weeks': len(submitted_weeks),
                    'Missing_Weeks': len(missing_weeks),
                    'Coverage_%': coverage_pct,
                    'Status': '✓ Complete' if coverage_pct == 100 else '⚠️ Incomplete' if coverage_pct > 0 else '❌ No Data'
                })
            
            st.session_state.missing_weeks_df = pd.DataFrame(missing_weeks_records)
            st.session_state.coverage_summary_df = pd.DataFrame(coverage_summary)
            
            # Log stores with no data at all
            stores_with_no_data = all_stores - processed_stores_with_data
            if stores_with_no_data:
                logger.warning(f"Stores with NO WSR data submitted: {sorted(stores_with_no_data)}")
                st.session_state.no_data_stores = sorted(stores_with_no_data)
            else:
                st.session_state.no_data_stores = []
            
            # Process reconciliation for each store - ALL USE STATEMENT PROCESSING NOW
            for store_num in processed_stores_with_data:
                designation = designation_map.get(store_num, 'Statement')  # Default to Statement
                store_ar_df = master_ar_df[master_ar_df['Store'] == store_num]
                
                logger.info(f"Processing store {store_num} (originally: {designation}) using Statement reconciliation")
                
                daily_ar = create_ar_totals(store_ar_df, store_num, general_merch, amex_merch)
                
                if daily_ar.empty:
                    logger.warning(f"No daily AR totals created for store {store_num}")
                    continue
                else:
                    logger.info(f"Created {len(daily_ar)} daily AR records for store {store_num}")
                
                # ALL stores now use Statement reconciliation
                if bank_df is not None and settle_df is not None:
                    bank_store_df = bank_df[bank_df['Store'] == store_num].copy()
                    logger.info(f"Found {len(bank_store_df)} bank records for store {store_num}")
                    recon_df, new_b, new_s = reconcile_store_statement(daily_ar, bank_store_df, settle_df, store_num, matched_bank_refs, matched_settlement_nums)
                    matched_bank_refs.update(new_b)
                    matched_settlement_nums.update(new_s)
                    logger.info(f"Statement reconciliation returned {len(recon_df)} records")
                else:
                    logger.warning(f"Bank or settlement file missing for reconciliation of store {store_num}")
                    if bank_df is None:
                        logger.warning("Bank file is None")
                    if settle_df is None:
                        logger.warning("Settlement file is None")
                    # Create unmatched records with all required columns
                    recon_df = daily_ar.copy()
                    recon_df['Status'] = 'No Bank/Settlement File'
                    recon_df['Source'] = 'Statement'
                    recon_df['Bank_Date'] = pd.NaT
                    recon_df['Bank_Amount'] = np.nan
                    recon_df['Bank_Reference'] = None
                    recon_df['Bank_Description'] = None
                    recon_df['Settlement_Amount'] = np.nan
                    recon_df['Settlement_Number'] = None
                    recon_df['Match_Group_ID'] = None
                
                if not recon_df.empty: 
                    all_recons.append(recon_df)
                    logger.info(f"Added {len(recon_df)} reconciliation records for store {store_num}")
                    
        if not all_recons: 
            raise ValueError("No data was reconciled. Check that required files are uploaded.")
            
        wsr_final_df = pd.concat(all_recons, ignore_index=True)
        
        # Ensure Bank_Reference column exists before processing
        if 'Bank_Reference' not in wsr_final_df.columns:
            wsr_final_df['Bank_Reference'] = None
            
        used_source_refs = set(wsr_final_df[wsr_final_df['Bank_Reference'].notna()]['Bank_Reference'])
        unmatched_source_items = []
        if bank_df is not None:
            for _, r in bank_df[~bank_df['Bank Reference'].isin(used_source_refs)].iterrows(): 
                unmatched_source_items.append({'Bank_Date': r['As Of'], 'Store': r['Store'], 'Bank_Amount': r['Amount'], 'Status': 'No WSR Match', 'Source': 'Statement', 'Bank_Reference': r['Bank Reference'], 'Bank_Description': r['Text'], 'Merchant_Number': r['Merchant_Number'], 'Channel': r['Channel'], 'Merchant_Type': r['Merchant_Type']})
        
        unmatched_source_df = pd.DataFrame(unmatched_source_items)
        if not unmatched_source_df.empty: 
            unmatched_source_df['Bank_Date'] = pd.to_datetime(unmatched_source_df['Bank_Date'])
        inferred_matches_n1, remaining_source_after_n1 = final_cleanup_pass(wsr_final_df, unmatched_source_df)
        if inferred_matches_n1:
            for idx, (match_data, status) in inferred_matches_n1.items():
                if idx in wsr_final_df.index:
                    for key, val in match_data.items():
                         if key in wsr_final_df.columns: 
                             wsr_final_df.loc[idx, key] = val
                    wsr_final_df.loc[idx, 'Status'] = status
        remaining_ar_after_n1 = wsr_final_df[wsr_final_df['Status'].str.contains("No .* Match", na=False)].copy()
        inferred_matches_2x2, used_ar_2x2, used_source_2x2 = find_two_by_two_matches(remaining_ar_after_n1, remaining_source_after_n1)
        if inferred_matches_2x2:
            for idx, (match_data, status) in inferred_matches_2x2.items():
                if idx in wsr_final_df.index:
                    for key, val in match_data.items():
                        if key in wsr_final_df.columns: 
                            wsr_final_df.loc[idx, key] = val
                    wsr_final_df.loc[idx, 'Status'] = status
        final_unmatched_source_df = remaining_source_after_n1[~remaining_source_after_n1.index.isin(used_source_2x2)]
        wsr_final_df['Outstanding_AR_Amount'] = np.where(wsr_final_df['Status'].str.contains("Matched|Reversed", na=False), 0, wsr_final_df['AR_Amount'])
        fee_condition = (wsr_final_df['Status'].isin(['Matched with Settlement', 'Matched (Net of Fees)']))
        wsr_final_df['Fee_Amount'] = np.where(fee_condition, wsr_final_df['AR_Amount'] - wsr_final_df['Bank_Amount'], 0)
        final_unmatched_source_df = final_unmatched_source_df.copy()
        final_unmatched_source_df['Outstanding_AR_Amount'] = 0
        final_df = pd.concat([wsr_final_df, final_unmatched_source_df], ignore_index=True).fillna({'Fee_Amount': 0})
        final_df['Sort_Date'] = final_df['WSR_Date'].fillna(final_df['Bank_Date'])
        final_df['Sort_Priority'] = np.where(final_df['WSR_Date'].notna(), 0, 1)
        wsr_final_df['Sort_Date'] = wsr_final_df['WSR_Date'].fillna(wsr_final_df['Bank_Date'])
        final_df_sorted = final_df.sort_values(['Store', 'Sort_Priority', 'Sort_Date'], na_position='last')
        wsr_final_df_sorted = wsr_final_df.sort_values(['Store', 'Sort_Date'], na_position='first')
        cols_order = ['Store', 'WSR_Date', 'Channel', 'Merchant_Type', 'Card_Type', 'Merchant_Number', 'AR_Amount', 'Outstanding_AR_Amount', 'Bank_Date', 'Bank_Amount', 'Fee_Amount', 'Status', 'Source', 'Bank_Reference', 'Bank_Description', 'Settlement_Amount', 'Settlement_Number', 'Match_Group_ID', 'WSR_File', 'WSR_Label']
        final_df_final = final_df_sorted.reindex(columns=cols_order + [c for c in final_df_sorted.columns if c not in cols_order and c not in ['Sort_Date', 'Sort_Priority']]).reset_index(drop=True)
        wsr_final_df_final = wsr_final_df_sorted.reindex(columns=cols_order + [c for c in wsr_final_df_sorted.columns if c not in cols_order and c not in ['Sort_Date']]).reset_index(drop=True)
        st.session_state.processed = True
        st.session_state.recon_df = final_df_final
        st.session_state.wsr_recon_df = wsr_final_df_final
        st.session_state.summary_df = create_summary(final_df)
        st.session_state.audit_df = pd.DataFrame(audit_entries)
        st.session_state.wsr_error_df = pd.DataFrame(wsr_error_entries)
        st.session_state.log_content = log_stream.getvalue()
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            st.session_state.recon_df.to_excel(writer, sheet_name="Full_Reconciliation", index=False)
            st.session_state.wsr_recon_df.to_excel(writer, sheet_name="WSR_Reconciliation", index=False)
            st.session_state.summary_df.to_excel(writer, sheet_name="Summary", index=False)
            if not st.session_state.get('coverage_summary_df', pd.DataFrame()).empty:
                st.session_state.coverage_summary_df.to_excel(writer, sheet_name="WSR_Coverage", index=False)
            if not st.session_state.get('missing_weeks_df', pd.DataFrame()).empty: 
                st.session_state.missing_weeks_df.to_excel(writer, sheet_name="Missing_WSR_Detail", index=False)
            if not st.session_state.audit_df.empty: 
                st.session_state.audit_df.to_excel(writer, sheet_name="Audit", index=False)
            if not st.session_state.wsr_error_df.empty: 
                st.session_state.wsr_error_df.to_excel(writer, sheet_name="WSR_Errors", index=False)
        st.session_state.excel_file = output.getvalue()
        return True
    except Exception as e:
        logger.exception("Processing failed")
        st.session_state.error_message, st.session_state.log_content = str(e), log_stream.getvalue()
        return False

def create_summary(df):
    if df.empty: 
        return pd.DataFrame()
    summary = []
    for store in sorted(df['Store'].dropna().unique()):
        store_df = df[df['Store'] == store]
        total_ar, matched_ar = store_df['AR_Amount'].sum(), store_df[store_df['Status'].str.contains('Matched|Reversed', na=False)]['AR_Amount'].sum()
        unmatched_bank = store_df[store_df['Status'] == 'No WSR Match']['Bank_Amount'].sum()
        summary.append({'Store': store, 'Total_AR_Amount': total_ar, 'Matched_AR_Amount': matched_ar, 'Outstanding_AR_Amount': total_ar - matched_ar, 'Unmatched_Bank_Amount': unmatched_bank})
    comp_total, comp_matched = df['AR_Amount'].sum(), df[df['Status'].str.contains('Matched|Reversed', na=False)]['AR_Amount'].sum()
    summary.append({'Store': 'Company-Wide', 'Total_AR_Amount': comp_total, 'Matched_AR_Amount': comp_matched, 'Outstanding_AR_Amount': comp_total - comp_matched, 'Unmatched_Bank_Amount': df[df['Status'] == 'No WSR Match']['Bank_Amount'].sum()})
    return pd.DataFrame(summary)

def display_visualizations(df):
    st.subheader("Visual Summary")
    ar_df = df[df['AR_Amount'].notna()].copy()
    if ar_df.empty: 
        st.warning("No A/R data available to generate visualizations.")
        return
    total_ar, outstanding_ar = ar_df['AR_Amount'].sum(), ar_df['Outstanding_AR_Amount'].sum()
    matched_ar, outstanding_pct = total_ar - outstanding_ar, (outstanding_ar / total_ar * 100) if total_ar > 0 else 0
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total A/R", f"${total_ar:,.2f}")
    col2.metric("Matched A/R", f"${matched_ar:,.2f}")
    col3.metric("Outstanding A/R", f"${outstanding_ar:,.2f}")
    col4.metric("Outstanding %", f"{outstanding_pct:.2f}%")
    st.markdown("---")
    v_col1, v_col2 = st.columns(2)
    with v_col1:
        st.markdown("##### Overall Reconciliation Status")
        status_data = pd.DataFrame({'Status': ['Matched A/R', 'Outstanding A/R'], 'Amount': [matched_ar, outstanding_ar]})
        fig_pie = px.pie(status_data, values='Amount', names='Status', color_discrete_map={'Matched A/R': 'green', 'Outstanding A/R': 'red'})
        st.plotly_chart(fig_pie, use_container_width=True)
        st.markdown("##### Amex vs. Non-Amex Reconciliation")
        amex_summary = ar_df.groupby('Merchant_Type')[['AR_Amount', 'Outstanding_AR_Amount']].sum().reset_index()
        amex_summary['Matched_AR_Amount'] = amex_summary['AR_Amount'] - amex_summary['Outstanding_AR_Amount']
        fig_amex = px.bar(amex_summary, x='Merchant_Type', y=['Matched_AR_Amount', 'Outstanding_AR_Amount'], title="Matched vs. Outstanding by Type", labels={'value': 'Total Amount ($)', 'Merchant_Type': 'Merchant Type'}, barmode='stack', color_discrete_map={'Matched_AR_Amount': 'green', 'Outstanding_AR_Amount': 'red'})
        st.plotly_chart(fig_amex, use_container_width=True)
    with v_col2:
        st.markdown("##### Reconciliation Health by Store")
        store_summary = ar_df.groupby('Store').agg(Total_AR_Amount=('AR_Amount', 'sum'), Outstanding_AR_Amount=('Outstanding_AR_Amount', 'sum')).reset_index()
        store_summary = store_summary[store_summary['Total_AR_Amount'] > 0]
        store_summary['Outstanding_Pct'] = store_summary['Outstanding_AR_Amount'] / store_summary['Total_AR_Amount']
        store_summary = store_summary.sort_values('Outstanding_AR_Amount', ascending=False)
        fig_store = px.bar(store_summary, x='Store', y='Outstanding_AR_Amount', hover_data={'Outstanding_AR_Amount': ':.2f', 'Total_AR_Amount': ':.2f', 'Outstanding_Pct': ':.2%'}, labels={'Outstanding_AR_Amount': 'Outstanding A/R ($)', 'Store': 'Store Number'})
        fig_store.update_traces(marker_color='orange')
        st.plotly_chart(fig_store, use_container_width=True)
        st.markdown("##### Reconciliation by Source")
        source_summary = ar_df.groupby('Source')[['AR_Amount', 'Outstanding_AR_Amount']].sum().reset_index()
        if not source_summary.empty: 
            source_summary['Matched_AR_Amount'] = source_summary['AR_Amount'] - source_summary['Outstanding_AR_Amount']
            fig_source = px.bar(source_summary, x='Source', y=['Matched_AR_Amount', 'Outstanding_AR_Amount'], title="Matched vs. Outstanding by Source", labels={'value': 'Total Amount ($)', 'Source': 'Data Source'}, barmode='stack', color_discrete_map={'Matched_AR_Amount': 'green', 'Outstanding_AR_Amount': 'red'})
            st.plotly_chart(fig_source, use_container_width=True)

# --- PROCESS FILES BUTTON AND RESULTS DISPLAY ---
if st.button("Process Files", key="process_files_button", type="primary", use_container_width=True):
    st.session_state.processed = False
    log_stream.truncate(0)
    log_stream.seek(0)
    with st.spinner('Processing reconciliation...'):
        success = process_files(recon_month, merchant_file, wsr_zip_file, bank_file, settlement_file)
    if success: 
        st.success("Processing completed successfully! All stores are now processed using Statement reconciliation.")
    else: 
        st.error(f"Processing failed: {st.session_state.error_message}")
        st.text_area("Error Logs", st.session_state.log_content, height=300)

# Rest of the display code remains the same...
if st.session_state.processed:
    st.subheader("Summary")
    st.dataframe(st.session_state.summary_df.style.format({c: "${:,.2f}" for c in st.session_state.summary_df.columns if 'Amount' in c}))
    
    display_visualizations(st.session_state.recon_df)
    st.subheader("Reconciliation Details")
    view_option = st.radio("Select View", ("WSR Reconciliation", "Full Reconciliation"), horizontal=True)
    display_df = st.session_state.wsr_recon_df if view_option == "WSR Reconciliation" else st.session_state.recon_df
    f1, f2, f3 = st.columns(3)
    filters = {'Store': f1.multiselect("Filter by Store", sorted(display_df["Store"].dropna().unique().astype(str))), 'Source': f2.multiselect("Filter by Source", sorted(display_df["Source"].dropna().unique())), 'Status': f3.multiselect("Filter by Status", sorted(display_df["Status"].dropna().unique()))}
    filtered_df = display_df.copy()
    for col, val in filters.items():
        if val: 
            filtered_df = filtered_df[filtered_df[col].isin(val)]
    st.dataframe(filtered_df.style.format({'AR_Amount': '${:,.2f}', 'Outstanding_AR_Amount': '${:,.2f}', 'Bank_Amount': '${:,.2f}', 'Fee_Amount': '${:,.2f}'}))
    st.subheader("WSR Coverage Report")
    
    # Show coverage summary first
    if not st.session_state.get('coverage_summary_df', pd.DataFrame()).empty:
        coverage_df = st.session_state.coverage_summary_df
        
        # Calculate overall stats
        total_expected = coverage_df['Expected_Weeks'].sum()
        total_submitted = coverage_df['Submitted_Weeks'].sum()
        total_missing = coverage_df['Missing_Weeks'].sum()
        overall_coverage = (total_submitted / total_expected * 100) if total_expected > 0 else 0
        
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Expected WSRs", f"{total_expected:,}")
        col2.metric("Total Submitted", f"{total_submitted:,}")
        col3.metric("Total Missing", f"{total_missing:,}")
        col4.metric("Overall Coverage", f"{overall_coverage:.1f}%")
        
        # Show stores with missing weeks
        incomplete_stores = coverage_df[coverage_df['Coverage_%'] < 100].sort_values('Coverage_%')
        if not incomplete_stores.empty:
            st.warning(f"⚠️ {len(incomplete_stores)} stores have missing WSR weeks")
            
            # Show critical stores (< 50% coverage)
            critical_stores = incomplete_stores[incomplete_stores['Coverage_%'] < 50]
            if not critical_stores.empty:
                st.error(f"🔴 {len(critical_stores)} stores have critical coverage issues (< 50% coverage)")
                st.dataframe(critical_stores[['Store', 'Expected_Weeks', 'Submitted_Weeks', 'Missing_Weeks', 'Coverage_%', 'Status']], 
                           use_container_width=True)
        
        # Show stores with no data
        if st.session_state.get('no_data_stores', []):
            st.error(f"❌ {len(st.session_state.no_data_stores)} stores submitted NO WSR data:")
            st.write(", ".join(st.session_state.no_data_stores))
        
        # Expandable detailed views
        with st.expander("Full Coverage Summary by Store"):
            st.dataframe(coverage_df.sort_values('Coverage_%'), use_container_width=True)
        
        with st.expander("Detailed Missing Weeks by Store"):
            if not st.session_state.get('missing_weeks_df', pd.DataFrame()).empty:
                missing_df = st.session_state.missing_weeks_df
                # Add ability to filter by store
                selected_store = st.selectbox("Filter by Store (optional)", 
                                            ['All'] + sorted(missing_df['Store'].unique().tolist()))
                if selected_store != 'All':
                    missing_df = missing_df[missing_df['Store'] == selected_store]
                st.dataframe(missing_df, use_container_width=True)
    
    st.subheader("Other Issues")
    if not st.session_state.audit_df.empty: 
        st.write("Audit Issues")
        st.dataframe(st.session_state.audit_df)
    if not st.session_state.wsr_error_df.empty: 
        st.write("WSR Errors")
        st.dataframe(st.session_state.wsr_error_df)
    if not st.session_state.get('diagnostic_df', pd.DataFrame()).empty:
        st.subheader("WSR Processing Diagnostics")
        problem_files = st.session_state.diagnostic_df[st.session_state.diagnostic_df['status'] != 'success']
        if not problem_files.empty:
            st.warning(f"Found {len(problem_files)} files with processing issues")
            with st.expander("Processing Issues"):
                st.dataframe(problem_files)
        success_files = st.session_state.diagnostic_df[st.session_state.diagnostic_df['status'] == 'success']
        if not success_files.empty:
            st.success(f"Successfully processed {len(success_files)} files")
            with st.expander("Successfully Processed Files"):
                st.dataframe(success_files[['file', 'store', 'ar_records']])
    
    # --- WSR AR COUNT AUDIT REPORT ---
    if not st.session_state.master_ar_df.empty:
        st.markdown("---")
        st.subheader("🔍 WSR AR Count Audit Report")
        
        # Generate audit reports
        audit_df, summary_stats, variance_analysis = create_wsr_audit_report(
            st.session_state.master_ar_df, 
            st.session_state.diagnostic_df
        )
        
        # Show high variance stores
        high_variance_stores = summary_stats[summary_stats['Store_Health'] != '🟢 Consistent']
        if not high_variance_stores.empty:
            st.warning(f"Found {len(high_variance_stores)} stores with variance issues")
            st.dataframe(high_variance_stores[['Store', 'AR_Count_Mean', 'CV_Percent', 'Store_Health']])
        
        # Show all stores summary
        with st.expander("All Stores Variance Analysis"):
            st.dataframe(summary_stats[['Store', 'AR_Count_Mean', 'AR_Count_Std', 
                                        'CV_Percent', 'Store_Health']], 
                        use_container_width=True)
    
    st.subheader("Download Results")
    if st.session_state.excel_file:
        st.download_button("Download Report", st.session_state.excel_file, f"reconciliation_{datetime.now():%Y%m%d}.xlsx", "application/vnd.ms-excel")
    st.subheader("Logs")
    st.text_area("Processing Logs", st.session_state.log_content, height=300)