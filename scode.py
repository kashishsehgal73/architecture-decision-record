import pandas as pd
import os
from pathlib import Path

# ============================================
# CONFIGURATION - EDITABLE VARIABLES
# ============================================

# Input folder containing RPT files
INPUT_FOLDER_PATH = r'd:\work\Samridhi'

# Output folder for processed files
OUTPUT_FOLDER_PATH = r'd:\work\Samridhi\output'

# File patterns to process
RPT_FILE_PATTERNS = ['*.rpt', 'RPT_FILE*.txt']

# Excel file with update instructions
EXCEL_FILE_PATH = r'd:\work\Samridhi\column_mappings.xlsx'
EXCEL_SHEET_NAME = 'Update_Values'

# Output file suffix
OUTPUT_SUFFIX = ''

# ============================================
# FUNCTIONS
# ============================================

def read_rpt_file(file_path):
    """
    Read a single .rpt file, skipping header (line 1) and footer (lines after ##)
    
    Args:
        file_path: Path to the .rpt file
    
    Returns:
        pandas DataFrame with the data, header line, footer lines, and columns with quotes
    """
    all_data = []
    columns = None
    header_line = None
    footer_lines = []
    columns_with_quotes = set()  # Track which columns had quoted values
    column_marker = None
    
    print(f"Processing: {file_path.name}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # Save header
    if len(lines) > 0:
        header_line = lines[0]
    
    # Skip first line (header)
    if len(lines) <= 1:
        return pd.DataFrame(), header_line, footer_lines, columns_with_quotes
    
    # Find where footer starts (line with ## at the beginning)
    footer_start = None
    for i, line in enumerate(lines):
        if line.strip().startswith('##'):
            footer_start = i
            break
    
    # Save footer
    if footer_start is not None:
        footer_lines = lines[footer_start:]
    
    # Extract data lines (skip line 0, stop before footer)
    if footer_start is not None:
        data_lines = lines[1:footer_start]
    else:
        data_lines = lines[1:]
    
    # Parse each data line
    first_data_row = True
    for line in data_lines:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        
        parts = line.split(',')
        
        # Parse column header: !1,SPCODE,POL_NO,...
        if columns is None and parts[0].startswith('!'):
            col_parts = [p.strip('!').strip() for p in parts]
            if col_parts:
                column_marker = col_parts[0]
                columns = col_parts[1:]
            continue
        
        # Parse data row: *,5,"XY1100",...
        if parts[0].startswith('*'):
            # Track quoted columns from first data row only
            if first_data_row:
                for i, p in enumerate(parts[1:]):
                    p = p.strip()
                    if p.startswith('"') and p.endswith('"') and i < len(columns):
                        columns_with_quotes.add(columns[i])
                first_data_row = False
            
            # Build row data - just remove quotes, pandas handles the rest
            row_data = [p.strip('"') for p in parts[1:]]
            
            if row_data and columns and len(row_data) == len(columns):
                all_data.append(row_data)
    
    # Create DataFrame
    if all_data and columns:
        df = pd.DataFrame(all_data, columns=columns)
        return df, header_line, footer_lines, columns_with_quotes, column_marker
    else:
        return pd.DataFrame(), header_line, footer_lines, columns_with_quotes, column_marker


def write_rpt_file(df, output_path, header_line, footer_lines, columns_with_quotes, column_marker=None):
    """
    Write DataFrame back to RPT file format with header and footer
    
    Args:
        df: pandas DataFrame to write
        output_path: Path to output file
        header_line: Original header line
        footer_lines: Original footer lines
        columns_with_quotes: Set of column names that should have quoted values
        column_marker: The marker value to use in column header (e.g., '1')
    """
    with open(output_path, 'w', encoding='utf-8') as f:
        # Update and write header (update column count if needed)
        if header_line:
            parts = header_line.strip().split()
            if len(parts) > 0:
                # Update first value (column count)
                parts[0] = str(len(df.columns))
                f.write(' '.join(parts) + '\n')
            else:
                f.write(header_line)
        
        # Write column header line
        if column_marker:
            col_line = '!' + column_marker + ',' + ','.join(df.columns)
        else:
            col_line = '!' + ','.join(df.columns)
        f.write(col_line + '\n')
        
        # Write data rows
        for idx, row in df.iterrows():
            # Format each value - add quotes based on original file format
            formatted_values = []
            for col_name, val in zip(df.columns, row):
                # Add quotes if this column had quotes originally
                if col_name in columns_with_quotes:
                    formatted_values.append(f'"{val}"')
                else:
                    formatted_values.append(str(val))
            
            data_line = '*,' + ','.join(formatted_values)
            f.write(data_line + '\n')
        
        # Write blank line before footer
        f.write('\n')
        
        # Write footer
        if footer_lines:
            for line in footer_lines:
                f.write(line)
    
    print(f"\nWritten to: {output_path}")


def smart_match(series, target_value):
    """Compare values - numeric if possible, otherwise case-sensitive string"""
    try:
        target_numeric = pd.to_numeric(target_value)
        series_numeric = pd.to_numeric(series, errors='coerce')
        valid_numeric = series_numeric.notna()
        numeric_mask = series_numeric == target_numeric
        string_mask = series.astype(str) == str(target_value)
        result = numeric_mask.copy()
        result[~valid_numeric] = string_mask[~valid_numeric]
        return result
    except (ValueError, TypeError):
        return series.astype(str) == str(target_value)


def apply_updates(rpt_data, update_values_df, columns_with_quotes):
    """Apply add, delete, and update actions to the dataframe"""
    
    # Add columns
    add_actions = update_values_df[update_values_df['Action'] == 'add']
    if not add_actions.empty:
        for idx, row in add_actions.iterrows():
            column_name = row['ColumnName']
            new_value = row['NewValue']
            rpt_data[column_name] = new_value
            
            # Mark string columns for quoting
            try:
                pd.to_numeric(new_value)
            except (ValueError, TypeError):
                columns_with_quotes.add(column_name)
            
            print(f"Added column '{column_name}' with value: {new_value}")
    
    # Delete columns
    delete_actions = update_values_df[update_values_df['Action'] == 'delete']
    if not delete_actions.empty:
        for idx, row in delete_actions.iterrows():
            column_name = row['ColumnName']
            if column_name in rpt_data.columns:
                rpt_data = rpt_data.drop(columns=[column_name])
                print(f"Deleted column '{column_name}'")
    
    # Update columns
    update_actions = update_values_df[update_values_df['Action'] == 'update']
    if not update_actions.empty:
        for idx, row in update_actions.iterrows():
            column_name = row['ColumnName']
            new_value = row['NewValue']
            
            if column_name not in rpt_data.columns:
                print(f"Column '{column_name}' not found, skipping")
                continue
            
            has_lookup_col = pd.notna(row.get('LookupColumn'))
            has_lookup_val = pd.notna(row.get('LookupValue'))
            has_current_val = pd.notna(row.get('CurrentValue'))
            
            lookup_column = row.get('LookupColumn') if has_lookup_col else None
            lookup_value = row.get('LookupValue') if has_lookup_val else None
            current_value = row.get('CurrentValue') if has_current_val else None
            
            # Apply update based on scenario
            if has_lookup_col and has_lookup_val and has_current_val:
                if lookup_column in rpt_data.columns:
                    mask = smart_match(rpt_data[lookup_column], lookup_value) & smart_match(rpt_data[column_name], current_value)
                    rpt_data.loc[mask, column_name] = new_value
                    print(f"Updated '{column_name}' where '{lookup_column}'=='{lookup_value}' AND '{column_name}'=='{current_value}' ({mask.sum()} rows)")
            
            elif has_lookup_col and has_lookup_val:
                if lookup_column in rpt_data.columns:
                    mask = smart_match(rpt_data[lookup_column], lookup_value)
                    rpt_data.loc[mask, column_name] = new_value
                    print(f"Updated '{column_name}' where '{lookup_column}'=='{lookup_value}' ({mask.sum()} rows)")
            
            elif has_current_val:
                mask = smart_match(rpt_data[column_name], current_value)
                rpt_data.loc[mask, column_name] = new_value
                print(f"Updated '{column_name}' from '{current_value}' to '{new_value}' ({mask.sum()} rows)")
            
            else:
                rpt_data[column_name] = new_value
                print(f"Updated all rows in '{column_name}' to '{new_value}'")
    
    return rpt_data


# ============================================
# MAIN SCRIPT
# ============================================

input_folder = Path(INPUT_FOLDER_PATH)
output_folder = Path(OUTPUT_FOLDER_PATH)

# Create output folder if it doesn't exist
output_folder.mkdir(parents=True, exist_ok=True)

rpt_files = []
for pattern in RPT_FILE_PATTERNS:
    rpt_files.extend(input_folder.glob(pattern))

print(f"\nFound {len(rpt_files)} RPT file(s) to process")

# Load Excel file with update instructions
has_excel = os.path.exists(EXCEL_FILE_PATH)

if has_excel:
    update_values_df = pd.read_excel(EXCEL_FILE_PATH, sheet_name=EXCEL_SHEET_NAME)
    print("\n" + "="*60)
    print("UPDATE VALUES DATA")
    print("="*60)
    print(update_values_df)
else:
    update_values_df = None

# Process each RPT file
for source_file in rpt_files:
    print("\n" + "="*60)
    print(f"PROCESSING: {source_file.name}")
    print("="*60)
    
    rpt_data, header_line, footer_lines, columns_with_quotes, column_marker = read_rpt_file(source_file)

    print("\nOriginal Data:")
    print(rpt_data)
    print(f"Shape: {rpt_data.shape}")
    
    if has_excel and update_values_df is not None:
        rpt_data = apply_updates(rpt_data, update_values_df, columns_with_quotes)
        
        print("\nUpdated Data:")
        print(rpt_data)
        print(f"Shape: {rpt_data.shape}")
    
    output_file = output_folder / f"{source_file.stem}{OUTPUT_SUFFIX}{source_file.suffix}"
    write_rpt_file(rpt_data, output_file, header_line, footer_lines, columns_with_quotes, column_marker)

print("\n" + "="*60)
print("ALL FILES PROCESSED")
print("="*60)
