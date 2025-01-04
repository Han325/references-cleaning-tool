import logging
from pathlib import Path
import pandas as pd
import unicodedata
import re
from typing import List, Dict, Set, Tuple

class ExcelDeduplicator:
    def __init__(self, comparison_columns: List[str], log_level=logging.INFO):
        """
        Initialize the deduplicator with specified columns to use for comparison.
        
        Args:
            comparison_columns: List of column names to use for detecting duplicates
            log_level: Logging level
        """
        # Set up logging
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/excel_deduplication.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Store comparison columns
        self.comparison_columns = comparison_columns
        
        # Create a separate file for duplicate entries
        self.duplicate_file = Path('duplicates/excel_duplicates.txt')
        # Clear the duplicates file at start
        self.duplicate_file.write_text('')

    def normalize_string(self, text: str) -> str:
        """Normalize strings for comparison by removing special characters and whitespace."""
        if pd.isna(text):  # Handle NaN/None values
            return ""
            
        text = str(text)  # Convert to string in case of numbers
        # Convert to lowercase and normalize unicode characters
        text = unicodedata.normalize('NFKD', text.lower())
        # Remove special characters and extra whitespace
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def generate_comparison_key(self, row: pd.Series) -> str:
        """Generate a unique comparison key from specified columns."""
        key_parts = []
        for col in self.comparison_columns:
            if col in row:
                val = self.normalize_string(row[col])
                key_parts.append(val)
            else:
                self.logger.warning(f"Column {col} not found in data")
                key_parts.append("")
        
        return "_".join(key_parts)

    def read_excel_file(self, file_path: Path) -> pd.DataFrame:
        """Read an Excel file and return its contents as a DataFrame."""
        try:
            df = pd.read_excel(file_path)
            df['source_file'] = str(file_path)  # Add source file information
            self.logger.info(f"Successfully read {len(df)} rows from {file_path}")
            return df
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {str(e)}")
            return pd.DataFrame()

    def deduplicate_entries(self, input_files: List[Path]) -> pd.DataFrame:
        """Process multiple Excel files and remove duplicates."""
        all_data = pd.DataFrame()
        unique_entries: Dict[str, pd.Series] = {}
        duplicates_found = 0

        # Read and process all input files
        for file_path in input_files:
            df = self.read_excel_file(file_path)
            if df.empty:
                continue
                
            # Process each row
            for idx, row in df.iterrows():
                comparison_key = self.generate_comparison_key(row)
                
                if comparison_key not in unique_entries:
                    unique_entries[comparison_key] = row
                    all_data = pd.concat([all_data, pd.DataFrame([row])], ignore_index=True)
                else:
                    duplicates_found += 1
                    original_row = unique_entries[comparison_key]
                    
                    # Log duplicate information
                    duplicate_info = (
                        f"\nDuplicate found:\n"
                        f"Original entry from: {original_row['source_file']}\n"
                    )
                    
                    # Add comparison column values
                    for col in self.comparison_columns:
                        if col in original_row:
                            duplicate_info += f"{col}: {original_row[col]}\n"
                    
                    duplicate_info += f"Duplicate entry from: {row['source_file']}\n"
                    
                    self.logger.info(duplicate_info)
                    
                    # Write to duplicates file
                    with open(self.duplicate_file, 'a', encoding='utf-8') as f:
                        f.write(duplicate_info)
                        f.write("\nOriginal row:\n")
                        f.write(str(original_row.to_dict()))
                        f.write("\n\nDuplicate row:\n")
                        f.write(str(row.to_dict()))
                        f.write("\n" + "="*50 + "\n")

        self.logger.info(f"Found and removed {duplicates_found} duplicate entries")
        self.logger.info(f"Retained {len(all_data)} unique entries")
        self.logger.info(f"Detailed duplicate information written to {self.duplicate_file}")
        
        return all_data

    def write_output(self, df: pd.DataFrame, output_file: Path) -> None:
        """Write the deduplicated entries to a new Excel file."""
        try:
            # Create output directory if it doesn't exist
            output_file.parent.mkdir(exist_ok=True)
            
            # Remove the source_file column before writing
            if 'source_file' in df.columns:
                df = df.drop('source_file', axis=1)
            
            # Write to Excel
            df.to_excel(output_file, index=False)
            self.logger.info(f"Successfully wrote {len(df)} entries to {output_file}")
        except Exception as e:
            self.logger.error(f"Error writing output file: {str(e)}")

def main():
    """Main function to run the deduplication process."""
    # Specify which columns to use for comparison
    comparison_columns = ['Article Title', 'Authors', 'Publication Year']  # Modify these based on your Excel structure
    
    # Initialize deduplicator
    deduplicator = ExcelDeduplicator(comparison_columns)
    
    # Get input files
    input_directory = Path('files/WS')
    input_files = list(input_directory.glob('*.xls*'))  # Matches .xls and .xlsx
    
    if not input_files:
        deduplicator.logger.error("No Excel files found in the input directory")
        return
    
    # Process the files
    deduplicated_data = deduplicator.deduplicate_entries(input_files)
    
    # Write output
    output_file = Path('output/deduplicated.xlsx')
    deduplicator.write_output(deduplicated_data, output_file)

if __name__ == "__main__":
    main()