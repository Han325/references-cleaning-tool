import logging
from pathlib import Path
import unicodedata
import re
from typing import List, Dict, Set, Any
import pandas as pd
import pydash as py_
import csv

class CSVDeduplicator:
    def __init__(self, comparison_columns: List[str], log_level=logging.DEBUG):  # Changed to DEBUG level
        """
        Initialize the CSV deduplicator with specified columns to use for comparison.
        
        Args:
            comparison_columns: List of column names to use for detecting duplicates
            log_level: Logging level
        """
        # Set up logging
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/csv_deduplication.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Store comparison columns
        self.comparison_columns = comparison_columns
        
        # Create a separate file for duplicate entries
        self.duplicate_file = Path('csv_duplicates.txt')
        # Clear the duplicates file at start
        self.duplicate_file.write_text('')

    def normalize_string(self, text: str) -> str:
        """Normalize strings for comparison by removing special characters and whitespace."""
        if pd.isna(text):  # Handle None/NaN values
            return ""
            
        text = str(text)  # Convert to string
        # Convert to lowercase and normalize unicode characters
        text = unicodedata.normalize('NFKD', text.lower())
        # Remove special characters and extra whitespace
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def generate_comparison_key(self, row: Dict[str, Any]) -> str:
        """Generate a unique comparison key from specified columns."""
        key_parts = []
        for col in self.comparison_columns:
            if col in row:
                val = self.normalize_string(str(row[col]))
                key_parts.append(val)
            else:
                self.logger.warning(f"Column {col} not found in data")
                key_parts.append("")
        
        return "_".join(key_parts)

    def read_csv_file(self, file_path: Path) -> pd.DataFrame:
        """Read a CSV file and return its entries as a DataFrame."""
        try:
            # Read CSV with utf-8 encoding
            df = pd.read_csv(file_path, encoding='utf-8')
            
            # Add source file information
            df['source_file'] = str(file_path)
            self.logger.info(f"Successfully read {len(df)} rows from {file_path}")
            
            return df
            
        except UnicodeDecodeError:
            # Try alternative encoding if utf-8 fails
            try:
                df = pd.read_csv(file_path, encoding='latin1')
                df['source_file'] = str(file_path)
                self.logger.info(f"Successfully read {len(df)} rows from {file_path} using latin1 encoding")
                return df
            except Exception as e:
                self.logger.error(f"Error reading file {file_path} with alternative encoding: {str(e)}")
                return pd.DataFrame()
                
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {str(e)}")
            return pd.DataFrame()
                        
    def deduplicate_entries(self, input_files: List[Path]) -> pd.DataFrame:
        """Process multiple CSV files and remove duplicates using pydash."""
        all_data = pd.DataFrame()
        duplicates_found = 0

        # Store all entries with their keys for debugging
        all_entries_with_keys = []

        # Read and process all input files
        for file_path in input_files:
            df = self.read_csv_file(file_path)
            if df.empty:
                continue
                
            # Convert DataFrame to list of dictionaries for easier processing
            entries = df.to_dict('records')
            
            # Generate and log comparison keys for debugging
            for entry in entries:
                key = self.generate_comparison_key(entry)
                # self.logger.debug(f"\nEntry from {entry['source_file']}:")
                # self.logger.debug(f"Title: {entry['Item Title']}")
                # self.logger.debug(f"Authors: {entry['Authors']}")
                # self.logger.debug(f"Year: {entry['Publication Year']}")
                # self.logger.debug(f"Generated key: {key}")
                all_entries_with_keys.append((key, entry))

        # Group by keys
        key_groups = py_.group_by(all_entries_with_keys, lambda x: x[0])
        
        # Process each group
        for key, group in key_groups.items():
            if len(group) > 1:
                # We found duplicates
                duplicates_found += len(group) - 1
                original_entry = group[0][1]
                duplicates = [g[1] for g in group[1:]]
                
                # Add first occurrence to result
                all_data = pd.concat([all_data, pd.DataFrame([original_entry])], ignore_index=True)
                
                # Log duplicates
                for duplicate in duplicates:
                    duplicate_info = (
                        f"\nDuplicate found:\n"
                        f"Original entry from: {original_entry['source_file']}\n"
                        f"Item Title: {original_entry['Item Title']}\n"
                        f"Authors: {original_entry['Authors']}\n"
                        f"Publication Year: {original_entry['Publication Year']}\n"
                        f"Duplicate entry from: {duplicate['source_file']}\n"
                    )
                    
                    self.logger.info(duplicate_info)
                    
                    # Write to duplicates file
                    with open(self.duplicate_file, 'a', encoding='utf-8') as f:
                        f.write(duplicate_info)
                        f.write("\nOriginal row:\n")
                        f.write(str(original_entry))
                        f.write("\n\nDuplicate row:\n")
                        f.write(str(duplicate))
                        f.write("\n" + "="*50 + "\n")
            else:
                # Unique entry
                all_data = pd.concat([all_data, pd.DataFrame([group[0][1]])], ignore_index=True)

        self.logger.info(f"Found and removed {duplicates_found} duplicate entries")
        self.logger.info(f"Retained {len(all_data)} unique entries")
        if duplicates_found > 0:
            self.logger.info(f"Detailed duplicate information written to {self.duplicate_file}")
        
        return all_data

    def write_output(self, df: pd.DataFrame, output_file: Path) -> None:
        """Write the deduplicated entries to a new CSV file."""
        try:
            # Create output directory if it doesn't exist
            output_file.parent.mkdir(exist_ok=True)
            
            # Remove source_file column before writing
            if 'source_file' in df.columns:
                df = df.drop('source_file', axis=1)
                
            # Write to CSV
            df.to_csv(output_file, index=False, encoding='utf-8')
            self.logger.info(f"Successfully wrote {len(df)} entries to {output_file}")
        except Exception as e:
            self.logger.error(f"Error writing output file: {str(e)}")

def main():
    """Main function to run the deduplication process."""
    # Specify which columns to use for comparison - matching the actual CSV structure
    comparison_columns = ['Item Title', 'Authors', 'Publication Year']  # Matching the CSV columns
    
    # Initialize deduplicator
    deduplicator = CSVDeduplicator(comparison_columns)
    
    # Get input files
    input_directory = Path('files/SL')  # Update to match your directory structure
    input_files = list(input_directory.glob('*csv'))
    
    if not input_files:
        deduplicator.logger.error("No CSV files found in the input directory")
        return
    
    # Process the files
    deduplicated_data = deduplicator.deduplicate_entries(input_files)
    
    # Write output
    output_file = Path('output/csv_deduplicated.csv')
    deduplicator.write_output(deduplicated_data, output_file)

if __name__ == "__main__":
    main()