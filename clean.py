import bibtexparser
import logging
from pathlib import Path
from typing import List, Dict, Set, Tuple
import re
import unicodedata
from difflib import SequenceMatcher

class BibDeduplicator:
    def __init__(self, log_level=logging.INFO):
        """Initialize the deduplicator with logging configuration."""
        # Create logs directory if it doesn't exist
        Path('logs').mkdir(exist_ok=True)
        
        # Set up logging
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/bib_deduplication.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
        # Create a separate file for duplicate entries
        self.duplicate_file = Path('duplicates.txt')
        # Clear the duplicates file at start
        self.duplicate_file.write_text('')

    def normalize_string(self, text: str) -> str:
        """Normalize strings for comparison by removing special characters and whitespace."""
        if not text:
            return ""
        # Convert to lowercase and normalize unicode characters
        text = unicodedata.normalize('NFKD', text.lower())
        # Remove special characters and extra whitespace
        text = re.sub(r'[^\w\s]', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def calculate_similarity(self, str1: str, str2: str) -> float:
        """Calculate similarity between two strings using SequenceMatcher."""
        return SequenceMatcher(None, str1, str2).ratio()

    def are_entries_similar(self, entry1: Dict, entry2: Dict, 
                          title_threshold: float = 0.95,
                          author_threshold: float = 0.8) -> bool:
        """Check if two entries are similar based on title and author similarity."""
        title1 = self.normalize_string(entry1.get('title', ''))
        title2 = self.normalize_string(entry2.get('title', ''))
        
        title_similarity = self.calculate_similarity(title1, title2)
        
        if title_similarity > title_threshold:
            # Check authors if titles are similar
            authors1 = self.normalize_string(entry1.get('author', ''))
            authors2 = self.normalize_string(entry2.get('author', ''))
            author_similarity = self.calculate_similarity(authors1, authors2)
            
            return author_similarity > author_threshold
        
        return False

    def check_doi_duplicates(self, entries: List[Dict]) -> List[Tuple[Dict, Dict]]:
        """Find entries with identical DOIs."""
        doi_map = {}
        duplicates = []
        
        for entry in entries:
            doi = entry.get('doi')
            if doi:
                doi = doi.lower().strip()  # Normalize DOI
                if doi in doi_map:
                    duplicates.append((doi_map[doi], entry))
                else:
                    doi_map[doi] = entry
                    
        return duplicates

    def read_bib_file(self, file_path: Path) -> List[Dict]:
        """Read a .bib file and return its entries."""
        try:
            with open(file_path, 'r', encoding='utf-8') as bibtex_file:
                parser = bibtexparser.bparser.BibTexParser(common_strings=True)
                bib_database = bibtexparser.load(bibtex_file, parser=parser)
                # Add source file information to each entry
                for entry in bib_database.entries:
                    entry['source_file'] = str(file_path)
                self.logger.info(f"Successfully read {len(bib_database.entries)} entries from {file_path}")
                return bib_database.entries
        except Exception as e:
            self.logger.error(f"Error reading file {file_path}: {str(e)}")
            return []

    def log_duplicate(self, original_entry: Dict, duplicate_entry: Dict, method: str):
        """Log duplicate entries with detailed information."""
        duplicate_info = (
            f"\nDuplicate found by {method}:\n"
            f"  Original entry from: {original_entry.get('source_file', 'Unknown file')}\n"
            f"  Title: {original_entry.get('title', 'Unknown Title')}\n"
            f"  Authors: {original_entry.get('author', 'Unknown Authors')}\n"
            f"  Year: {original_entry.get('year', 'Unknown Year')}\n"
            f"  DOI: {original_entry.get('doi', 'No DOI')}\n"
            f"  Duplicate entry from: {duplicate_entry.get('source_file', 'Unknown file')}\n"
        )
        self.logger.info(duplicate_info)
        
        # Write to duplicates file
        with open(self.duplicate_file, 'a', encoding='utf-8') as f:
            f.write(duplicate_info)
            f.write("\nOriginal BibTeX entry:\n")
            f.write(str(original_entry))
            f.write("\n\nDuplicate BibTeX entry:\n")
            f.write(str(duplicate_entry))
            f.write("\n" + "="*50 + "\n")

    def deduplicate_entries(self, input_files: List[Path]) -> List[Dict]:
        """Process multiple .bib files and remove duplicates using multiple methods."""
        all_entries: List[Dict] = []
        duplicates_count = 0
        removed_entries = set()  # Track entries to remove
        
        # Read all input files
        for file_path in input_files:
            entries = self.read_bib_file(file_path)
            all_entries.extend(entries)
        
        # First check DOI duplicates
        doi_duplicates = self.check_doi_duplicates(all_entries)
        for original, duplicate in doi_duplicates:
            duplicates_count += 1
            removed_entries.add(id(duplicate))
            self.log_duplicate(original, duplicate, "DOI match")
        
        # Then check similarity-based duplicates
        for i in range(len(all_entries)):
            if id(all_entries[i]) in removed_entries:
                continue
            for j in range(i + 1, len(all_entries)):
                if id(all_entries[j]) in removed_entries:
                    continue
                if self.are_entries_similar(all_entries[i], all_entries[j]):
                    duplicates_count += 1
                    removed_entries.add(id(all_entries[j]))
                    self.log_duplicate(all_entries[i], all_entries[j], "Title/Author similarity")

        # Create final list of unique entries
        unique_entries = [entry for entry in all_entries if id(entry) not in removed_entries]
        
        self.logger.info(f"Found and removed {duplicates_count} duplicate entries")
        self.logger.info(f"Retained {len(unique_entries)} unique entries")
        self.logger.info(f"Detailed duplicate information written to {self.duplicate_file}")
        
        return unique_entries

    def write_output(self, entries: List[Dict], output_file: Path) -> None:
        """Write the deduplicated entries to a new .bib file."""
        try:
            # Create output directory if it doesn't exist
            output_file.parent.mkdir(exist_ok=True)
            
            db = bibtexparser.bibdatabase.BibDatabase()
            db.entries = entries
            
            with open(output_file, 'w', encoding='utf-8') as bibtex_file:
                bibtexparser.dump(db, bibtex_file)
            
            self.logger.info(f"Successfully wrote {len(entries)} entries to {output_file}")
        except Exception as e:
            self.logger.error(f"Error writing output file: {str(e)}")

def main():
    """Main function to run the deduplication process."""
    # Initialize deduplicator
    deduplicator = BibDeduplicator()
    
    # Get input files (you can modify this to accept command line arguments)
    input_directory = Path('files/Cleaned_Bib')
    input_files = list(input_directory.glob('*.bib'))
    
    if not input_files:
        deduplicator.logger.error("No .bib files found in the input directory")
        return
    
    # Process the files
    deduplicated_entries = deduplicator.deduplicate_entries(input_files)
    
    # Write output
    output_file = Path('cleaned_output/deduplicated.bib')
    deduplicator.write_output(deduplicated_entries, output_file)

if __name__ == "__main__":
    main()