import pandas as pd
import bibtexparser
import logging
from pathlib import Path
import unicodedata
import re

class Deduplicator:
    def __init__(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('logs/mf_deduplication.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def normalize_text(self, text):
        if pd.isna(text):
            return ""
        text = str(text).lower()
        text = unicodedata.normalize('NFKD', text)  # Decompose characters
        text = ''.join(c for c in text if unicodedata.category(c) != 'Mn')  # Remove non-spacing marks
        text = re.sub(r'[^\w\s]', ' ', text)  # Replace non-alphanumeric characters with spaces
        return ' '.join(text.split())


    def read_csv(self, file_path):
        try:
            df = pd.read_csv(file_path)
            self.logger.info(f"CSV columns found: {list(df.columns)}")

            entries = []
            for idx, row in df.iterrows():
                try:
                    entry = {
                        'ENTRYTYPE': 'article',
                        'ID': f'ref{idx}',
                        'title': str(row['Item Title']) if pd.notna(row['Item Title']) else '',
                        'author': str(row['Authors']) if pd.notna(row['Authors']) else '',
                        'year': str(row['Publication Year']) if pd.notna(row['Publication Year']) else '',
                        'doi': str(row['Item DOI']) if pd.notna(row['Item DOI']) else '',
                        'journal': str(row['Publication Title']) if pd.notna(row['Publication Title']) else '',
                        'volume': str(row['Journal Volume']) if pd.notna(row['Journal Volume']) else '',
                        'number': str(row['Journal Issue']) if pd.notna(row['Journal Issue']) else '',
                        'url': str(row['URL']) if pd.notna(row['URL']) else '',
                        'series': str(row['Book Series Title']) if pd.notna(row['Book Series Title']) else '',
                        'type': str(row['Content Type']) if pd.notna(row['Content Type']) else ''
                    }
                    entries.append(entry)
                except Exception as e:
                    self.logger.warning(f"Error processing row {idx}: {e}")
                    continue
                    
            self.logger.info(f"Successfully processed {len(entries)} entries from CSV")
            return entries
            
        except Exception as e:
            self.logger.error(f"Error reading CSV file {file_path}: {e}")
            self.logger.error(f"Full error: {str(e)}")
            return []
        
    def read_excel(self, file_path):
        try:
            df = pd.read_excel(file_path)
            self.logger.info(f"Excel columns found: {list(df.columns)}")

            entries = []
            for idx, row in df.iterrows():
                try:
                    entry = {
                        'ENTRYTYPE': 'article',
                        'ID': f'ref{idx}',
                        'title': str(row['Article Title']) if pd.notna(row['Article Title']) else '',
                        'author': str(row['Authors']) if pd.notna(row['Authors']) else '',
                        'year': str(row['Publication Year']) if pd.notna(row['Publication Year']) else '',
                        'doi': str(row['DOI']) if pd.notna(row['DOI']) else '',
                        'journal': str(row['Source Title']) if pd.notna(row['Source Title']) else '',
                    }
                    entries.append(entry)
                except Exception as e:
                    self.logger.warning(f"Error processing row {idx}: {e}")
                    continue
                    
            self.logger.info(f"Successfully processed {len(entries)} entries from Excel")
            return entries
            
        except Exception as e:
            self.logger.error(f"Error reading Excel file {file_path}: {e}")
            self.logger.error(f"Full error: {str(e)}")
            return []

    def read_bibtex(self, file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as bibtex_file:
                parser = bibtexparser.bparser.BibTexParser(common_strings=True)
                db = bibtexparser.load(bibtex_file, parser=parser)
                self.logger.info(f"Successfully processed {len(db.entries)} entries from Bibtex")
                return db.entries
        except Exception as e:
            self.logger.error(f"Error reading BibTeX file {file_path}: {e}")
            return []

    def are_duplicates(self, entry1, entry2):
        # If DOIs exist and match
        if entry1.get('doi') and entry2.get('doi'):
            if entry1['doi'].strip() == entry2['doi'].strip():
                return True

        # Compare normalized titles
        title1 = self.normalize_text(entry1.get('title', ''))
        title2 = self.normalize_text(entry2.get('title', ''))
        if title1 and title2 and title1 == title2:
            return True
            
        return False

    def process_files(self, input_files):
        all_entries = []
        unique_entries = []
        duplicates = []

        for file_path in input_files:
            if file_path.suffix.lower() == '.bib':
                entries = self.read_bibtex(file_path)
            elif file_path.suffix.lower() == '.csv':
                entries = self.read_csv(file_path)
            elif file_path.suffix.lower() == '.xlsx':
                entries = self.read_excel(file_path)
            else:
                self.logger.warning(f"Unsupported file type: {file_path}")
                continue

            if entries:
                all_entries.extend(entries)

        for entry in all_entries:
            is_duplicate = False
            for unique_entry in unique_entries:
                if self.are_duplicates(entry, unique_entry):
                    duplicates.append((unique_entry, entry))
                    is_duplicate = True
                    break
            if not is_duplicate:
                unique_entries.append(entry)

        self.logger.info(f"Found {len(duplicates)} duplicates")
        self.logger.info(f"Kept {len(unique_entries)} unique entries")

        return unique_entries, duplicates

    def export_bibtex(self, entries, output_file):
        try:
            # Ensure required fields
            for entry in entries:
                if 'ENTRYTYPE' not in entry:
                    entry['ENTRYTYPE'] = 'article'
                if 'ID' not in entry:
                    entry['ID'] = 'ref_' + self.normalize_text(entry.get('title', ''))[:30]

            db = bibtexparser.bibdatabase.BibDatabase()
            db.entries = entries

            with open(output_file, 'w', encoding='utf-8') as f:
                writer = bibtexparser.bwriter.BibTexWriter()
                f.write(writer.write(db))
                
            self.logger.info(f"Successfully exported {len(entries)} entries to BibTeX")
            
        except Exception as e:
            self.logger.error(f"Error exporting to BibTeX: {e}")

    def export_csv(self, entries, output_file):
        try:
            df = pd.DataFrame([{
                'Item Title': entry.get('title', ''),
                'Publication Title': entry.get('journal', ''),
                'Book Series Title': entry.get('series', ''),
                'Journal Volume': entry.get('volume', ''),
                'Journal Issue': entry.get('number', ''),
                'Item DOI': entry.get('doi', ''),
                'Authors': entry.get('author', ''),
                'Publication Year': entry.get('year', ''),
                'URL': entry.get('url', ''),
                'Content Type': entry.get('type', '')
            } for entry in entries])
            
            df.to_csv(output_file, index=False)
            self.logger.info(f"Successfully exported {len(entries)} entries to CSV")
            
        except Exception as e:
            self.logger.error(f"Error exporting to CSV: {e}")

def main():
    deduplicator = Deduplicator()
    
    input_dir = Path('cleaned_files/All_Cleaned')
    output_dir = Path('output')
    output_dir.mkdir(exist_ok=True)
    
    input_files = []
    input_files.extend(input_dir.glob('*.bib'))
    input_files.extend(input_dir.glob('*.csv'))
    input_files.extend(input_dir.glob('*.xlsx'))
    
    if not input_files:
        deduplicator.logger.error("No input files found")
        return
    
    unique_entries, duplicates = deduplicator.process_files(input_files)
    
    deduplicator.export_csv(unique_entries, output_dir / 'deduplicated.csv')
    
    with open(output_dir / 'duplicates.txt', 'w', encoding='utf-8') as f:
        for orig, dup in duplicates:
            f.write(f"\nDuplicate found:\n")
            f.write(f"Title: {orig.get('title', '')}\n")
            f.write(f"Authors: {orig.get('author', '')}\n")
            f.write(f"DOI: {orig.get('doi', '')}\n")
            f.write("="*50 + "\n")

if __name__ == "__main__":
    main()