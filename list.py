import bibtexparser
from pathlib import Path
from typing import List, Dict, Optional
import logging
import textwrap

class BibContentExtractor:
    def __init__(self):
        """Initialize the content extractor with basic logging."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)

    def clean_text(self, text: str) -> str:
        """Clean text by removing extra spaces and newlines."""
        if not text:
            return ""
        return ' '.join(text.split())

    def extract_content(self, bib_file_path: str) -> List[Dict[str, str]]:
        """
        Extract titles and abstracts from a bib file.
        
        Args:
            bib_file_path (str): Path to the bib file
            
        Returns:
            List[Dict[str, str]]: List of dictionaries containing paper info
        """
        try:
            # Read the bib file
            with open(bib_file_path, 'r', encoding='utf-8') as bibtex_file:
                parser = bibtexparser.bparser.BibTexParser(common_strings=True)
                bib_database = bibtexparser.load(bibtex_file, parser=parser)
            
            # Extract content
            papers = []
            for entry in bib_database.entries:
                paper = {
                    'id': entry.get('ID', 'Unknown ID'),
                    'title': self.clean_text(entry.get('title', '')),
                    'abstract': self.clean_text(entry.get('abstract', '')),
                    'year': entry.get('year', 'N/A'),
                    'author': entry.get('author', 'N/A')
                }
                papers.append(paper)
            
            # Sort by year and then title
            papers.sort(key=lambda x: (x['year'], x['title']))
            
            self.logger.info(f"Successfully extracted content from {len(papers)} papers")
            return papers
            
        except FileNotFoundError:
            self.logger.error(f"File not found: {bib_file_path}")
            return []
        except Exception as e:
            self.logger.error(f"Error processing file: {str(e)}")
            return []

    def save_content_to_file(self, papers: List[Dict[str, str]], output_file: str, wrap_width: int = 80) -> None:
        """
        Save the extracted content to a formatted text file.
        
        Args:
            papers (List[Dict[str, str]]): List of paper information
            output_file (str): Path to the output file
            wrap_width (int): Width for text wrapping (default: 80)
        """
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                for i, paper in enumerate(papers, 1):
                    # Format paper information with nice formatting and text wrapping
                    f.write(f"Paper {i}\n")
                    f.write("="*wrap_width + "\n\n")
                    
                    # Title
                    f.write("Title:\n")
                    wrapped_title = textwrap.fill(paper['title'], width=wrap_width, 
                                                initial_indent="  ", subsequent_indent="  ")
                    f.write(f"{wrapped_title}\n\n")
                    
                    # Year and Author
                    f.write(f"Year: {paper['year']}\n")
                    f.write(f"Authors: {paper['author']}\n\n")
                    
                    # Abstract
                    f.write("Abstract:\n")
                    if paper['abstract']:
                        wrapped_abstract = textwrap.fill(paper['abstract'], width=wrap_width, 
                                                       initial_indent="  ", subsequent_indent="  ")
                        f.write(f"{wrapped_abstract}\n")
                    else:
                        f.write("  [No abstract available]\n")
                    
                    # Separator between papers
                    f.write("\n" + "-"*wrap_width + "\n\n")
            
            self.logger.info(f"Successfully saved content to {output_file}")
        except Exception as e:
            self.logger.error(f"Error saving content to file: {str(e)}")

    def generate_summary(self, papers: List[Dict[str, str]], output_file: Optional[str] = None) -> str:
        """Generate a summary of the papers."""
        total_papers = len(papers)
        papers_with_abstract = sum(1 for p in papers if p['abstract'])
        years = sorted(set(p['year'] for p in papers if p['year'] != 'N/A'))
        
        summary = f"""
Summary of Extracted Papers
==========================
Total number of papers: {total_papers}
Papers with abstracts: {papers_with_abstract}
Year range: {years[0]} - {years[-1] if years else 'N/A'}
        """
        
        if output_file:
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(summary)
                
        return summary

def main():
    # Initialize extractor
    extractor = BibContentExtractor()
    
    # Process a bib file
    bib_file = "files/SD/SD_Web_Application_Testing.bib"
    papers = extractor.extract_content(bib_file)
    
    # Save detailed content
    extractor.save_content_to_file(papers, "output/sd_paper_contents.txt")
    
    # Generate and print summary
    summary = extractor.generate_summary(papers, "output/sd_summary.txt")
    print(summary)

if __name__ == "__main__":
    main()