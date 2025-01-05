import pandas as pd
import os
from typing import List, Dict, Tuple
from pathlib import Path


class PaperFilter:
    def __init__(self):
        # Keywords that suggest paper is about web testing
        self.web_testing_keywords = [
            'web',
            'gui testing',
            'ui test',
            'test generation',
            'selenium',
            'automated testing',
            'e2e',
            'end-to-end',
            'browser',
            'test repair',
            'locator'
        ]
        
        # Keywords that suggest paper is NOT about web testing
        self.exclusion_keywords = [
            'mobile app ',  # space added to avoid matching "mobile application testing"
            'android ',     # space added to avoid matching "android browser"
            'ios ',         # space added to avoid matching iOS-related web testing
            'hardware',
            'compiler',
            'embedded system',
            'network protocol',
            'blockchain'
        ]

    def is_relevant_paper(self, title: str) -> bool:
        """
        Check if a paper is relevant based on its title.
        
        Args:
            title (str): The title of the paper
            
        Returns:
            bool: True if the paper is relevant, False otherwise
        """
        if not isinstance(title, str):
            return False
            
        title = title.lower()
        
        # Check if any web testing keywords are present
        has_web_testing_keyword = any(
            keyword.lower() in title 
            for keyword in self.web_testing_keywords
        )
        
        # Check if any exclusion keywords are present
        has_exclusion_keyword = any(
            keyword.lower() in title 
            for keyword in self.exclusion_keywords
        )
        
        return has_web_testing_keyword and not has_exclusion_keyword

    def filter_papers(self, input_file: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Filter papers from an Excel file into relevant and excluded papers.
        
        Args:
            input_file (str): Path to the input Excel file
            
        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: Tuple of (relevant_papers, excluded_papers)
        """
        # Read the Excel file
        try:
            df = pd.read_excel(input_file)
        except Exception as e:
            print(f"Error reading file: {e}")
            return pd.DataFrame(), pd.DataFrame()

        # Check if required column exists
        if 'Item Title' not in df.columns:
            print("Error: 'Item Title' column not found in the Excel file")
            return pd.DataFrame(), pd.DataFrame()

        # Filter papers
        relevant_mask = df['Item Title'].apply(self.is_relevant_paper)
        relevant_papers = df[relevant_mask].copy()
        excluded_papers = df[~relevant_mask].copy()

        # Add relevance status
        relevant_papers['Status'] = 'Relevant'
        excluded_papers['Status'] = 'Excluded'

        return relevant_papers, excluded_papers

    def save_results(self, relevant_papers: pd.DataFrame, 
                    excluded_papers: pd.DataFrame, 
                    output_dir: str = 'output') -> None:
        """
        Save the filtered results to Excel files.
        
        Args:
            relevant_papers (pd.DataFrame): DataFrame of relevant papers
            excluded_papers (pd.DataFrame): DataFrame of excluded papers
            output_dir (str): Directory to save the output files
        """
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)

        # Save to Excel files
        relevant_papers.to_excel(
            os.path.join(output_dir, 'filtered_papers.xlsx'), 
            index=False
        )
        excluded_papers.to_excel(
            os.path.join(output_dir, 'excluded_papers.xlsx'), 
            index=False
        )

        # Print summary
        print(f"\nResults Summary:")
        print(f"Total papers: {len(relevant_papers) + len(excluded_papers)}")
        print(f"Relevant papers: {len(relevant_papers)}")
        print(f"Excluded papers: {len(excluded_papers)}")
        print(f"\nFiles saved in '{output_dir}' directory")

def main():
    # Initialize the filter
    paper_filter = PaperFilter()
    
    # Get input file
    input_file = Path('files/EXCEL/Balsam_2023_2024_Analysis.xlsx')
    
    # Process the file
    relevant_papers, excluded_papers = paper_filter.filter_papers(input_file)
    
    # Save results
    if not relevant_papers.empty or not excluded_papers.empty:
        paper_filter.save_results(relevant_papers, excluded_papers)

if __name__ == "__main__":
    main()