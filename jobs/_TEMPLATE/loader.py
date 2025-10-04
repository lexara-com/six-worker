#!/usr/bin/env python3
"""
Template Loader
Replace this with your custom loader implementation
"""
import sys
import os

# Add parent directories to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.graph_types import ProposeAPIClient


class TemplateLoader:
    """
    Template for creating new data loaders

    Replace 'Template' with your job name (e.g., MyDataSourceLoader)
    """

    def __init__(self, config: dict):
        """
        Initialize the loader

        Args:
            config: Configuration dictionary from job submission or YAML file
        """
        self.config = config
        self.connection = None  # Set by distributed worker
        self.stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0
        }

    def run(
        self,
        file_path: str = None,
        limit: int = None,
        batch_size: int = 100,
        checkpoint_callback=None,
        log_callback=None,
        error_callback=None,
        **kwargs
    ):
        """
        Execute the loading process

        Args:
            file_path: Path to input data file
            limit: Maximum number of records to process
            batch_size: Number of records per batch
            checkpoint_callback: Function to save checkpoint (optional)
            log_callback: Function to send log messages (optional)
            error_callback: Function to report errors (optional)
            **kwargs: Additional arguments

        Returns:
            dict: Statistics about the load operation
        """
        # Get configuration
        file_path = file_path or self.config.get('input', {}).get('file_path')
        limit = limit or self.config.get('processing', {}).get('limit')

        if not file_path:
            raise ValueError("file_path is required")

        # TODO: Implement your loading logic here
        print(f"Loading from: {file_path}")
        print(f"Limit: {limit}")
        print(f"Config: {self.config}")

        # Example processing loop:
        # with open(file_path, 'r') as f:
        #     for i, line in enumerate(f):
        #         if limit and i >= limit:
        #             break
        #
        #         # Process each record
        #         try:
        #             self._process_record(line)
        #             self.stats['successful'] += 1
        #         except Exception as e:
        #             self.stats['failed'] += 1
        #             if error_callback:
        #                 error_callback({
        #                     'issue_type': 'processing_error',
        #                     'message': str(e)
        #                 })
        #
        #         self.stats['total_processed'] += 1
        #
        #         # Save checkpoint
        #         if checkpoint_callback and i % batch_size == 0:
        #             checkpoint_callback({
        #                 'records_processed': self.stats['total_processed']
        #             })

        return self.stats

    def _process_record(self, record):
        """
        Process a single record

        Args:
            record: Record data to process
        """
        # TODO: Implement record processing
        pass


if __name__ == '__main__':
    # For local testing
    loader = TemplateLoader({
        'input': {
            'file_path': './data/sample.csv'
        },
        'processing': {
            'limit': 10
        }
    })

    results = loader.run()
    print(f"Results: {results}")
