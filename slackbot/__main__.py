"""
Entry point for running the Databricks Slack App as a module.

Usage:
    python -m slackbot
"""

if __name__ == "__main__":
    # Import and run main function
    from .utils import main
    main() 