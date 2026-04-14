"""
Email Extractor — Entry Point
Run: python run.py
Open: http://localhost:5000
"""
import sys
import os

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from logging_setup import setup_logging

from web import create_app

setup_logging()
app = create_app()

if __name__ == "__main__":
    print("\n  Email Extractor is running!")
    print("  Open http://localhost:5000 in your browser\n")
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
