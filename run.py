"""
Email Extractor — Entry Point
Run: python run.py
Open: http://localhost:5000
"""
import sys
import os
import logging

# Ensure project root is on the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)

from web import create_app

app = create_app()

if __name__ == "__main__":
    print("\n  Email Extractor is running!")
    print("  Open http://localhost:5000 in your browser\n")
    app.run(host="0.0.0.0", port=5000, debug=True, use_reloader=False)
