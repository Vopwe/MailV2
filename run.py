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
    debug_mode = os.getenv("FLASK_DEBUG", "0") == "1"
    host = os.getenv("FLASK_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_PORT", "5000"))
    print("\n  Email Extractor is running!")
    print(f"  Open http://localhost:{port} in your browser\n")
    if debug_mode:
        print("  ⚠ DEBUG MODE ON — do not expose this publicly\n")
    app.run(host=host, port=port, debug=debug_mode, use_reloader=False)
