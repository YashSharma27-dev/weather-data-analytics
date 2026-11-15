"""
Weather Analytics Dashboard Entry Point
Redirects or executes dashboard/app.py for team synchronization.
"""

import os
import sys

# Add parent directory to path
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

dashboard_script = os.path.join(parent_dir, "dashboard", "app.py")

with open(dashboard_script, "r", encoding="utf-8") as f:
    code = f.read()

exec(code)
