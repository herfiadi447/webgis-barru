import sys
import os

# Tambahkan root project ke path agar bisa import app.py
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app import app

# Vercel membutuhkan variabel bernama 'app' atau 'handler'
# Flask app sudah bernama 'app', jadi langsung diexpose
