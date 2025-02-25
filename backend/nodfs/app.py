import os
import sqlite3
import hashlib
from flask import Flask, request, jsonify, send_from_directory, abort
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Use Railway's persistent directory; default to /data if not set.
PERSISTENT_DIR = os.getenv("RAILWAY_PERSISTENT_DIR", "/data")
os.makedirs(PERSISTENT_DIR, exist_ok=True)
print("Persistent storage directory set to:", PERSISTENT_DIR)

# Define directory for storing whole files.
NODFS_DIR = os.path.join(PERSISTENT_DIR, "nodfs")
os.makedirs(NODFS_DIR, exist_ok=True)
print("No-DFS storage directory set to:", NODFS_DIR)

# Database path for metadata.
DB_PATH = os.path.join(PERSISTENT_DIR, "nodfs_metadata.db")
print("No-DFS metadata DB path set to:", DB_PATH)

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS files_nodfs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            file_hash TEXT,
            file_size INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    conn.close()
    print("No-DFS metadata DB initialized.")

init_db()

@app.route('/')
def index():
    return "Hello from No-DFS Minimal Service!"

# Upload endpoint: stores the entire file and records metadata.
@app.route('/upload_nodfs', methods=['POST'])
def upload_nodfs():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    filename = file.filename
    file_path = os.path.join(NODFS_DIR, filename)
    try:
        file.save(file_path)
        # Compute file hash.
        sha = hashlib.sha256()
        file_size = os.path.getsize(file_path)
        with open(file_path, 'rb') as f:
            while True:
                data = f.read(4096)
                if not data:
                    break
                sha.update(data)
        file_hash = sha.hexdigest()
        # Insert metadata into DB.
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        c.execute("INSERT INTO files_nodfs (filename, file_hash, file_size) VALUES (?, ?, ?)",
                  (filename, file_hash, file_size))
        file_id = c.lastrowid
        conn.commit()
        conn.close()
        print(f"Uploaded {filename} with id {file_id}")
        return jsonify({'message': 'File uploaded successfully (No-DFS)!', 'file_id': file_id}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# List endpoint: returns all files stored.
@app.route('/list_nodfs', methods=['GET'])
def list_nodfs():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, filename, file_hash, file_size, created_at FROM files_nodfs")
    rows = c.fetchall()
    conn.close()
    files = [{
        'id': row[0],
        'filename': row[1],
        'file_hash': row[2],
        'file_size': row[3],
        'created_at': row[4]
    } for row in rows]
    return jsonify({'files': files})

# Download endpoint: streams the entire file.
@app.route('/download_nodfs', methods=['GET'])
def download_nodfs():
    file_id = request.args.get('file_id')
    if not file_id:
        return jsonify({'error': 'Missing file_id parameter'}), 400
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT filename FROM files_nodfs WHERE id=?", (file_id,))
    row = c.fetchone()
    conn.close()
    if not row:
        return jsonify({'error': 'File not found'}), 404
    filename = row[0]
    return send_from_directory(NODFS_DIR, filename, as_attachment=True)

# Delete endpoint: deletes the file from disk and removes metadata.
@app.route('/delete_nodfs', methods=['DELETE'])
def delete_nodfs():
    file_id = request.args.get('file_id')
    if not file_id:
        return jsonify({'error': 'Missing file_id parameter'}), 400
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT filename FROM files_nodfs WHERE id=?", (file_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'File not found'}), 404
    filename = row[0]
    file_path = os.path.join(NODFS_DIR, filename)
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Deleted file at {file_path}")
    except Exception as e:
        conn.close()
        return jsonify({'error': f"Error deleting file: {str(e)}"}), 500
    c.execute("DELETE FROM files_nodfs WHERE id=?", (file_id,))
    conn.commit()
    conn.close()
    return jsonify({'message': f'File {file_id} deleted successfully'}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print("Starting No-DFS Minimal service on port", port)
    app.run(host='0.0.0.0', port=port)
