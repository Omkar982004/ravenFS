import os
import sqlite3
from flask import Flask, request, jsonify, abort
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Use Railway's persistent directory; default to /data if not set.
PERSISTENT_DIR = os.getenv("RAILWAY_PERSISTENT_DIR", "/data")
os.makedirs(PERSISTENT_DIR, exist_ok=True)  # Ensure the persistent directory exists
print("Persistent storage directory set to:", PERSISTENT_DIR)

# Database path inside persistent storage.
DB_PATH = os.path.join(PERSISTENT_DIR, "file_chunks.db")
print("Database path set to:", DB_PATH)

# Define an assets directory inside persistent storage.
ASSETS_DIR = os.path.join(PERSISTENT_DIR, "assets")
os.makedirs(ASSETS_DIR, exist_ok=True)
print("Assets directory set to:", ASSETS_DIR)

# Use ASSETS_DIR to store file-related assets.
UPLOAD_ROOT = os.path.join(ASSETS_DIR, "chunks")
TMP_FOLDER = os.path.join(UPLOAD_ROOT, "tmp")
# For our DFS prototype, we assume three storage folders.
FOLDERS = ['folder1', 'folder2', 'folder3']
# For no-DFS storage (whole file)
NODFS_FOLDER = os.path.join(UPLOAD_ROOT, "nodfs")
CHUNK_SIZE = 1024 * 1024  # 1 MB

# Ensure directories exist.
os.makedirs(TMP_FOLDER, exist_ok=True)
os.makedirs(NODFS_FOLDER, exist_ok=True)
for folder in FOLDERS:
    os.makedirs(os.path.join(UPLOAD_ROOT, folder), exist_ok=True)

print("Upload root set to:", UPLOAD_ROOT)
print("Temporary folder set to:", TMP_FOLDER)
print("No-DFS folder set to:", NODFS_FOLDER)
for folder in FOLDERS:
    print(f"DFS folder '{folder}' is set up at:", os.path.join(UPLOAD_ROOT, folder))

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # Files table (metadata service for DFS)
    c.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            file_hash TEXT,
            file_size INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            total_chunks INTEGER
        );
    """)
    # Chunks table (mapping of file to its chunks and storage nodes)
    c.execute("""
        CREATE TABLE IF NOT EXISTS chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER,
            chunk_order INTEGER,
            storage_nodes TEXT, -- e.g., comma-separated list of storage node IDs
            chunk_hash TEXT,
            FOREIGN KEY(file_id) REFERENCES files(id)
        );
    """)
    # Table for whole-file storage (No-DFS)
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
    print("Database initialized (tables created if not existing).")

init_db()

# --- Endpoints for Metadata Service ---

# Register a new file (metadata only)
@app.route('/files', methods=['POST'])
def register_file():
    data = request.get_json()
    if not data or 'filename' not in data:
        return jsonify({'error': 'filename required'}), 400
    filename = data['filename']
    file_hash = data.get('file_hash', '')
    file_size = data.get('file_size', 0)
    total_chunks = data.get('total_chunks', 0)
    
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("INSERT INTO files (filename, file_hash, file_size, total_chunks) VALUES (?, ?, ?, ?)",
              (filename, file_hash, file_size, total_chunks))
    file_id = c.lastrowid
    conn.commit()
    conn.close()
    return jsonify({'id': file_id, 'filename': filename, 'file_hash': file_hash, 'file_size': file_size, 'total_chunks': total_chunks}), 201

# Add chunk mappings for a file
@app.route('/files/<int:file_id>/chunks', methods=['POST'])
def add_chunks(file_id):
    data = request.get_json()
    if not data or 'chunks' not in data:
        return jsonify({'error': 'chunks list required'}), 400
    chunks = data['chunks']
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    for chunk in chunks:
        chunk_order = chunk.get('chunk_order')
        storage_nodes = chunk.get('storage_nodes', '')
        chunk_hash = chunk.get('chunk_hash', '')
        c.execute("INSERT INTO chunks (file_id, chunk_order, storage_nodes, chunk_hash) VALUES (?, ?, ?, ?)",
                  (file_id, chunk_order, storage_nodes, chunk_hash))
    conn.commit()
    conn.close()
    return jsonify({'message': 'Chunks added successfully'}), 201

# List all files
@app.route('/files', methods=['GET'])
def list_files():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, filename, file_hash, file_size, created_at, total_chunks FROM files")
    rows = c.fetchall()
    conn.close()
    files = []
    for row in rows:
        files.append({
            'id': row[0],
            'filename': row[1],
            'file_hash': row[2],
            'file_size': row[3],
            'created_at': row[4],
            'total_chunks': row[5]
        })
    return jsonify({'files': files})

# Get details for a specific file (metadata + chunks)
@app.route('/files/<int:file_id>', methods=['GET'])
def get_file(file_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, filename, file_hash, file_size, created_at, total_chunks FROM files WHERE id=?", (file_id,))
    file_row = c.fetchone()
    if not file_row:
        conn.close()
        abort(404, description="File not found")
    c.execute("SELECT chunk_order, storage_nodes, chunk_hash FROM chunks WHERE file_id=? ORDER BY chunk_order", (file_id,))
    chunk_rows = c.fetchall()
    conn.close()
    chunks = [{'chunk_order': row[0], 'storage_nodes': row[1], 'chunk_hash': row[2]} for row in chunk_rows]
    file_info = {
        'id': file_row[0],
        'filename': file_row[1],
        'file_hash': file_row[2],
        'file_size': file_row[3],
        'created_at': file_row[4],
        'total_chunks': file_row[5],
        'chunks': chunks
    }
    return jsonify(file_info)

# Delete a file and its metadata (DFS or No-DFS can be implemented similarly)
@app.route('/files/<int:file_id>', methods=['DELETE'])
def delete_file(file_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # Delete chunk files from DFS storage
    c.execute("SELECT filename, total_chunks FROM files WHERE id=?", (file_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'File not found'}), 404
    filename, total_chunks = row
    base_name, extension = os.path.splitext(filename)
    errors = []
    # Delete chunk files from each DFS folder
    for order in range(1, total_chunks + 1):
        chunk_filename = f"{base_name}_chunk{order}{extension}"
        for folder in ['folder1', 'folder2', 'folder3']:
            folder_path = os.path.join(UPLOAD_ROOT, folder)
            chunk_path = os.path.join(folder_path, chunk_filename)
            try:
                if os.path.exists(chunk_path):
                    os.remove(chunk_path)
                    print(f"Deleted {chunk_path}")
            except Exception as e:
                errors.append(f"Error deleting {chunk_path}: {e}")
    # Delete metadata from database
    c.execute("DELETE FROM chunks WHERE file_id=?", (file_id,))
    c.execute("DELETE FROM files WHERE id=?", (file_id,))
    conn.commit()
    conn.close()
    if errors:
        return jsonify({'data': 'Deletion partially completed', 'errors': errors}), 207
    return jsonify({'message': f'File {file_id} and its metadata deleted successfully'}), 200

# (Additional endpoints for No-DFS are similar but operate on files_nodfs)

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print("Starting metadata service on port", port)
    app.run(host='0.0.0.0', port=port)
