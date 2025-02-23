import os
import sqlite3
from flask import Flask, request, jsonify, abort
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Use Railway's persistent directory; default to /data if not set.
PERSISTENT_DIR = os.getenv("RAILWAY_PERSISTENT_DIR", "/data")
os.makedirs(PERSISTENT_DIR, exist_ok=True)
print("Persistent storage directory set to:", PERSISTENT_DIR)

# Database path inside persistent storage.
DB_PATH = os.path.join(PERSISTENT_DIR, "metadata.db")
print("Database path set to:", DB_PATH)

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    # Files table for metadata service.
    c.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            filename TEXT NOT NULL,
            file_hash TEXT,
            file_size INTEGER,
            total_chunks INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        );
    """)
    # Chunks table for mapping file chunks.
    c.execute("""
        CREATE TABLE IF NOT EXISTS chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            file_id INTEGER,
            chunk_order INTEGER,
            storage_nodes TEXT,
            chunk_hash TEXT,
            FOREIGN KEY(file_id) REFERENCES files(id)
        );
    """)
    conn.commit()
    conn.close()
    print("Database initialized (tables 'files' and 'chunks' created if not existing).")

init_db()

@app.route('/')
def index():
    return "Hello from the Metadata Service!"

# Endpoint to register a new file (metadata only)
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
    
    return jsonify({
        'id': file_id,
        'filename': filename,
        'file_hash': file_hash,
        'file_size': file_size,
        'total_chunks': total_chunks
    }), 201

# Endpoint to add chunk mappings for a file
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

# Endpoint to list all files
@app.route('/files', methods=['GET'])
def list_files():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, filename, file_hash, file_size, total_chunks, created_at FROM files")
    rows = c.fetchall()
    conn.close()
    files = [{
        'id': row[0],
        'filename': row[1],
        'file_hash': row[2],
        'file_size': row[3],
        'total_chunks': row[4],
        'created_at': row[5]
    } for row in rows]
    return jsonify({'files': files})

# Endpoint to get details for a specific file (including chunk mappings)
@app.route('/files/<int:file_id>', methods=['GET'])
def get_file(file_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id, filename, file_hash, file_size, total_chunks, created_at FROM files WHERE id=?", (file_id,))
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
        'total_chunks': file_row[4],
        'created_at': file_row[5],
        'chunks': chunks
    }
    return jsonify(file_info)

# Endpoint to delete a file and its metadata
@app.route('/files/<int:file_id>', methods=['DELETE'])
def delete_file(file_id):
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute("SELECT id FROM files WHERE id=?", (file_id,))
    row = c.fetchone()
    if not row:
        conn.close()
        return jsonify({'error': 'File not found'}), 404
    c.execute("DELETE FROM chunks WHERE file_id=?", (file_id,))
    c.execute("DELETE FROM files WHERE id=?", (file_id,))
    conn.commit()
    conn.close()
    return jsonify({'message': f'File {file_id} and its metadata deleted successfully'}), 200

# --- New Endpoint: View Entire Database ---
@app.route('/db_view', methods=['GET'])
def db_view():
    """
    Returns the entire contents of the metadata database for debugging or administrative purposes.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute("SELECT * FROM files")
    files = [dict(row) for row in c.fetchall()]
    c.execute("SELECT * FROM chunks")
    chunks = [dict(row) for row in c.fetchall()]
    conn.close()
    return jsonify({'files': files, 'chunks': chunks})

print("Metadata service loaded and ready!")

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print("Starting metadata service on port", port)
    app.run(host='0.0.0.0', port=port)
