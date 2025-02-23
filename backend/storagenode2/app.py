import os
from flask import Flask, request, jsonify, send_from_directory, abort
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# Set the directory where chunks will be stored.
# Use the CHUNKS_DIR environment variable if provided; otherwise default to "chunks_storage".
CHUNKS_DIR = os.getenv("CHUNKS_DIR", "chunks_storage")
os.makedirs(CHUNKS_DIR, exist_ok=True)
print("Chunks storage directory set to:", CHUNKS_DIR)

@app.route('/')
def index():
    return "Hello from Storage Node!"

# Endpoint to upload a chunk.
# Clients should POST a file (with key 'chunk') and a form field 'chunk_id'.
@app.route('/upload_chunk', methods=['POST'])
def upload_chunk():
    if 'chunk' not in request.files:
        return jsonify({'error': 'No chunk file provided'}), 400
    chunk_file = request.files['chunk']
    chunk_id = request.form.get('chunk_id')
    if not chunk_id:
        return jsonify({'error': 'Missing chunk_id'}), 400

    # Construct a filename using the chunk_id.
    filename = f"chunk_{chunk_id}"
    file_path = os.path.join(CHUNKS_DIR, filename)
    try:
        chunk_file.save(file_path)
        print(f"Uploaded chunk {chunk_id} saved at {file_path}")
        return jsonify({'message': f'Chunk {chunk_id} uploaded successfully'}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Endpoint to download a chunk.
@app.route('/download_chunk/<chunk_id>', methods=['GET'])
def download_chunk(chunk_id):
    filename = f"chunk_{chunk_id}"
    file_path = os.path.join(CHUNKS_DIR, filename)
    if not os.path.exists(file_path):
        return jsonify({'error': 'Chunk not found'}), 404
    print(f"Serving chunk {chunk_id} from {file_path}")
    return send_from_directory(CHUNKS_DIR, filename, as_attachment=True)

# Endpoint to delete a chunk.
@app.route('/delete_chunk/<chunk_id>', methods=['DELETE'])
def delete_chunk(chunk_id):
    filename = f"chunk_{chunk_id}"
    file_path = os.path.join(CHUNKS_DIR, filename)
    if not os.path.exists(file_path):
        return jsonify({'error': 'Chunk not found'}), 404
    try:
        os.remove(file_path)
        print(f"Deleted chunk {chunk_id} from {file_path}")
        return jsonify({'message': f'Chunk {chunk_id} deleted successfully'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Health-check endpoint to verify the node is up.
@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok'}), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print("Starting storage node on port", port)
    app.run(host='0.0.0.0', port=port)