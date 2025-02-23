import os
import hashlib
from flask import Flask, request, jsonify, abort, Response
from flask_cors import CORS
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

app = Flask(__name__)
CORS(app)

# Environment variables
# Set the metadata service URL to your provided URL.
METADATA_URL = os.getenv("METADATA_URL", "https://ravenfs-production.up.railway.app")
# STORAGE_NODES should be a comma-separated list of storage node URLs.
storage_nodes_str = os.getenv("STORAGE_NODES", "celebrated-radiance-production.up.railway.app,celebrated-radiance-copy-production.up.railway.app,celebrated-radiance-copy-1-production.up.railway.app")
STORAGE_NODES = [node.strip() for node in storage_nodes_str.split(",") if node.strip()]

# Use 4 MB chunks
CHUNK_SIZE = 4 * 1024 * 1024

@app.route('/')
def index():
    return "Welcome to the DFS Gateway!"

# Helper: Split file data into chunks of given size.
def split_file(file_data: bytes, chunk_size: int = CHUNK_SIZE):
    return [file_data[i:i+chunk_size] for i in range(0, len(file_data), chunk_size)]

# Helper: Upload a single chunk to a storage node.
def upload_chunk(storage_node: str, chunk_id: str, chunk_data: bytes):
    files = {'chunk': (chunk_id, chunk_data)}
    data = {'chunk_id': chunk_id}
    try:
        response = requests.post(f"{storage_node}/upload_chunk", data=data, files=files, timeout=60)
        response.raise_for_status()
        return True
    except Exception as e:
        print(f"Error uploading {chunk_id} to {storage_node}: {e}")
        return False

# Helper: Replicate a chunk to all storage nodes.
def replicate_chunk(chunk_id: str, chunk_data: bytes):
    successful_nodes = []
    with ThreadPoolExecutor(max_workers=len(STORAGE_NODES)) as executor:
        futures = {executor.submit(upload_chunk, node, chunk_id, chunk_data): node for node in STORAGE_NODES}
        for future in as_completed(futures):
            node = futures[future]
            try:
                if future.result():
                    successful_nodes.append(node)
            except Exception as e:
                print(f"Exception for node {node}: {e}")
    return successful_nodes

# Helper: Download a chunk from a single storage node.
def download_chunk_from_storage(storage_node: str, chunk_id: str):
    try:
        response = requests.get(f"{storage_node}/download_chunk/{chunk_id}", timeout=60)
        response.raise_for_status()
        return response.content
    except Exception as e:
        print(f"Error downloading chunk {chunk_id} from {storage_node}: {e}")
        return None

# Helper: Download a replicated chunk from multiple nodes concurrently.
def download_replicated_chunk(storage_nodes_str: str, chunk_id: str):
    nodes = [node.strip() for node in storage_nodes_str.split(",") if node.strip()]
    with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
        futures = {executor.submit(download_chunk_from_storage, node, chunk_id): node for node in nodes}
        for future in as_completed(futures):
            data = future.result()
            if data is not None:
                return data
    return None

# Helper: Delete a chunk from a single storage node.
def delete_chunk_from_storage(storage_node: str, chunk_id: str):
    try:
        response = requests.delete(f"{storage_node}/delete_chunk/{chunk_id}", timeout=60)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"Error deleting chunk {chunk_id} from {storage_node}: {e}")
        return {"error": str(e)}

# Helper: Delete a replicated chunk from all nodes concurrently.
def delete_replicated_chunk(storage_nodes_str: str, chunk_id: str):
    nodes = [node.strip() for node in storage_nodes_str.split(",") if node.strip()]
    results = {}
    with ThreadPoolExecutor(max_workers=len(nodes)) as executor:
        futures = {executor.submit(delete_chunk_from_storage, node, chunk_id): node for node in nodes}
        for future in as_completed(futures):
            node = futures[future]
            try:
                results[node] = future.result()
            except Exception as e:
                results[node] = {"error": str(e)}
    return results

# Upload endpoint: orchestrates file upload, chunking, replication, and metadata registration.
@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No file selected'}), 400
    filename = file.filename
    file_data = file.read()
    file_size = len(file_data)
    file_hash = hashlib.sha256(file_data).hexdigest()

    # Split file into 4MB chunks.
    chunks = split_file(file_data, CHUNK_SIZE)
    total_chunks = len(chunks)
    chunk_metadata = []
    with ThreadPoolExecutor(max_workers=total_chunks) as executor:
        futures = {}
        for i, chunk in enumerate(chunks, start=1):
            chunk_hash = hashlib.sha256(chunk).hexdigest()
            chunk_id = f"{filename}_chunk{i}"
            futures[i] = executor.submit(replicate_chunk, chunk_id, chunk)
            chunk_metadata.append({
                'chunk_order': i,
                # We will join the list of nodes where this chunk is replicated.
                'storage_nodes': None,
                'chunk_hash': chunk_hash
            })
        for i in futures:
            successful_nodes = futures[i].result()
            chunk_metadata[i-1]['storage_nodes'] = ",".join(successful_nodes)

    # Register file metadata with the metadata service.
    meta_payload = {
        'filename': filename,
        'file_hash': file_hash,
        'file_size': file_size,
        'total_chunks': total_chunks
    }
    meta_resp = requests.post(f"{METADATA_URL}/files", json=meta_payload, timeout=60)
    meta_resp.raise_for_status()
    meta_data = meta_resp.json()
    file_id = meta_data.get('id')

    # Register chunk mappings with the metadata service.
    chunks_payload = {
        'chunks': chunk_metadata
    }
    chunks_resp = requests.post(f"{METADATA_URL}/files/{file_id}/chunks", json=chunks_payload, timeout=60)
    chunks_resp.raise_for_status()

    return jsonify({
        'message': 'File uploaded, chunked, and replicated successfully',
        'file_id': file_id,
        'filename': filename,
        'file_hash': file_hash,
        'file_size': file_size,
        'total_chunks': total_chunks,
        'chunk_metadata': chunk_metadata
    }), 201

# Download endpoint: retrieves file metadata, downloads chunks in parallel, merges them, and streams the file.
@app.route('/download', methods=['GET'])
def download_file():
    file_id = request.args.get('file_id')
    if not file_id:
        return jsonify({'error': 'Missing file_id parameter'}), 400

    # Retrieve file metadata (including chunk mappings) from metadata service.
    meta_resp = requests.get(f"{METADATA_URL}/files/{file_id}", timeout=60)
    meta_resp.raise_for_status()
    meta_data = meta_resp.json()
    filename = meta_data.get('filename')
    total_chunks = meta_data.get('total_chunks')
    chunks_info = meta_data.get('chunks')

    downloaded_chunks = [None] * total_chunks
    with ThreadPoolExecutor(max_workers=total_chunks) as executor:
        futures = {}
        for chunk in chunks_info:
            order = chunk.get('chunk_order')
            # storage_nodes is a comma-separated string.
            storage_nodes = chunk.get('storage_nodes')
            chunk_id = f"{filename}_chunk{order}"
            futures[order] = executor.submit(download_replicated_chunk, storage_nodes, chunk_id)
        for i in range(1, total_chunks + 1):
            data = futures[i].result()
            if data is None:
                return jsonify({'error': f'Failed to download chunk {i}'}), 500
            downloaded_chunks[i - 1] = data

    merged_data = b''.join(downloaded_chunks)
    return Response(merged_data, mimetype='application/octet-stream', headers={
        'Content-Disposition': f'attachment; filename="{filename}"'
    })

# Delete endpoint: deletes file metadata and instructs all storage nodes to delete the replicated chunks.
@app.route('/delete', methods=['DELETE'])
def delete_file():
    file_id = request.args.get('file_id')
    if not file_id:
        return jsonify({'error': 'Missing file_id parameter'}), 400

    # Retrieve metadata from the metadata service.
    meta_resp = requests.get(f"{METADATA_URL}/files/{file_id}", timeout=60)
    if meta_resp.status_code != 200:
        return jsonify({'error': 'File not found in metadata service'}), 404
    meta_data = meta_resp.json()
    filename = meta_data.get('filename')
    chunks_info = meta_data.get('chunks')

    delete_results = {}
    with ThreadPoolExecutor(max_workers=len(chunks_info)) as executor:
        futures = {}
        for chunk in chunks_info:
            order = chunk.get('chunk_order')
            storage_nodes = chunk.get('storage_nodes')
            chunk_id = f"{filename}_chunk{order}"
            futures[order] = executor.submit(delete_replicated_chunk, storage_nodes, chunk_id)
        for i in futures:
            delete_results[i] = futures[i].result()

    # Delete metadata from the metadata service.
    del_resp = requests.delete(f"{METADATA_URL}/files/{file_id}", timeout=60)
    if del_resp.status_code not in (200, 204):
        return jsonify({'error': 'Failed to delete metadata'}), 500

    return jsonify({
        'message': f'File {file_id} deleted successfully',
        'delete_results': delete_results
    }), 200

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    print("Starting main DFS gateway on port", port)
    app.run(host='0.0.0.0', port=port)
