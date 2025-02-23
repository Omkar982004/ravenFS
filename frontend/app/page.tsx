'use client';

import { useState, useEffect, ChangeEvent, FormEvent } from 'react';
import Link from 'next/link';

interface FileRecord {
  id: number;
  filename: string;
  file_hash: string;
  file_size: number;
  total_chunks: number;
  created_at: string;
}

const backendUrl =
  process.env.NEXT_PUBLIC_BACKEND_URL ||
  'https://tender-energy-production.up.railway.app';

export default function DFSPage() {
  const [fileList, setFileList] = useState<FileRecord[]>([]);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState<boolean>(false);
  const [downloadProgress, setDownloadProgress] = useState<number[]>([]);
  const [downloadingFile, setDownloadingFile] =
    useState<FileRecord | null>(null);
  const [downloadTime, setDownloadTime] = useState<number | null>(null);
  const [errorMessage, setErrorMessage] = useState<string>('');

  // Fetch the file list from metadata service (or proxy through gateway).
  const fetchFiles = async () => {
    try {
      const res = await fetch(`${backendUrl}/list`);
      if (!res.ok) throw new Error('Failed to fetch file list');
      const data = await res.json();
      setFileList(data.files);
    } catch (error) {
      console.error(error);
      setErrorMessage('Error fetching file list');
    }
  };

  useEffect(() => {
    fetchFiles();
  }, []);

  const handleFileChange = (e: ChangeEvent<HTMLInputElement>) => {
    if (e.target.files && e.target.files.length > 0) {
      setSelectedFile(e.target.files[0]);
    }
  };

  const handleUploadSubmit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    if (!selectedFile) return;
    setUploading(true);
    setErrorMessage('');
    try {
      const formData = new FormData();
      formData.append('file', selectedFile);
      const res = await fetch(`${backendUrl}/upload`, {
        method: 'POST',
        body: formData,
      });
      if (!res.ok) throw new Error('Upload failed');
      const data = await res.json();
      console.log('Upload successful', data);
      setSelectedFile(null);
      await fetchFiles();
    } catch (error) {
      console.error(error);
      setErrorMessage('Upload failed');
    } finally {
      setUploading(false);
    }
  };

  // Download file and show parallel download progress for each chunk.
  const handleDownload = async (fileRecord: FileRecord) => {
    setDownloadingFile(fileRecord);
    const totalChunks = fileRecord.total_chunks;
    // Initialize progress for each chunk to 0%
    setDownloadProgress(new Array(totalChunks).fill(0));
    setDownloadTime(null);
    setErrorMessage('');

    const startTime = Date.now();
    try {
      const response = await fetch(
        `${backendUrl}/download?file_id=${fileRecord.id}`
      );
      if (!response.body) throw new Error('No response body for download');
      // Create a reader to read the streaming response.
      const reader = response.body.getReader();
      const chunks: Uint8Array[] = [];
      let received = 0;
      const totalSize =
        Number(response.headers.get('Content-Length')) || 1; // Fallback
      
      // Since we're downloading merged data, we simulate per-chunk progress:
      // For demonstration, we assume each chunk is roughly equal size.
      // const chunkSizeApprox = totalSize / totalChunks;

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        if (value) {
          chunks.push(value);
          received += value.length;
          // Update overall progress; here we simulate updating each chunk proportionally.
          const overallProgress = Math.min(100, (received / totalSize) * 100);
          // Distribute overall progress across chunks
          const newProgress = downloadProgress.slice();
          for (let i = 0; i < totalChunks; i++) {
            newProgress[i] = Math.min(100, overallProgress);
          }
          setDownloadProgress(newProgress);
        }
      }
      const endTime = Date.now();
      setDownloadTime(endTime - startTime);
      const mergedBuffer = new Uint8Array(received);
      let offset = 0;
      for (const chunk of chunks) {
        mergedBuffer.set(chunk, offset);
        offset += chunk.length;
      }
      const blob = new Blob([mergedBuffer]);
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = fileRecord.filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
    } catch (error) {
      console.error(error);
      setErrorMessage('Download failed');
    } finally {
      setDownloadingFile(null);
      setDownloadProgress([]);
    }
  };

  const handleDelete = async (fileId: number) => {
    setErrorMessage('');
    try {
      const res = await fetch(`${backendUrl}/delete?file_id=${fileId}`, {
        method: 'DELETE',
      });
      if (!res.ok) throw new Error('Delete failed');
      const data = await res.json();
      console.log('Delete successful', data);
      await fetchFiles();
    } catch (error) {
      console.error(error);
      setErrorMessage('Delete failed');
    }
  };

  return (
    <div className="container mx-auto p-4">
      <h1 className="text-3xl font-bold mb-6">DFS Project Frontend</h1>
      {errorMessage && (
        <div className="bg-red-200 text-red-800 p-2 mb-4">
          {errorMessage}
        </div>
      )}
      <form onSubmit={handleUploadSubmit} className="mb-6">
        <input type="file" onChange={handleFileChange} className="border p-2" />
        <button
          type="submit"
          className="ml-4 px-4 py-2 bg-blue-500 text-white rounded"
          disabled={uploading}
        >
          {uploading ? 'Uploading...' : 'Upload File'}
        </button>
      </form>
      <h2 className="text-2xl font-semibold mb-4">Uploaded Files</h2>
      {fileList.length === 0 ? (
        <p>No files uploaded yet.</p>
      ) : (
        <ul>
          {fileList.map((file) => (
            <li
              key={file.id}
              className="border p-4 mb-2 flex justify-between items-center"
            >
              <div>
                <p className="font-medium">{file.filename}</p>
                <p className="text-sm text-gray-600">
                  Size: {file.file_size} bytes
                </p>
                <p className="text-sm text-gray-600">
                  Uploaded: {file.created_at}
                </p>
              </div>
              <div className="flex space-x-4">
                <button
                  className="px-4 py-2 bg-green-500 text-white rounded"
                  onClick={() => handleDownload(file)}
                >
                  Download
                </button>
                <button
                  className="px-4 py-2 bg-red-500 text-white rounded"
                  onClick={() => handleDelete(file.id)}
                >
                  Delete
                </button>
              </div>
            </li>
          ))}
        </ul>
      )}
      {/* Parallel download progress graphics */}
      {downloadingFile && downloadProgress.length > 0 && (
        <div className="mt-6">
          <h3 className="text-xl font-semibold mb-2">
            Downloading: {downloadingFile.filename}
          </h3>
          <div className="grid grid-cols-3 gap-4">
            {downloadProgress.map((prog, idx) => (
              <div key={idx} className="border p-2 rounded shadow">
                <p className="text-sm font-medium mb-1">Chunk {idx + 1}</p>
                <div className="w-full bg-gray-300 h-4 rounded">
                  <div
                    className="bg-blue-500 h-4 rounded"
                    style={{ width: `${prog}%` }}
                  ></div>
                </div>
                <p className="text-xs text-gray-700 mt-1">{prog.toFixed(0)}%</p>
              </div>
            ))}
          </div>
        </div>
      )}
      {downloadTime !== null && (
        <div className="mt-4 text-lg">
          <p>Download completed in {downloadTime} ms</p>
        </div>
      )}
      <div className="mt-8">
        <Link href="/nodfs" className="text-blue-500 underline">
          Switch to No-DFS Pipeline
        </Link>
      </div>
    </div>
  );
}
