'use client';

import { useState, useEffect, ChangeEvent, FormEvent } from 'react';
import Link from 'next/link';

interface NoDFSFile {
  id: number;
  filename: string;
  file_hash: string;
  file_size: number;
  created_at: string;
}

const backendUrl =
  process.env.NEXT_PUBLIC_BACKEND_URL ||
  'https://your-nodfs-service.up.railway.app';

export default function NoDFSPage() {
  const [fileList, setFileList] = useState<NoDFSFile[]>([]);
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [uploading, setUploading] = useState<boolean>(false);
  const [downloadTime, setDownloadTime] = useState<number | null>(null);
  const [errorMessage, setErrorMessage] = useState<string>('');

  const fetchFiles = async () => {
    try {
      const res = await fetch(`${backendUrl}/list_nodfs`);
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
    if (e.target.files && e.target.files[0]) {
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
      const res = await fetch(`${backendUrl}/upload_nodfs`, {
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

  const handleDownload = async (file: NoDFSFile) => {
    setErrorMessage('');
    try {
      const startTime = Date.now();
      const res = await fetch(`${backendUrl}/download_nodfs?file_id=${file.id}`);
      if (!res.body) throw new Error('No response body for download');
      const blob = await res.blob();
      const endTime = Date.now();
      setDownloadTime(endTime - startTime);
      const url = window.URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = file.filename;
      document.body.appendChild(a);
      a.click();
      a.remove();
    } catch (error) {
      console.error(error);
      setErrorMessage('Download failed');
    }
  };

  const handleDelete = async (fileId: number) => {
    setErrorMessage('');
    try {
      const res = await fetch(`${backendUrl}/delete_nodfs?file_id=${fileId}`, {
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
      <h1 className="text-3xl font-bold mb-6">No-DFS Pipeline (Whole Files)</h1>
      <Link href="/" className="text-blue-500 underline">
        Switch to DFS Pipeline
      </Link>
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
            <li key={file.id} className="border p-4 mb-2 flex justify-between items-center">
              <div>
                <p className="font-medium">{file.filename}</p>
                <p className="text-sm text-gray-600">Size: {file.file_size} bytes</p>
                <p className="text-sm text-gray-600">Uploaded: {file.created_at}</p>
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
      {downloadTime !== null && (
        <div className="mt-4 text-lg">
          <p>Download completed in {downloadTime} ms</p>
        </div>
      )}
    </div>
  );
}
