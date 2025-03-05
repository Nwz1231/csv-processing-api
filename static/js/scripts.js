document.getElementById('csvForm').addEventListener('submit', function(e) {
  e.preventDefault();

  const fileInput = document.getElementById('csvFile');
  const file = fileInput.files[0];
  const messageDiv = document.getElementById('message');
  
  if (!file) {
    messageDiv.textContent = 'Please select a file.';
    return;
  }

  messageDiv.textContent = 'Processing your file...';

  const formData = new FormData();
  formData.append('file', file);

  fetch('/process-csv', {
  method: 'POST',
  body: formData
})
.then(response => {
  if (!response.ok) {
    throw new Error('Error processing file.');
  }

  const contentDisposition = response.headers.get('Content-Disposition');
  
  if (!contentDisposition) {
    throw new Error("No filename found in response headers.");
  }

  const filenameMatch = contentDisposition.match(/filename="(.+)"/);
  if (!filenameMatch || !filenameMatch[1]) {
    throw new Error("Filename parsing error.");
  }

  const filename = filenameMatch[1];

  return response.blob().then(blob => {
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = filename;
    link.click();
    messageDiv.textContent = 'File processed and downloaded!';
  });
})
.catch(error => {
  messageDiv.textContent = 'Error: ' + error.message;
});

