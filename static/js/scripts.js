document.getElementById('csvForm').addEventListener('submit', function (e) {
    e.preventDefault();

    const fileInput = document.getElementById('csvFile');
    const file = fileInput.files[0];
    const messageDiv = document.getElementById('message');

    if (!file) {
        messageDiv.textContent = '❌ Please select a file.';
        return;
    }

    messageDiv.textContent = '⏳ Processing your file...';

    const formData = new FormData();
    formData.append('file', file);

    fetch('/process-csv', {
        method: 'POST',
        body: formData
    })
    .then(response => {
        if (!response.ok) {
            throw new Error('❌ Error processing file.');
        }

        // Get Content-Disposition header
        const contentDisposition = response.headers.get('Content-Disposition');
        if (!contentDisposition) {
            console.warn("⚠ No 'Content-Disposition' header found in response.");
            return response.blob().then(blob => {
                const defaultFilename = file.name.replace(/\.[^/.]+$/, "") + "_processed.csv"; // Default filename
                downloadFile(blob, defaultFilename);
            });
        }

        // Extract filename from Content-Disposition
        const filenameMatch = contentDisposition.match(/filename="(.+)"/);
        const filename = (filenameMatch && filenameMatch[1]) ? filenameMatch[1] : "processed_file.csv";

        return response.blob().then(blob => downloadFile(blob, filename));
    })
    .catch(error => {
        console.error("File Processing Error:", error);
        messageDiv.textContent = '❌ Error: ' + error.message;
    });
});

// ✅ Function to download file
function downloadFile(blob, filename) {
    const link = document.createElement('a');
    link.href = URL.createObjectURL(blob);
    link.download = filename;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    document.getElementById('message').textContent = '✅ File processed and downloaded!';
}
