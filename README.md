**Analyzing PDFs using Benford's Law**

Key Features

This project is designed to be:

    Serverless: Leveraging AWS Lambda to handle tasks without the need to manage servers.
    Asynchronous: Tasks execute independently, ensuring scalability and efficiency.
    Event-Driven: The workflow is triggered by specific events, such as the appearance of a PDF in an S3 bucket.

When a PDF is uploaded to the S3 bucket, the system automatically:

    Downloads the PDF.
    Analyzes its contents.
    Uploads the analysis results back to the S3 bucket as a .txt file.
