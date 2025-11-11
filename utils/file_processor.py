import os
import requests
import fitz  # PyMuPDF
import pdfplumber
from PIL import Image
import pytesseract
import logging
from urllib.parse import urlparse
import docx

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def download_file(file_url: str, output_path: str) -> bool:
    """Downloads a file from a URL and saves it locally."""
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        with requests.get(file_url, headers=headers, stream=True, timeout=60, allow_redirects=True) as r:
            r.raise_for_status()
            with open(output_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
            logger.info(f"Successfully downloaded file to {output_path}")
            return True
        else:
            logger.error(f"Downloaded file is empty or missing: {output_path}")
            return False
            
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to download from {file_url}: {e}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred during download from {file_url}: {e}")
        return False

def extract_text_from_file(file_path: str) -> str:
    """Extracts text from a given file (PDF, DOCX, TXT, or Image)."""
    _, extension = os.path.splitext(file_path.lower())
    text_content = ""

    try:
        # Handle DOCX files
        if extension == '.docx':
            doc = docx.Document(file_path)
            full_text = []
            for para in doc.paragraphs:
                full_text.append(para.text)
            text_content = '\n'.join(full_text)
        
        # Handle TXT files
        elif extension == '.txt':
            with open(file_path, 'r', encoding='utf-8') as f:
                text_content = f.read()

        # Handle PDF files
        elif extension == '.pdf':
            with fitz.open(file_path) as doc:
                for page in doc:
                    text_content += page.get_text()
            if not text_content.strip():
                 with pdfplumber.open(file_path) as pdf:
                    for page in pdf.pages:
                        page_text = page.extract_text()
                        if page_text:
                            text_content += page_text + "\n"
        
        # Fallback to OCR for images or if text extraction yielded nothing
        # This will also handle image file types like .png, .jpg
        if not text_content.strip():
            logger.warning(f"No text extracted from {file_path}. Attempting OCR.")
            image_extensions = ['.png', '.jpg', '.jpeg', '.bmp', '.tiff']
            if extension == '.pdf':
                doc = fitz.open(file_path)
                for page_num in range(len(doc)):
                    page = doc.load_page(page_num)
                    pix = page.get_pixmap(dpi=200)
                    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                    text_content += pytesseract.image_to_string(img) + "\n"
                doc.close()
            elif extension in image_extensions:
                 img = Image.open(file_path)
                 text_content = pytesseract.image_to_string(img)
        
        if not text_content.strip():
            return "Error: Could not extract any text from the document."

        return text_content.strip()

    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        return f"Error: Failed to process file. Reason: {e}"