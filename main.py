import fitz  # PyMuPDF
from PIL import Image
import io
import os
import time
import base64
import requests
from dotenv import load_dotenv

load_dotenv()

# Configuration
MODEL_NAME = "qwen/qwen3.5-9b"
PDF_PATH = "./raw"
OUTPUT_FILE = "./results"
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")


def get_processed_pages(output_file):
    """Return a set of page numbers already processed in the output file."""
    processed = set()
    if not os.path.exists(output_file):
        return processed

    with open(output_file, "r", encoding="utf-8") as f:
        for line in f:
            if line.startswith("## Page "):
                try:
                    page_num = int(line.split("Page ")[1].strip())
                    processed.add(page_num)
                except (ValueError, IndexError):
                    pass
    return processed


def main():
    if not os.path.exists("./results"):
        os.makedirs("./results")

    files = [f for f in os.listdir(PDF_PATH) if f.lower().endswith(".pdf")]

    for filename in files:
        pdf_file = os.path.join(PDF_PATH, filename)
        output_file = os.path.join("./results", os.path.splitext(filename)[0] + ".md")

        print(f"Processing PDF: {pdf_file}")
        doc = fitz.open(pdf_file)

        processed_pages = get_processed_pages(output_file)
        total_pages = len(doc)

        if processed_pages:
            print(
                f"Found {len(processed_pages)}/{total_pages} pages already processed."
            )

        for page_num in range(total_pages):
            page_number = page_num + 1

            if page_number in processed_pages:
                print(f"Skipping page {page_number}/{total_pages} (already processed)")
                continue

            print(
                f"Processing page {page_number}/{total_pages}...", end=" ", flush=True
            )

            try:
                # Render page to image
                page = doc.load_page(page_num)
                pix = page.get_pixmap(dpi=150)
                img_bytes = pix.tobytes("png")
                img_base64 = base64.b64encode(img_bytes).decode("utf-8")

                # Use OpenRouter to generate OCR text
                start_time = time.time()
                response = requests.post(
                    "https://openrouter.ai/api/v1/chat/completions",
                    headers={
                        "Content-Type": "application/json",
                        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                    },
                    json={
                        "model": MODEL_NAME,
                        "messages": [
                            {
                                "role": "user",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": "Text recognition, do not add anything other than content of the images.",
                                    },
                                    {
                                        "type": "image_url",
                                        "image_url": {
                                            "url": f"data:image/png;base64,{img_base64}"
                                        },
                                    },
                                ],
                            }
                        ],
                        "reasoning": {"enabled": True},
                    },
                )
                elapsed = time.time() - start_time

                response.raise_for_status()
                output_text = response.json()["choices"][0]["message"]["content"]

                # Save immediately to file (Append mode)
                with open(output_file, "a", encoding="utf-8") as f:
                    f.write(f"## Page {page_number}\n\n{output_text}\n\n")

                print(f"✅ Done in ({elapsed})")
            except Exception as e:
                print(f"❌ Error: {e}")

        print(f"Finished {filename}! Saved to {output_file}")


if __name__ == "__main__":
    main()
