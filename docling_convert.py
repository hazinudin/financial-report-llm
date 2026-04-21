import os
import time
from docling.document_converter import DocumentConverter
PDF_PATH = "./raw"
OUTPUT_DIR = "./results"
def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)
    files = [f for f in os.listdir(PDF_PATH) if f.lower().endswith(".pdf")]
    converter = DocumentConverter()
    for filename in files:
        pdf_file = os.path.join(PDF_PATH, filename)
        output_file = os.path.join(OUTPUT_DIR, os.path.splitext(filename)[0] + ".md")
        if os.path.exists(output_file):
            print(f"Skipping {filename} (already processed)")
            continue
        print(f"Processing PDF: {pdf_file}")
        try:
            start_time = time.time()
            result = converter.convert(pdf_file)
            elapsed = time.time() - start_time
            markdown_text = result.document.export_to_markdown()
            with open(output_file, "w", encoding="utf-8") as f:
                f.write(markdown_text)
            print(f"✅ Done in {elapsed:.2f}s. Saved to {output_file}")
        except Exception as e:
            print(f"❌ Error processing {filename}: {e}")
if __name__ == "__main__":
    main()