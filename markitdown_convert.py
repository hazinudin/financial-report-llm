import os
import time
from markitdown import MarkItDown
from dotenv import load_dotenv

load_dotenv()

# Configuration
PDF_PATH = "./raw"
OUTPUT_DIR = "./results"


def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    files = [f for f in os.listdir(PDF_PATH) if f.lower().endswith(".pdf")]
    md = MarkItDown()

    for filename in files:
        pdf_file = os.path.join(PDF_PATH, filename)
        output_file = os.path.join(OUTPUT_DIR, os.path.splitext(filename)[0] + ".md")

        print(f"Processing PDF: {pdf_file}")

        try:
            start_time = time.time()
            result = md.convert(pdf_file)
            elapsed = time.time() - start_time

            with open(output_file, "w", encoding="utf-8") as f:
                f.write(result.text_content)

            print(f"✅ Done in {elapsed:.2f}s. Saved to {output_file}")
        except Exception as e:
            print(f"❌ Error processing {filename}: {e}")


if __name__ == "__main__":
    main()
