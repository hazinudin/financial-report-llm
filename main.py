import ollama
import fitz  # PyMuPDF
from PIL import Image
import io
import os

# Configuration
MODEL_NAME = "glm-ocr"
PDF_PATH = "./raw"
OUTPUT_FILE = "./results"


def main():
    if not os.path.exists("./results"):
        os.makedirs("./results")

    files = [f for f in os.listdir(PDF_PATH) if f.lower().endswith(".pdf")]

    for filename in files:
        pdf_file = os.path.join(PDF_PATH, filename)
        output_file = os.path.join("./results", os.path.splitext(filename)[0] + ".md")

        if os.path.exists(output_file):
            print(f"Skipping {filename}, output file already exists.")
            continue

        print(f"Processing PDF: {pdf_file}")
        doc = fitz.open(pdf_file)

        for page_num in range(len(doc)):
            print(f"Processing page {page_num + 1}/{len(doc)}...", end=" ", flush=True)

            try:
                # Render page to image
                page = doc.load_page(page_num)
                pix = page.get_pixmap(matrix=fitz.Matrix(2, 2))
                img_bytes = pix.tobytes("png")

                # Use Ollama to generate OCR text
                response = ollama.generate(
                    model=MODEL_NAME, prompt="Text Recognition:", images=[img_bytes]
                )

                output_text = response["response"]

                # Save immediately to file (Append mode)
                with open(output_file, "a", encoding="utf-8") as f:
                    f.write(f"## Page {page_num + 1}\n\n{output_text}\n\n")

                print("✅ Done")
            except Exception as e:
                print(f"❌ Error: {e}")
                with open(output_file, "a", encoding="utf-8") as f:
                    f.write(f"## Page {page_num + 1}\n\n[Error parsing page]\n\n")

        print(f"Finished {filename}! Saved to {output_file}")


if __name__ == "__main__":
    main()
