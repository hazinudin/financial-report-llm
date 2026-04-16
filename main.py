import ollama
import fitz  # PyMuPDF
from PIL import Image
import io
import os

# Configuration
MODEL_NAME = "glm-ocr"
PDF_PATH = "/Users/hannanazinuddin/Downloads/LK_PTPS_Tahunan_2025.pdf"
OUTPUT_FILE = "./results/parsed_output.md"


def main():
    if not os.path.exists("./results"):
        os.makedirs("./results")

    print(f"Opening PDF: {PDF_PATH}")
    doc = fitz.open(PDF_PATH)

    # Clear previous results
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)

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
            with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                f.write(f"## Page {page_num + 1}\n\n{output_text}\n\n")

            print("✅ Done")
        except Exception as e:
            print(f"❌ Error: {e}")
            with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
                f.write(f"## Page {page_num + 1}\n\n[Error parsing page]\n\n")

    print(f"\nFinished! All pages parsed and saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
