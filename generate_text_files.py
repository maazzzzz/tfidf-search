from faker import Faker
import os
import sys

def generate_large_text_file(path, size_mb=100):
    """
    Generate a large realistic text file using Faker until size is reached.
    Produces paragraphs and sentences that look like natural language.

    Args:
        path (str): Output filename
        size_mb (int): Target size in megabytes
    """

    fake = Faker()
    target_bytes = size_mb * 1024 * 1024
    written = 0

    print(f"[GEN] Generating file: {path} ({size_mb} MB target)")

    with open(path, "w", encoding="utf-8") as f:
        while written < target_bytes:
            # Generate a realistic paragraph
            paragraph = fake.paragraph(nb_sentences=5) + "\n\n"

            f.write(paragraph)
            written += len(paragraph)

            # progress every 10 MB
            if written // (10 * 1024 * 1024) != (written - len(paragraph)) // (10 * 1024 * 1024):
                print(f"[GEN] Written {written // (1024*1024)} MB...")

    print(f"[DONE] Generated {path}: {written / (1024*1024):.2f} MB")


if __name__ == "__main__":
    # Default: generate 100MB
    size = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    filename = sys.argv[1] if len(sys.argv) > 1 else "large_faker.txt"

    generate_large_text_file(filename, size_mb=size)
