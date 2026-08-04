"""Download and describe the public PDF corpus."""

import hashlib
from dataclasses import dataclass
from pathlib import Path

import httpx

from examples.document_processing.models import DocumentInput, DocumentRecord

EXAMPLE_DIR = Path(__file__).parent
DOCUMENT_DIR = EXAMPLE_DIR / "documents"


@dataclass(frozen=True)
class DocumentCase:
    """One source PDF and its reviewed extraction label."""

    document_id: str
    filename: str
    url: str
    sha256: str
    expected: DocumentRecord

    @property
    def path(self) -> Path:
        """Return the local corpus path."""
        return DOCUMENT_DIR / self.filename

    def replay_input(self) -> DocumentInput:
        """Build the compact input stored in the Langfuse trace."""
        return DocumentInput(
            document_id=self.document_id,
            pdf_path=str(self.path.relative_to(EXAMPLE_DIR.parents[1])),
        )


CASES = (
    DocumentCase(
        document_id="nist-ai-rmf-1.0",
        filename="nist-ai-rmf-1.0.pdf",
        url="https://nvlpubs.nist.gov/nistpubs/ai/NIST.AI.100-1.pdf",
        sha256="7576edb531d9848825814ee88e28b1795d3a84b435b4b797d3670eafdc4a89f1",
        expected=DocumentRecord(
            title="Artificial Intelligence Risk Management Framework (AI RMF 1.0)",
            publication_id="NIST AI 100-1",
            publication_month="2023-01",
            framework_functions=["GOVERN", "MAP", "MEASURE", "MANAGE"],
        ),
    ),
    DocumentCase(
        document_id="nist-genai-profile",
        filename="nist-genai-profile.pdf",
        url="https://nvlpubs.nist.gov/nistpubs/ai/NIST.AI.600-1.pdf",
        sha256="6e73620ab6b64e90ef2c04bf0e0d6246185a2f4b1b13cab0df494496cff89b6a",
        expected=DocumentRecord(
            title=(
                "Artificial Intelligence Risk Management Framework: "
                "Generative Artificial Intelligence Profile"
            ),
            publication_id="NIST AI 600-1",
            publication_month="2024-07",
            framework_functions=["GOVERN", "MAP", "MEASURE", "MANAGE"],
        ),
    ),
    DocumentCase(
        document_id="nist-csf-2.0",
        filename="nist-csf-2.0.pdf",
        url="https://nvlpubs.nist.gov/nistpubs/CSWP/NIST.CSWP.29.pdf",
        sha256="3c31f46fee98cac0c4323453e5109291a213b4de7fef8c058af9bf67f717433c",
        expected=DocumentRecord(
            title="The NIST Cybersecurity Framework (CSF) 2.0",
            publication_id="NIST CSWP 29",
            publication_month="2024-02",
            framework_functions=[
                "GOVERN",
                "IDENTIFY",
                "PROTECT",
                "DETECT",
                "RESPOND",
                "RECOVER",
            ],
        ),
    ),
)


def _matches_checksum(path: Path, expected: str) -> bool:
    """Check one file against its pinned digest."""
    return path.is_file() and hashlib.sha256(path.read_bytes()).hexdigest() == expected


def download_documents() -> None:
    """Download missing PDFs and reject changed source files."""
    DOCUMENT_DIR.mkdir(parents=True, exist_ok=True)
    with httpx.Client(follow_redirects=True, timeout=60) as client:
        for case in CASES:
            if _matches_checksum(case.path, case.sha256):
                continue
            response = client.get(case.url)
            response.raise_for_status()
            digest = hashlib.sha256(response.content).hexdigest()
            if digest != case.sha256:
                raise RuntimeError(
                    f"Checksum mismatch for {case.url}: expected {case.sha256}, "
                    f"received {digest}"
                )
            case.path.write_bytes(response.content)


if __name__ == "__main__":
    download_documents()
