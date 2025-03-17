import os
from pathlib import Path

from mistralai import DocumentURLChunk, Mistral
from mistralai.models import OCRResponse

from pipeline.transform.base_ocr_reader import BaseOCRReader


class MistralOCRReader(BaseOCRReader):
    def __init__(self):
        super().__init__()
        api_key = os.environ["MISTRAL_API_KEY"]
        self.client = Mistral(api_key=api_key)

    def _upload(self, pdf_path: str):
        pdf_file = Path(pdf_path)
        assert pdf_file.is_file()

        uploaded_file = self.client.files.upload(
            file={
                "file_name": pdf_file.stem,
                "content": pdf_file.read_bytes(),
            },
            purpose="ocr",
        )

        signed_url = self.client.files.get_signed_url(file_id=uploaded_file.id, expiry=1)
        return signed_url

    def _replace_images_in_markdown(self, markdown_str: str, images_dict: dict) -> str:
        for img_name, base64_str in images_dict.items():
            markdown_str = markdown_str.replace(f"![{img_name}]({img_name})", f"![{img_name}]({base64_str})")
        return markdown_str

    def _get_combined_markdown(self, ocr_response: OCRResponse) -> str:
        markdowns: list[str] = []
        for page in ocr_response.pages:
            image_data = {}
            for img in page.images:
                image_data[img.id] = img.image_base64
            markdowns.append(self._replace_images_in_markdown(page.markdown, image_data))

        return "\n\n".join(markdowns)

    def read(self, pdf_path: str):
        signed_url = self._upload(pdf_path)

        pdf_response = self.client.ocr.process(
            document=DocumentURLChunk(document_url=signed_url.url),
            model="mistral-ocr-latest",
            include_image_base64=True
        )

        return self._get_combined_markdown(pdf_response)
