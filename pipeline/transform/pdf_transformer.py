import logging
import re
from typing import Dict, Optional, Any, List

import fitz
import numpy as np
from PIL import Image

from pipeline.config.run_configuration import RunConfiguration
from pipeline.transform.base_ocr_reader import BaseOCRReader
from pipeline.transform.base_transformer import BaseTransformer
from pipeline.transform.fitz_ocr_reader import FitzOCRReader


class PdfTransformer(BaseTransformer):
    """Transform extracted text from SEC ADV PDF files"""

    def __init__(self, config: RunConfiguration, ocr_reader: BaseOCRReader = FitzOCRReader()):
        super().__init__(config)
        logging.basicConfig(level=logging.INFO)
        self.ocr_reader = ocr_reader

    def transform(self, firm_crd_number: int) -> Dict[str, Any]:
        """Extract text from a PDF file and parse relevant information"""
        pdf_path = f"{self.config.input_dir}/{firm_crd_number}.pdf"
        self.logger.info(f"Transforming PDF content at {pdf_path}...")

        full_text = self.ocr_reader.read(pdf_path)

        return {
            'firm_crd_nb': self._extract_firm_crd_number(full_text),
            'sec_nb': self._extract_sec_number(full_text),
            'business_name': self._extract_business_name(full_text),
            'full_legal_name': self._extract_full_legal_name(full_text),
            'address': self._extract_address(full_text),
            'phone_number': self._extract_phone_number(full_text),
            'compensation_arrangements': self._extract_compensation_arrangements(pdf_path),
            'employee_count': self._extract_investment_advisory_employee_count(full_text),
            'client_types': self._extract_client_types(full_text),
            'private_funds': self._extract_private_funds_and_ids(full_text),
            'signatory': self._extract_signatory(full_text)
        }

    def _extract_with_regex(self, text: str, pattern: str) -> Optional[str]:
        """Extract information using regex and return the first capturing group"""
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1).strip()
        self.logger.debug(f"No match found for pattern: {pattern}")
        return None

    def _extract_firm_crd_number(self, text: str) -> Optional[int]:
        """Extract firm CRD number"""
        patterns = [
            r'CRD Number:\s*(\d+)',
            r'your CRD number:\s*(\d+)',
            r'CRD number:\s*(\d+)',
            r'CRD Number\D*(\d+)'
        ]

        for pattern in patterns:
            result = self._extract_with_regex(text, pattern)
            if result:
                try:
                    return int(result)
                except ValueError:
                    self.logger.warning("Failed to convert extracted firm CRD number to int.")
                    return None

        self.logger.warning("Failed to extract Firm CRD Number.")
        return None

    def _extract_sec_number(self, text: str) -> Optional[str]:
        """Extract SEC file number"""
        patterns = [
            r'SEC file number:\s*([\d-]+)',
            r'SEC File Number:\s*([\d-]+)',
            r'you are registered with the SEC as an investment adviser, your SEC file number:\s*([\d-]+)',
            r'your SEC file number:\s*([\d-]+)'
        ]

        for pattern in patterns:
            result = self._extract_with_regex(text, pattern)
            if result:
                return result

        self.logger.warning("Failed to extract SEC Number.")
        return None

    def _extract_business_name(self, text: str) -> Optional[str]:
        """Extract primary business name"""
        patterns = [
            r'Primary Business Name:\s*([^\n]+)',
            r'Name under which you primarily conduct your advisory business[^:]*:\s*([^\n]+)'
        ]

        for pattern in patterns:
            result = self._extract_with_regex(text, pattern)
            if result:
                return result

        self.logger.warning("Failed to extract Business Name.")
        return None

    def _extract_full_legal_name(self, text: str) -> Optional[str]:
        """Extract full legal name"""
        patterns = [
            r'Your full legal name.*?:\s*([^\n]+)',
            r'full legal name.*?:\s*([^\n]+)',
            r'A\.\s+Your full legal name.*?:\s*([^\n]+)'
        ]

        for pattern in patterns:
            result = self._extract_with_regex(text, pattern)
            if result:
                return result

        self.logger.warning("Failed to extract Full Legal Name.")
        return None

    def _extract_phone_number(self, text: str) -> Optional[str]:
        """Extract phone number"""
        patterns = [
            r'Telephone Number:\s*([\d\- \(\)\+]+)',
            r'Telephone number at this location:\s*([\d\- \(\)\+]+)'
        ]

        for pattern in patterns:
            result = self._extract_with_regex(text, pattern)
            if result:
                return result

        self.logger.warning("Failed to extract Phone Number.")
        return None

    def _extract_investment_advisory_employee_count(self, text: str) -> Optional[int]:
        """Extract the number of employees performing investment advisory functions"""
        pattern = r'Approximately how many of the employees reported in 5\.A\. perform investment advisory functions.*?\n(\d+)'
        match = re.search(pattern, text, re.IGNORECASE | re.DOTALL)

        if match:
            try:
                return int(match.group(1).strip())
            except ValueError:
                self.logger.warning("Failed to convert extracted employee count to int.")
                return None

        self.logger.warning("Investment advisory employee count not found.")
        return None

    def _extract_address(self, text: str) -> Optional[str]:
        """Extract and format address from text input"""
        components = {}
        patterns = {
            'street1': r'Number and Street 1:\s*(.+?)(?:\s*Number and Street 2:|$)',
            'street2': r'Number and Street 2:\s*(.+?)(?:\s*City:|$)',
            'city': r'City:\s*(.+?)(?:\s*State:|$)',
            'state': r'State:\s*(.+?)(?:\s*Country:|$)',
            'country': r'Country:\s*(.+?)(?:\s*ZIP\+4/Postal Code:|$)',
            'postal_code': r'ZIP\+4/Postal Code:\s*(.+?)(?:\s*If this address is|$)'
        }

        for key, pattern in patterns.items():
            match = re.search(pattern, text, re.DOTALL)
            components[key] = match.group(1).strip() if match and match.group(1).strip() else None

        address_parts = []

        street_parts = []
        if components['street1']:
            street_parts.append(components['street1'])
        if components['street2']:
            street_parts.append(components['street2'])
        if street_parts:
            address_parts.append(', '.join(street_parts))

        if components['country'] == 'United States':
            city_state = []
            if components['city']:
                city_state.append(components['city'])
            if components['state']:
                city_state.append(components['state'])
            if city_state:
                address_parts.append(', '.join(city_state))
        elif components['city']:
            address_parts.append(components['city'])

        if components['country'] == 'United States' and components['postal_code']:
            if address_parts and components['state']:
                last_part = address_parts[-1]
                address_parts[-1] = f"{last_part} {components['postal_code']}"
            else:
                address_parts.append(components['postal_code'])
        elif components['postal_code']:
            address_parts.append(components['postal_code'])

        if components['country']:
            address_parts.append(components['country'])

        if not address_parts:
            self.logger.warning("No valid address components found.")
            return None

        return ', '.join(address_parts)

    def _is_checkbox_checked(self, page, rect, threshold=150):
        """Determine if a checkbox is checked based on pixel intensity."""
        pix = page.get_pixmap(matrix=fitz.Matrix(2, 2), clip=rect, colorspace="gray")  # High-res grayscale
        img = Image.frombytes("L", [pix.width, pix.height], pix.samples)  # Convert to PIL
        img_array = np.array(img)  # Convert to NumPy

        # Determine black pixel ratio
        return np.sum(img_array < threshold) / img_array.size > 0.12  # Adjust sensitivity if needed

    def _extract_compensation_arrangements(self, pdf_path: str) -> List[str]:
        doc = fitz.open(pdf_path)
        page = None
        for page_num in range(len(doc)):
            target_page = doc[page_num]
            found_rects = target_page.search_for("Compensation Arrangements")
            if found_rects:
                page = target_page
                break  # Stop at the first occurrence

        compensation_options = {
            "A percentage of assets under your management": "(1)",
            "Hourly charges": "(2)",
            "Subscription fees (for a newsletter or periodical)": "(3)",
            "Fixed fees (other than subscription fees)": "(4)",
            "Commissions": "(5)",
            "Performance-based fees": "(6)",
            "Other (specify):": "(7)"
        }

        checked_options = []

        for label, option_number in compensation_options.items():
            rects = page.search_for(label)
            if not rects:
                continue

            label_rect = rects[0]
            checkbox_rect = fitz.Rect(label_rect.x0 - 44, label_rect.y0 + 1, label_rect.x0 - 38, label_rect.y1 - 1)

            # Draw rectangles to visualize
            # page.draw_rect(label_rect, color=(1, 0, 0), width=0.5)  # Red for labels
            # page.draw_rect(checkbox_rect, color=(0, 1, 0), width=1.0)  # Green for checkboxes

            if self._is_checkbox_checked(page, checkbox_rect):
                checked_options.append(label)

        # for debugging pixel map
        # pix = page.get_pixmap()
        # img = Image.open(io.BytesIO(pix.tobytes()))
        # img.save("test.png")

        return checked_options

    def _extract_client_types(self, text: str) -> dict:
        """Extract types of clients served and their AUM"""
        section_pattern = r'Type of Client.*?under Management(.*?)(?:Item|Section|\Z)'
        section_match = re.search(section_pattern, text, re.DOTALL)

        if not section_match:
            section_pattern = r'Type of Client(.*?)(?:Item|\Z)'
            section_match = re.search(section_pattern, text, re.DOTALL)

            if not section_match:
                self.logger.warning("Client types section not found.")
                return {}

        client_section = section_match.group(1)
        aum_details = {}

        client_mapping = {
            'a': 'Individuals',
            'b': 'High Net Worth Individuals',
            'c': 'Banking Institutions',
            'd': 'Investment Companies',
            'e': 'Business Development Companies',
            'f': 'Pooled Investment Vehicles',
            'g': 'Pension Plans',
            'h': 'Charitable Organizations',
            'i': 'State Entities',
            'j': 'Other Advisers',
            'k': 'Insurance Companies',
            'l': 'Sovereign Wealth Funds',
            'm': 'Corporations',
            'n': 'Other'
        }

        letter_sequence = list(client_mapping.keys())

        for i, letter in enumerate(letter_sequence):
            client_type = client_mapping[letter]
            next_pattern = rf'\({letter_sequence[i + 1]}\)' if i < len(letter_sequence) - 1 else r'Item|\Z'
            client_pattern = rf'\({letter}\)(.*?)(?:{next_pattern})'
            client_match = re.search(client_pattern, client_section, re.DOTALL)

            if client_match:
                line_text = client_match.group(1)

                if letter == 'n' and ':' in line_text:
                    other_desc_match = re.search(r'Other:\s*([^\n$]+)', line_text)
                    if other_desc_match:
                        other_desc = other_desc_match.group(1).strip()
                        client_type = f"{client_type}: {other_desc}"

                aum_value = 0
                aum_match = re.search(r'\$\s*([\d,]+)', line_text)
                if aum_match:
                    aum_str = aum_match.group(1).replace(',', '')
                    if aum_str:
                        aum_value = int(aum_str)

                if aum_value > 0:
                    aum_details[client_type] = aum_value

        return aum_details

    def _extract_private_funds_and_ids(self, text: str) -> dict:
        """Extract private fund names and their identification numbers"""
        pattern = r"Name of the private fund:\s*([^\n]+?)\s*\n\s*\(b\) Private fund identification number:\s*\(include the \"805-\" prefix also\)\s*(805-\d+)"
        matches = re.findall(pattern, text)

        results = {}
        for fund_name, fund_id in matches:
            clean_name = fund_name.strip()
            clean_id = fund_id.strip()
            results[clean_name] = clean_id

        return results

    def _extract_signatory(self, text: str) -> Optional[str]:
        """Extract signatory information from the form"""
        patterns = [
            r'Printed Name:\s*\n([^\n]+)',
            r'Printed Name:\s*([^:\n]+?)(?=\s*\n)',
            r'Signature:\s*\n([^\n]+)',
            r'Signature:\s*([^:\n]+?)(?=\s*\n)'
        ]

        for pattern in patterns:
            result = self._extract_with_regex(text, pattern)
            if result:
                cleaned_result = result.strip()
                if "Title:" in cleaned_result:
                    cleaned_result = cleaned_result.split("Title:")[0].strip()

                if cleaned_result and not cleaned_result.isspace():
                    return cleaned_result

        self.logger.warning("Failed to extract Signatory.")
        return None
