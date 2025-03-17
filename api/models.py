from typing import List, Optional, Dict

from pydantic import BaseModel


class PrivateFund(BaseModel):
    name: str
    identification_number: str


class FirmRequest(BaseModel):
    firm_crd_nb: int
    sec_nb: Optional[str]
    business_name: str
    full_legal_name: str
    address: str
    phone_number: str
    employee_count: int
    signatory: Optional[str]
    compensation_arrangements: List[str]
    client_types: Dict[str, int]
    private_funds: List[PrivateFund]


class FirmResponse(BaseModel):
    firm_crd_nb: str
    sec_nb: str
    business_name: str
    full_legal_name: str
    address: str
    phone_number: str
    employee_count: int
    signatory: str
    compensation_arrangements: List[str]
    client_types: Dict[str, int]
    private_funds: List[Dict[str, str]]
