"""
CIK lookup model representing a CIK to company name mapping.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, Any

from ..exceptions import ValidationError


@dataclass
class CikLookup:
    """
    Represents a CIK lookup entity with validation.
    
    Attributes:
        cik: Central Index Key (primary key)
        company_name: Name of the company
        created_at: Timestamp when the record was created
        last_updated_at: Timestamp when the record was last updated
    """
    cik: int
    company_name: str
    created_at: Optional[datetime] = None
    last_updated_at: Optional[datetime] = None
    
    def __post_init__(self):
        """Clean and validate the CIK lookup data after initialization."""
        # Clean company name
        self.company_name = self.company_name.strip()
        self.validate()
    
    def validate(self):
        """
        Validate CIK lookup data.
        
        Raises:
            ValidationError: If validation fails
        """
        # Validate CIK
        if self.cik <= 0:
            raise ValidationError("cik", self.cik, "CIK must be a positive integer")
        
        # Validate company name
        if not self.company_name:
            raise ValidationError("company_name", self.company_name, "Company name cannot be empty")
        
        if len(self.company_name) > 255:
            raise ValidationError("company_name", self.company_name, "Company name cannot be longer than 255 characters")
    
    def to_dict(self, include_timestamps: bool = True) -> Dict[str, Any]:
        """
        Convert CIK lookup to dictionary.
        
        Args:
            include_timestamps: Whether to include created_at and last_updated_at
        
        Returns:
            Dictionary representation of the CIK lookup
        """
        result: Dict[str, Any] = {
            'cik': self.cik,
            'company_name': self.company_name
        }
        
        if include_timestamps:
            result.update({
                'created_at': self.created_at.isoformat() if self.created_at else None,
                'last_updated_at': self.last_updated_at.isoformat() if self.last_updated_at else None
            })
        
        return result
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'CikLookup':
        """
        Create CikLookup instance from dictionary.
        
        Args:
            data: Dictionary containing CIK lookup data
        
        Returns:
            CikLookup instance
        """
        # Handle datetime conversion
        created_at = data.get('created_at')
        if isinstance(created_at, str):
            created_at = datetime.fromisoformat(created_at)
        
        last_updated_at = data.get('last_updated_at')
        if isinstance(last_updated_at, str):
            last_updated_at = datetime.fromisoformat(last_updated_at)
        
        return cls(
            cik=data['cik'],
            company_name=data['company_name'],
            created_at=created_at,
            last_updated_at=last_updated_at
        )
    
    def __repr__(self) -> str:
        """String representation of CIK lookup."""
        return f"CikLookup(cik={self.cik}, company_name='{self.company_name}')"
    
    def __eq__(self, other: object) -> bool:
        """Compare two CikLookup instances."""
        if not isinstance(other, CikLookup):
            return False
        return self.cik == other.cik
    
    def __hash__(self) -> int:
        """Hash based on CIK (primary key)."""
        return hash(self.cik)
