from pydantic import BaseModel, Field

class EpilepsyPatientContract(BaseModel):
    # The patient ID must exist and be a string of text
    patient_id: str
    
    # Age must be an integer, greater than or equal to 0, and less than 120
    age: int = Field(..., ge=0, le=120)
    
    # Core clinical fields we expect from the Hugging Face dataset
    seizure_type: str
    aed_status: str
    
    # The treatment gap must be a binary indicator (either 0 or 1)
    treatment_gap: int = Field(..., ge=0, le=1)

    class Config:
        # This converts incoming data smoothly even if it's packed in a pandas series
        from_attributes = True
