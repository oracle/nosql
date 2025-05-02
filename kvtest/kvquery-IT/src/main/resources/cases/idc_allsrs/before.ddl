CREATE TABLE patient(
    id  INTEGER,
    recordData JSON,
    lastupdated TIMESTAMP(0),
    primary key (id)
)

CREATE INDEX idx1 on patient(
recordData.PATIENT_PRESCRIPTION_INFO.PRESCRIPTION_INFO[].PRESCRIPTION_DATE."@value" as string
)
