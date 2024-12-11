CREATE DATABASE HospitalDB;

USE HospitalDB;
GO

CREATE TABLE Addresses (
	AddressID int IDENTITY (1,1) PRIMARY KEY,
	Address1 nvarchar(100) NOT NULL,
	Address2 nvarchar(100),
	City nvarchar(50),
	Postcode nvarchar(10)
	)
;

CREATE TABLE EmergencyContacts(
	EmergencyContactID int IDENTITY (1,1) PRIMARY KEY,
	FirstName nvarchar(30) NOT NULL,
	LastName nvarchar(30) NOT NULL,
	Relationship nvarchar(20) NOT NULL,
	TelephoneNumber nvarchar(20) NOT NULL,
	AddressID int NOT NULL,
	FOREIGN KEY (AddressID) REFERENCES Addresses (AddressID)
	)
;

CREATE TABLE Patients (
	PatientID int IDENTITY (1,1) PRIMARY KEY,
	FirstName nvarchar(30) NOT NULL,
	LastName nvarchar(30) NOT NULL,
	AddressID int NOT NULL,
	DateOfBirth DATE NOT NULL,
	EmailAddress nvarchar(100),
	TelephoneNumber nvarchar(20),
	EmergencyContactID int NOT NULL,
	Username nvarchar(20) NOT NULL,
	Password nvarchar(20) NOT NULL,
	FOREIGN KEY (AddressID) REFERENCES Addresses (AddressID),
	FOREIGN KEY (EmergencyContactID) REFERENCES EmergencyContacts (EmergencyContactID)
	)
;

CREATE TABLE PatientEmergencyContacts (
	PatientID int,
	EmergencyContactID int,
	PRIMARY KEY (PatientID, EmergencyContactID),
	FOREIGN KEY (PatientID) REFERENCES Patients (PatientID),
	FOREIGN KEY (EmergencyContactID) REFERENCES EmergencyContacts (EmergencyContactID)
	)
;

CREATE TABLE Doctors (
	DoctorID int IDENTITY (1,1) PRIMARY KEY,
	FirstName nvarchar(30) NOT NULL,
	LastName nvarchar(30) NOT NULL
	)
;

CREATE TABLE MedicalRecords (
	RecordID int IDENTITY (1,1) PRIMARY KEY,
	PatientID int NOT NULL,
	FOREIGN KEY (PatientID) REFERENCES Patients (PatientID)
	)
;

CREATE TABLE Appointments (
	AppointmentID int IDENTITY (1,1) PRIMARY KEY,
	PatientID int NOT NULL,
	DoctorID int NOT NULL,
	Date date NOT NULL,
	Time time NOT NULL,
	Department nvarchar(20) NOT NULL,
	Status nvarchar(20) NOT NULL,
	Feedback nvarchar(100),
	FOREIGN KEY (PatientID) REFERENCES Patients (PatientID),
	FOREIGN KEY (DoctorID) REFERENCES Doctors (DoctorID)
	)
;

CREATE TABLE AppointmentRecords (
	AppointmentRecordID int IDENTITY (1,1) PRIMARY KEY,
	RecordID int NOT NULL,
	AppointmentID int NOT NULL,
	FOREIGN KEY (RecordID) REFERENCES MedicalRecords (RecordID),
	FOREIGN KEY (AppointmentID) REFERENCES Appointments (AppointmentID)
	)
;

CREATE TABLE Allergies (
	AllergyID int IDENTITY (1,1) PRIMARY KEY,
	Allergy nvarchar(50) NOT NULL
	)
;

CREATE TABLE PatientAllergies (
	RecordID int NOT NULL,
	AllergyID int NOT NULL,
	PRIMARY KEY (RecordID, AllergyID),
	FOREIGN KEY (RecordID) REFERENCES MedicalRecords (RecordID),
	FOREIGN KEY (AllergyID) REFERENCES Allergies (AllergyID)
	)
;

CREATE TABLE Diagnoses (
	DiagnosisID int IDENTITY (1,1) PRIMARY KEY,
	RecordID int NOT NULL,
	Diagnosis nvarchar(200) NOT NULL,
	FOREIGN KEY (RecordID) REFERENCES MedicalRecords (RecordID)
	)
;

CREATE TABLE Medicines (
	MedicineID int IDENTITY (1,1) PRIMARY KEY,
	Medicine nvarchar(100) NOT NULL
	)
;

CREATE TABLE PatientMedicines (
	RecordID int NOT NULL,
	MedicineID int NOT NULL,
	PRIMARY KEY (RecordID, MedicineID),
	FOREIGN KEY (RecordID) REFERENCES MedicalRecords (RecordID),
	FOREIGN KEY (MedicineID) REFERENCES Medicines (MedicineID)
	)
;

INSERT INTO Addresses (
	Address1,
	Address2,
	City,
	Postcode
	)
SELECT DISTINCT
	A_Address1,
	A_Address2,
	A_City,
	A_Postcode
FROM dbo.AssignmentData
UNION
SELECT DISTINCT
	E_Address1,
	E_Address2,
	E_City,
	E_Postcode
FROM dbo.AssignmentData

INSERT INTO Addresses (
	Address1,
	Address2,
	City,
	Postcode
	)
SELECT
	E_Address1,
	E_Address2,
	E_City,
	E_Postcode
FROM dbo.AssignmentData

DELETE FROM Addresses

INSERT INTO EmergencyContacts (
	FirstName,
	LastName,
	Relationship,
	TelephoneNumber,
	AddressID
	)
VALUES (
	'Trung',
	'Le',
	'Husband',
	'07572314908',
	31
	)
;

INSERT INTO Patients (
	FirstName,
	LastName,
	AddressID,
	DateOfBirth,
	EmailAddress,
	TelephoneNumber,
	EmergencyContactID,
	Username,
	Password
	)
VALUES (
	'Thi',
	'Duong',
	34,
	CONVERT(DATE, '26-04-1987', 105),
	'thi.duong@hotmail.com',
	'07568294571',
	10,
	'thi.duong',
	'Jt987210'
	)

UPDATE Patients
SET DateOfBirth = '1953-04-24'
WHERE PatientID = 10


INSERT INTO PatientEmergencyContacts (
	PatientID,
	EmergencyContactID
	)
SELECT
	PatientID,
	EmergencyContactID
FROM Patients

INSERT INTO Doctors (
	FirstName,
	LastName
	)
VALUES (
	'Sonny',
	'Tran'
	)

CREATE PROCEDURE CreateAppointment
	@PatientID int,
	@DoctorID int,
	@AppointmentDateText nvarchar(15),
	@AppointmentTime time,
	@Department nvarchar(20)
AS
BEGIN
	DECLARE @AppointmentDate DATE = CONVERT(DATE, @AppointmentDateText, 105);
	DECLARE @AppointmentDateTime DATETIME = CAST(@AppointmentDate AS DATETIME) + CAST(@AppointmentTime AS DATETIME);
	DECLARE @Status nvarchar(20);
	IF @AppointmentDateTime > GETDATE()
		SET @Status = 'Scheduled'
	ELSE
		SET @Status = 'Completed';
	INSERT INTO Appointments (
		PatientID,
		DoctorID,
		Date,
		Time,
		Department,
		Status
		)
	VALUES (
		@PatientID,
		@DoctorID,
		@AppointmentDate,
		@AppointmentTime,
		@Department,
		@Status
		);
END;

EXEC CreateAppointment
	@PatientID = 5,
	@DoctorID = 3,
	@AppointmentDateText = '16-04-2024',
	@AppointmentTime = '17:20',
	@Department = 'Cardiology';

ALTER TABLE Doctors
ADD Department nvarchar(20);

UPDATE Doctors
SET Department = 'Oncology'
WHERE DoctorID = 1;

UPDATE Doctors
SET Department = 'Neurology'
WHERE DoctorID = 2;

UPDATE Doctors
SET Department = 'Cardiology'
WHERE DoctorID = 3;

UPDATE Doctors
SET Department = 'Dermatology'
WHERE DoctorID = 4;

UPDATE Doctors
SET Department = 'Endoscopy'
WHERE DoctorID = 5;

Update Doctors
SET Department = 'Gastroenterology'
WHERE DoctorID = 5;

CREATE OR ALTER PROCEDURE CreateAppointment
	@PatientID int,
	@DoctorID int,
	@AppointmentDateText nvarchar(15),
	@AppointmentTime time
AS
BEGIN
	DECLARE @AppointmentDate DATE = CONVERT(DATE, @AppointmentDateText, 105);
	DECLARE @AppointmentDateTime DATETIME = CAST(@AppointmentDate AS DATETIME) + CAST(@AppointmentTime AS DATETIME);
	DECLARE @Department nvarchar(20);
	DECLARE @Status nvarchar(20);
	SELECT @Department = Department FROM Doctors WHERE DoctorID = @DoctorID;
	IF @AppointmentDateTime > GETDATE()
		SET @Status = 'Scheduled'
	ELSE
		SET @Status = 'Completed';
	INSERT INTO Appointments (
		PatientID,
		DoctorID,
		Date,
		Time,
		Department,
		Status
		)
	VALUES (
		@PatientID,
		@DoctorID,
		@AppointmentDate,
		@AppointmentTime,
		@Department,
		@Status
		);
END;

EXEC CreateAppointment
	@PatientID = 7,
	@DoctorID = 3,
	@AppointmentDateText = '23-04-2024',
	@AppointmentTime = '11:30'

CREATE PROCEDURE GiveFeedback
	@AppointmentID int,
	@Feedback nvarchar(100)
AS
BEGIN
	UPDATE Appointments
	SET Feedback = @Feedback
	WHERE AppointmentID = @AppointmentID;
END;
	
EXEC GiveFeedback
	@AppointmentID = 22,
	@Feedback = 'The doctor seemed very knowledgeable about my issue.'

ALTER TABLE Appointments 
ALTER COLUMN Time time(0);

INSERT INTO MedicalRecords (PatientID)
SELECT PatientID FROM Patients

INSERT INTO AppointmentRecords (
	RecordID,
	AppointmentID
	)
SELECT 
	mr.RecordID,
	a.AppointmentID
FROM MedicalRecords mr
INNER JOIN Appointments a
ON mr.PatientID = a.PatientID
WHERE a.Date = CAST(GETDATE() AS DATE)

INSERT INTO Allergies
	(Allergy)
VALUES
	('Peanuts'),
	('Lactose'),
	('Eggs'),
	('Wheat'),
	('Gluten'),
	('Pollen'),
	('Latex'),
	('Shellfish'),
	('Fish'),
	('Soy'),
	('Insect Bites'),
	('Penicillin'),
	('Aspirin')
;

INSERT INTO Medicines
	(Medicine)
Values
	('Acetaminophen'),
	('Ibuprofen'),
	('Morphine'),
	('Fetanyl'),
	('Amoxicillin'),
	('Ciprofloxacin'),
	('Vancomycin'),
	('Piperacillin'),
	('Atenolol'),
	('Simvastatin'),
	('Warfarin'),
	('Lisanopril'),
	('Heparin'),
	('Enoxaparin'),
	('Albuterol'),
	('Fluticasone'),
	('Omeprazole'),
	('Metoclopramide'),
	('Metformin'),
	('Insulin'),
	('Sertraline'),
	('Haloperidol'),
	('Midazolam'),
	('Lorazepam'),
	('Furosemide'),
	('Levothyroxine'),
	('Dexamethsaone')
;

INSERT INTO Diagnoses (
	Diagnosis,
	RecordID
	)
VALUES 
	('Headache and fever. Ongoing for 2 weeks.', 5),
	('Suffers from arthritis, pain worsens over the last two weeks.', 3),
	('Surgery in abdominal area.', 2),
	('Surgery to remove cancerous cells around lungs.', 7),
	('Cough for 3 weeks. Bacterial.', 1),
	('Urinary Tract Infection.', 3),
	('Complaints about stomach. Gastrointestinal infection.', 9),
	('Meningitis.', 4),
	('Pneumonia.', 6),
	('Very high blood pressure.', 4),
	('Very overweight, need to reduce cholesterol levels due to risk of heart disease.', 8),
	('At risk of stroke. Requires Warfarin.', 4),
	('High risk of heart failure.', 10),
	('Risk of blood clots due to too much sitting during work hours.', 1),
	('Blood clots formed. Needs treatment immediately.', 6),
	('Asthma getting worse due to mould at property.', 8),
	('Cough causing imflammation due to asthma.', 3),
	('Gastroesophageal reflux disease.', 10),
	('Vomitting regularly after food due to gastro issues.', 2),
	('Overweight. Type 2 diabetes.', 10),
	('Type 1 diabetes.', 1),
	('Depression and PTSD due to traumatic event.', 6),
	('Suffers from schizophrenia.', 3),
	('Severe agitation after surgery.', 7),
	('Anxiety disorder.', 9),
	('Congestive heart failure.', 8),
	('Thyroid cancer.', 5),
	('Severe infection from COVID-19.', 8)

INSERT INTO PatientMedicines (
	MedicineID,
	RecordID
	)
VALUES
	(1, 5),
	(2, 3),
	(3, 2),
	(4, 7),
	(5, 1),
	(6, 3),
	(7, 9),
	(8, 4),
	(9, 8),
	(10, 4),
	(11, 10),
	(12, 1),
	(13, 6),
	(14, 8), 
	(15, 3),
	(16, 10),
	(17, 2),
	(18, 10),
	(19, 1),
	(20, 6),
	(21, 3), 
	(22, 7),
	(23, 9),
	(24, 8), 
	(25, 5),
	(26, 8)

INSERT INTO PatientAllergies (
	RecordID,
	AllergyID
)
VALUES
	(1, 10),
	(1, 4),
	(3, 11),
	(3, 2),
	(3, 10),
	(4, 5),
	(5, 6),
	(7, 11),
	(7, 9),
	(8, 1),
	(10, 9),
	(10, 3)

ALTER TABLE Appointments
ADD CONSTRAINT chk_NotPastDate
	CHECK (date >= CAST(GETDATE() AS date) OR Status = 'Completed');

SELECT
	p.FirstName,
	p.LastName,
	p.DateOfBirth,
	d.Diagnosis
FROM Patients p
INNER JOIN MedicalRecords mr
ON p.PatientID = mr.PatientID
INNER JOIN Diagnoses d
ON mr.RecordID = d.RecordID
WHERE DATEDIFF(year, p.DateOfBirth, GETDATE()) >= 40
AND d.Diagnosis LIKE '%cancer%'

SELECT
	m.Medicine,
	a.Date
FROM Medicines m
INNER JOIN PatientMedicines pm
ON pm.MedicineID = m.MedicineID
INNER JOIN MedicalRecords mr
ON mr.RecordID = pm.RecordID
INNER JOIN AppointmentRecords ar
ON ar.RecordID = mr.RecordID
INNER JOIN Appointments a
ON a.AppointmentID = ar.AppointmentID
WHERE a.Date <= CAST(GETDATE() AS DATE)
ORDER BY ABS(DATEDIFF(day, GETDATE(), a.Date)), a.Date DESC;

CREATE PROCEDURE SearchForMedicine
	@Medicine nvarchar(100)
AS
BEGIN
	SELECT
		m.Medicine,
		a.Date
	FROM Medicines m
	INNER JOIN PatientMedicines pm
	ON pm.MedicineID = m.MedicineID
	INNER JOIN MedicalRecords mr
	ON mr.RecordID = pm.RecordID
	INNER JOIN AppointmentRecords ar
	ON ar.RecordID = mr.RecordID
	INNER JOIN Appointments a
	ON a.AppointmentID = ar.AppointmentID
	WHERE a.Date <= CAST(GETDATE() AS DATE)
	AND m.Medicine LIKE '%' + @Medicine + '%'
	ORDER BY ABS(DATEDIFF(day, GETDATE(), a.Date)), a.Date DESC;
END

EXEC SearchForMedicine @Medicine = 'Sertraline';

CREATE PROCEDURE RetreivePatientInfoToday
	@PatientID int
AS
BEGIN
	SELECT 
		p.FirstName,
		p.LastName,
		d.Diagnosis, 
		a.Allergy
	FROM Diagnoses d
	INNER JOIN MedicalRecords mr
	ON mr.RecordID = d.RecordID
	INNER JOIN PatientAllergies pa
	ON pa.RecordID = mr.RecordID
	INNER JOIN Allergies a
	ON a.AllergyID = pa.AllergyID
	INNER JOIN AppointmentRecords ar
	ON ar.RecordID = mr.RecordID
	INNER JOIN Appointments ap
	ON ap.AppointmentID = ar.AppointmentID
	INNER JOIN Patients p
	ON p.PatientID = mr.PatientID
	WHERE ap.Date = CAST(GETDATE() AS DATE)
	AND mr.PatientID = @PatientID;
END;

EXEC RetreivePatientInfoToday @PatientID = 7

EXEC CreateAppointment
	@PatientID = 8,
	@DoctorID = 4,
	@AppointmentDateText = '23-04-2024',
	@AppointmentTime = '15:30'

SELECT AppointmentID, Date
FROM Appointments
WHERE Date = CAST(GETDATE() AS DATE)

SELECT 
	mr.PatientID,
	ap.Date,
	d.Diagnosis, 
	a.Allergy
FROM Diagnoses d
INNER JOIN MedicalRecords mr
ON mr.RecordID = d.RecordID
INNER JOIN PatientAllergies pa
ON pa.RecordID = mr.RecordID
INNER JOIN Allergies a
ON a.AllergyID = pa.AllergyID
INNER JOIN AppointmentRecords ar
ON ar.RecordID = mr.RecordID
INNER JOIN Appointments ap
ON ap.AppointmentID = ar.AppointmentID
WHERE Date = CAST(GETDATE() AS DATE)

ALTER TABLE Doctors
ADD Specialty nvarchar(100);

CREATE PROCEDURE UpdateDoctorDetails
	@DoctorID int,
	@Department nvarchar(100),
	@Specialty nvarchar(100)
AS
BEGIN
	UPDATE Doctors
	SET Specialty = @Specialty,
		Department = @Department
	WHERE DoctorID = @DoctorID
END;

EXEC UpdateDoctorDetails
	@DoctorID = 5,
	@Department = 'Gastroenterology',
	@Specialty = 'Hepatologist'

CREATE PROCEDURE DeleteCompletedAppointments
AS
BEGIN
	DELETE FROM Appointments
	WHERE Status = 'Completed'
END

CREATE VIEW DoctorDetails AS
	SELECT
		d.DoctorID,
		d.Department,
		d.Specialty,
		a.AppointmentID,
		a.Feedback
	FROM Doctors d
	INNER JOIN Appointments a
	ON d.DoctorID = a.DoctorID
;

SELECT * FROM DoctorDetails

SELECT
	d.DoctorID,
	d.Department,
	d.Specialty,
	a.AppointmentID,
	a.Feedback
FROM Doctors d
INNER JOIN Appointments a
ON d.DoctorID = a.DoctorID

CREATE TRIGGER t_CancelledAppointment
ON Appointments
AFTER UPDATE
AS BEGIN
	IF UPDATE (Status)
	BEGIN
		UPDATE ap 
		SET ap.Status = 'Available'
		FROM Appointments ap
		INNER JOIN inserted i 
		ON ap.AppointmentID = i.AppointmentID
		WHERE i.Status = 'Cancelled';
	END
END;

UPDATE Appointments
SET Status = 'Cancelled'
WHERE AppointmentID = 13

SELECT COUNT(*) AS 'Gastroenterology Appointments'
FROM Appointments ap
INNER JOIN Doctors d
ON d.DoctorID = ap.DoctorID
WHERE d.Department = 'Gastroenterology'
AND ap.Status = 'Completed'

SELECT COUNT(*) AS 'Gastroenterology Appointments'
FROM Appointments ap
INNER JOIN Doctors d
ON d.DoctorID = ap.DoctorID
WHERE d.Specialty = 'Gastroenterology'
AND ap.Status = 'Completed'