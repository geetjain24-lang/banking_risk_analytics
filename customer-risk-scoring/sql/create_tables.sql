
-- Customers table
CREATE TABLE Customer (
    CustomerID INT PRIMARY KEY,
    Name VARCHAR(100),
    Country VARCHAR(50),
    CustomerType VARCHAR(20), -- 'Individual' or 'Corporate'
    OnboardingChannel VARCHAR(20) -- 'Online' or 'Face-to-Face'
);

-- create transaction table
CREATE TABLE Transactions (
    TxnID INT PRIMARY KEY,
    CustomerID INT,
    Amount DECIMAL(18,2),
    TxnType VARCHAR(50), -- 'Domestic', 'International'
    Date DATE,
    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID)
);


-- Loans table
CREATE TABLE Loans (
    LoanID INT PRIMARY KEY,
    CustomerID INT,
    LoanAmount DECIMAL(18,2),
    LoanType VARCHAR(50),
    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID)
);


-- Sanctions List table
CREATE TABLE SanctionsList (
    EntityName VARCHAR(100),
    Country VARCHAR(50)
);



-- Risk Scores table
CREATE TABLE RiskScores (
    CustomerID INT PRIMARY KEY,
    Score INT,
    RiskLevel VARCHAR(20),
    LastUpdated DATE,
    FOREIGN KEY (CustomerID) REFERENCES Customer(CustomerID)
);

