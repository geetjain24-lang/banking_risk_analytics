

-- Insert into Customer
DECLARE @num INT = 1;

WHILE @num <= 500
BEGIN
    INSERT INTO Customer (CustomerID, Name, Country, CustomerType, OnboardingChannel)
    VALUES (
        @num,
        -- Assign name
        Concat('Customer_',@num),

        --Assign Country
        case when @num % 5 = 0 then 'IRAN'
             when @num % 7 = 0 then 'UK'
        else 'USA' end,

       -- asisgn Customer type
       case when @num % 2 = 0 then 'P'
       else 'C' end,

        -- Onboarding channel
        CASE WHEN @num % 4 = 0 THEN 'F2F' ELSE 'Online' END

    );
        SET @num = @num + 1;
END;

-- Insert 200 sample loans
DECLARE @i INT = 1;

WHILE @i <= 200
BEGIN
    INSERT INTO Loans (LoanID, CustomerID, LoanAmount, LoanType)
    VALUES (
        @i,
        -- Cycle through CustomerIDs 1–100
        ((@i - 1) % 100) + 1,

        -- Random loan amount between 5,000 and 500,000
        (ABS(CHECKSUM(NEWID())) % 495000) + 5000,

        -- Loan type variation
        CASE
            WHEN @i % 3 = 0 THEN 'Mortgage'
            WHEN @i % 3 = 1 THEN 'Personal'
            ELSE 'Business'
        END
    );

    SET @i = @i + 1;
END;



-- Insert into transaction table
SET @i = 1;

WHILE @i <= 500
BEGIN
    INSERT INTO Transactions (TxnID, CustomerID, Amount, TxnType, Date)
    VALUES (
        @i,
        -- Assign transactions to customers 1–100
        ((@i - 1) % 100) + 1,

        -- Amount: vary between 100 and 50,000
        (ABS(CHECKSUM(NEWID())) % 50000) + 100,

        -- Transaction type: alternate Domestic/International
        CASE WHEN @i % 4 = 0 THEN 'International' ELSE 'Domestic' END,

        -- Random date between 2023-01-01 and 2026-03-01
        DATEADD(DAY, ABS(CHECKSUM(NEWID())) % 1150, '2023-01-01')
    );

    SET @i = @i + 1;
END;




-- Insert 10 records into SanctionsList
INSERT INTO SanctionsList (EntityName, Country)
VALUES
('John Doe', 'USA'),
('XYZ Trading Ltd.', 'Russia'),
('ABC Bank', 'Iran'),
('Global Shipping Corp', 'North Korea'),
('Maria Ivanova', 'Ukraine'),
('Desert Oil Co.', 'Libya'),
('Hassan Ali', 'Pakistan'),
('Eastern Finance Ltd.', 'China'),
('Banco del Sur', 'Venezuela'),
('Ahmed Khalid', 'Sudan');