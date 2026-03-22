-- Customer Risk Assessment with scoring
SELECT
    c.CustomerID,
    c.Name,
    c.CustomerType,
    c.Country,
    c.OnboardingChannel,

-- Risk Score calculation
    (
        CASE
            WHEN s.EntityName IS NOT NULL THEN 100   -- Sanctioned entity
            ELSE 0
        END
        +
        CASE
            WHEN c.Country IN ('Iran','North Korea','Libya','Sudan') THEN 40
            ELSE 0
        END
        +
        CASE
            WHEN c.CustomerType = 'Corporate'  THEN 30
            ELSE 0
        END
        +
        CASE
            WHEN c.OnboardingChannel = 'Online' THEN 20
            ELSE 0
        END
        +
        CASE
            WHEN EXISTS (SELECT 1 FROM Loans l WHERE l.CustomerID = c.CustomerID ) THEN 25
            ELSE 0
        END
        +
        CASE
            WHEN EXISTS (SELECT 1 FROM Transactions t WHERE t.CustomerID = c.CustomerID AND t.Amount > 100000) THEN 15
            ELSE 0
        END
    ) AS RiskScore,

    -- Risk Category based on score
    CASE
        WHEN s.EntityName IS NOT NULL THEN 'Sanctioned Entity'
        WHEN c.country IN ('Iran','North Korea','Libya','Sudan') THEN 'High-Risk Jurisdiction'
        WHEN c.CustomerType = 'Corporate' THEN 'Foreign Corporate Risk'
        WHEN c.OnboardingChannel = 'Online' THEN 'Digital Channel Risk'
        WHEN EXISTS (SELECT 1 FROM Loans l WHERE l.CustomerID = c.CustomerID ) THEN 'Loan Default Risk'
        WHEN EXISTS (SELECT 1 FROM Transactions t WHERE t.CustomerID = c.CustomerID AND t.Amount > 100000) THEN 'Large Transaction Risk'
        ELSE 'Low Risk'
    END AS RiskCategory

FROM Customer c
LEFT JOIN SanctionsList s
    ON c.Name = s.EntityName
    AND c.Country = s.Country;