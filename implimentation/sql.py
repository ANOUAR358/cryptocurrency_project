import mysql.connector

# Establish connection to the MySQL database
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="358WNMGF",
    database="crypto"
)
cursor = connection.cursor()

# Corrected SQL query to create the table
cursor.execute("""
CREATE TABLE IF NOT EXISTS sentiment (
    id INT AUTO_INCREMENT PRIMARY KEY,  -- Unique ID for each record
    source TEXT,                -- Source (Twitter or NewsAPI)
    content TEXT,                      -- Content of the tweet or article
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Time of insertion
);
""")



cursor.execute("""
    CREATE TABLE IF NOT EXISTS crypto_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        Coin VARCHAR(50),
        Open DECIMAL(20, 8),
        High DECIMAL(20, 8),
        Low DECIMAL(20, 8),
        Close DECIMAL(20, 8),
        Volume DECIMAL(20, 8),
        Market_Cap DECIMAL(30,10),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""")


cursor.execute("""
    CREATE TABLE IF NOT EXISTS dominance (
        id INT AUTO_INCREMENT PRIMARY KEY,
        BTC_Dominance DECIMAL(5,2),  -- BTC dominance in percentage
        ETH_Dominance DECIMAL(5,2),  -- ETH dominance in percentage
        Altcoin_Dominance DECIMAL(5,2),  -- Altcoin dominance in percentage
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Time of insertion
    );
""")


# Create GoldPrice table
cursor.execute("""
CREATE TABLE IF NOT EXISTS GoldPrice (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Timestamp DATETIME,
    Open DECIMAL(20, 8),
    High DECIMAL(20, 8),
    Low DECIMAL(20, 8),
    Close DECIMAL(20, 8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Time of insertion
);
""")

# Create InterestRate table
cursor.execute("""
CREATE TABLE IF NOT EXISTS InterestRate (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Timestamp DATETIME,
    open DECIMAL(20, 8),
    high DECIMAL(20, 8),
    low DECIMAL(20, 8),
    close DECIMAL(20, 8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Time of insertion
);
""")

# Create stocksPrices table
cursor.execute("""
CREATE TABLE IF NOT EXISTS stocksPrices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Timestamp DATETIME,
    Open DECIMAL(20, 8),
    High DECIMAL(20, 8),
    Low DECIMAL(20, 8),
    Close DECIMAL(20, 8),
    Volume DECIMAL(20, 8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Time of insertion
);
""")

# Create BlockchainsInfo table
cursor.execute("""
CREATE TABLE IF NOT EXISTS blockchain_statistics (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Timestamp DATETIME,
    Coin VARCHAR(50),
    Block_Reward_Static DECIMAL(20, 8),
    Consensus_Mechanism VARCHAR(50),
    Difficulty VARCHAR(50),
    Hashrate_24h VARCHAR(50),
    Pending_Transactions INT,
    Reduction_Rate VARCHAR(50),
    Total_Blocks INT,
    Total_Transactions VARCHAR(50),
    Tps_24h DECIMAL(20, 8),
    First_Block_Timestamp DATETIME,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
""")

# Create ExchangeRate table
cursor.execute("""
CREATE TABLE IF NOT EXISTS ExchangeRate (
    id INT AUTO_INCREMENT PRIMARY KEY,
    Timestamp DATETIME,
    name VARCHAR(50),
    unit VARCHAR(10),
    value DECIMAL(20, 8),
    type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- Time of insertion
);
""")

print("Tables created successfully!")

# Close the connection
cursor.close()
connection.close()