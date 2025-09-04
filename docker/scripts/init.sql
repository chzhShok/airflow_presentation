CREATE TABLE IF NOT EXISTS test_table (
    id SERIAL PRIMARY KEY,
    created_date DATE DEFAULT CURRENT_DATE,
    data_text TEXT
);
