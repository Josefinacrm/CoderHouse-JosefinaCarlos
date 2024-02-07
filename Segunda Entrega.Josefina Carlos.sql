 CREATE TABLE IF NOT EXISTS indicator_data (
            id INT PRIMARY KEY GENERATED BY DEFAULT AS IDENTITY,
            name VARCHAR(255),
            interval VARCHAR(50),
            unit VARCHAR(50),
            date DATE,
            value FLOAT
        );
        '''