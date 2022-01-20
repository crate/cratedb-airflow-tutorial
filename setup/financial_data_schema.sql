CREATE TABLE IF NOT EXISTS doc.sp500 (
	closing_date TIMESTAMP,
	ticker TEXT,
	adjusted_close FLOAT,
	PRIMARY KEY (closing_date, ticker)
);
