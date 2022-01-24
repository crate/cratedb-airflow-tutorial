CREATE TABLE IF NOT EXISTS doc.sp500 (
<<<<<<< HEAD
	closing_date TIMESTAMP, 
	ticker TEXT, 
	adjusted_close FLOAT
);
=======
	closing_date TIMESTAMP,
	ticker TEXT,
	adjusted_close FLOAT,
	PRIMARY KEY (closing_date, ticker)
);
>>>>>>> 7b2f65a71175acd05c7d723a6b6b8f73d2a48f43
