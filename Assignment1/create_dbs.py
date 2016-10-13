import sqlite3

if __name__ == '__main__':
    conn = sqlite3.connect('baseball.db')
    c = conn.cursor()
    c.execute(''' CREATE TABLE baseball_stats
		    (player_name text, 
		    games_played real, 
		    average real, 
                    salary real) ''')
    conn.commit()
    conn.close()

    conn2 = sqlite3.connect("stocks.db")
    c2 = conn2.cursor()
    c2.execute(''' CREATE TABLE stock_stats
	(company_name text, 
	ticker text,
	country text,
	price real,
	exchange_rate real,
	shares_outstanding real,
	net_income real,
	market_value real,
	pe_ratio real) ''')
    conn2.commit()
    conn2.close()
