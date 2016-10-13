import sqlite3
from collections import deque
import csv
#import create_dbs


class AbstractRecord:
    """ The base class to store records. """
    def __init__(self, name):
        self.name = name

    def __repr__(self):
        return str(self)

class BaseballStatRecord (AbstractRecord):
    """ The class to store baseball state records. """

    def __init__(self, name, salary, games_played, batting_average):
        """ Create a baseball state record for the given information,
        player name is used as the name of the record."""
        AbstractRecord.__init__(self, name)
        self.salary = salary
        self.games_played = games_played
        self.batting_average = batting_average

    def __str__(self):
        return "BaseballStatRecord (%s, %.2f, %d, %.2f)" % \
                (self.name, self.salary, self.games_played, self.batting_average)

class StockStatRecord (AbstractRecord):
    """ The class to store stock state records. """

    def __init__(self, ticker, exchange_country, price, exchange_rate, shares_outstanding, net_incomes, market_value_usd, pe_ratio, company_name=""):
        """ Create a stock state record for the given information,
        ticker is used as the name of the record. """
        AbstractRecord.__init__(self, ticker)
        self.company_name = company_name
        self.exchange_country = exchange_country
        self.price = price
        self.exchange_rate = exchange_rate
        self.shares_outstanding = shares_outstanding
        self.net_incomes = net_incomes
        self.market_value_usd = market_value_usd
        self.pe_ratio = pe_ratio

    def __str__(self):
        return "StockStatRecord (%s, %s, %.2f, %.2f, %.2f, %.2f, %.2f, %.2f)" % \
                (self.name, self.exchange_country, self.price, self.exchange_rate, \
                        self.shares_outstanding, self.net_incomes, self.market_value_usd, \
                        self.pe_ratio)

class AbstractCSVReader:
    """ The base reader to read CSV files. """

    def __init__(self, path):
        """ Create a reader to read the CSV at the given path. """
        self.path = path

    def row_to_record(self, row):
        """ Convert the row to a corresponding data record. """
        raise NotImplementedError()

    def load(self):
        """ Read the CSV file and return a list of records. """
        records = []
        with open(self.path, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    record = self.row_to_record(row)
                    # add to the list of records
                    records.append(record)
                except BadData:
                    # skip the record
                    continue
        return records

class BaseballCSVReader (AbstractCSVReader):
    """ Implement the reader to read the baseball csv files. """

    def __init__(self, path):
        """ Create a reader to read the CSV at the given path. """
        AbstractCSVReader.__init__(self, path)

    def row_to_record(self, row):
        """ Convert the row to a base ball record. """
        try:
            # salary, games_played, batting_average
            name = row['PLAYER']
            salary = float(row['SALARY'])
            games_played = int(row['G'])
            batting_average = float(row['AVG'])
            # validate the data
            if not name:
                raise Exception('name is empty')
            return BaseballStatRecord(name, salary, games_played, batting_average)
        except Exception as e:
            # generate an exception when parsing the data
            # raise a BadData exception
            raise BadData(str(e))

class StocksCSVReader (AbstractCSVReader):
    """ Implement the reader to read the stock csv files. """

    def __init__(self, path):
        """ Create a reader to read the CSV at the given path. """
        AbstractCSVReader.__init__(self, path)

    def row_to_record(self, row):
        """ Convert the row to a stock record. """
        try:
            ticker = row['ticker']
            exchange_country = row['exchange_country']
            company_name = row['company_name']
            price = float(row['price'])
            exchange_rate = float(row['exchange_rate'])
            shares_outstanding = float(row['shares_outstanding'])
            net_incomes = float(row['net_income'])
            market_value_usd = price * exchange_rate * shares_outstanding
            pe_ratio = price * shares_outstanding / net_incomes
            # validate the data
            if not company_name:
                company_name = ""
            if not ticker:
                raise Exception('name is empty')
            if not exchange_country:
                raise Exception('exchange_country is empty')
            return StockStatRecord(ticker, exchange_country, price, exchange_rate, \
                    shares_outstanding, net_incomes, market_value_usd, pe_ratio,\
                    company_name=company_name)
        except Exception as e:
            # generate an exception when parsing the data
            # raise a BadData exception
            raise BadData(str(e))


class BadData(Exception):
    """ The exception to indicate bad data in the CSV file. """
    def __init__(self, msg = ""):
        Exception.__init__(self, msg)


class AbstractDAO:
    def __init__(self, db_name=None):
        self.db_name = db_name
    def insert_records(self, records):
        raise(NotImplementedError)
    def select_all(self):
        raise(NotImplementedError)
    def connect(self):
        conn = sqlite3.connect(self.db_name)
        return conn

class BaseballStatsDAO(AbstractDAO):
    def __init__(self, name):
        AbstractDAO.__init__(self, name) 

    def insert_records(self, baseballStatRecord):
        conn = self.connect()
        cursor = conn.cursor()
        for bsr in baseballStatRecord:
            cursor.execute(''' INSERT INTO baseball_stats VALUES \
                          (?,?,?,?)''', (bsr.name, bsr.games_played, \
                          bsr.batting_average, bsr.salary))
        conn.commit()
        conn.close()

    def select_all(self):
        conn = self.connect()
        cursor = conn.cursor()
        stats_queue = deque()
        for row in cursor.execute(''' SELECT player_name, games_played, \
                                  average, salary FROM baseball_stats;'''):
            stats_queue.append(BaseballStatRecord(name=row[0], \
                                                     games_played=row[1], \
                                                     batting_average=row[2], \
                                                     salary=row[3]))
        conn.close()
        return stats_queue

class StockStatsDAO(AbstractDAO):
    def __init__(self, name):
        AbstractDAO.__init__(self, name) 

    def insert_records(self, stockStatRecord):
        conn = self.connect()
        cursor = conn.cursor()
        for ssr in stockStatRecord:
            cursor.execute(''' INSERT INTO stock_stats VALUES \
                          (?,?,?,?,?,?,?,?,?)''', (ssr.company_name, \
                                                 ssr.name, \
                                                 ssr.exchange_country, \
                                                 ssr.price, \
                                                 ssr.exchange_rate, \
                                                 ssr.shares_outstanding, \
                                                 ssr.net_incomes, \
                                                 ssr.market_value_usd, \
                                                 ssr.pe_ratio))
        conn.commit()
        conn.close()

    def select_all(self):
        conn = self.connect()
        cursor = conn.cursor()
        stats_queue = deque()
        for row in cursor.execute(''' SELECT company_name, ticker, country,
                                  price, exchange_rate, shares_outstanding,
                                  net_income, market_value, pe_ratio
                                  FROM stock_stats;'''):
            stats_queue.append(StockStatRecord(company_name=row[0], \
                                                 ticker=row[1], \
                                                 exchange_country=row[2], \
                                                 price=row[3], \
                                                 exchange_rate=row[4], \
                                                 shares_outstanding=row[5], \
                                                 net_incomes=row[6], \
                                                 market_value_usd=row[7], \
                                                 pe_ratio=row[8]))
        conn.close()
        return stats_queue

if __name__ == '__main__':

    stock_records = StocksCSVReader('StockValuations.csv').load()
    ss = StockStatsDAO("stocks.db")
    ss.insert_records(stock_records)
    stocks = ss.select_all()
    ticker_count = {}
    for stock in stocks:
        key = stock.exchange_country
        if key in ticker_count.keys():
            ticker_count[key] += 1
        else:
            ticker_count[key] = 1

    print "country | ticker_count"
    for key, value in ticker_count.items():
        print str(key) + " " + str(value)

    players = BaseballCSVReader('MLB2008.csv').load()
    bs = BaseballStatsDAO("baseball.db")
    bs.insert_records(players)
    player_records = bs.select_all()
    player_avg = {}
    for player in player_records:
        key = player.batting_average
        if key in player_avg.keys():
            player_avg[key]["salary"] += player.salary
            player_avg[key]["count"] += 1
        else:
            player_avg[key] = {"salary": player.salary, "count": 1}

    print "batting average, salary"
    for key, value in player_avg.items():
        print str(key) +  " " + str(value["salary"]/value["count"])

