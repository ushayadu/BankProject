from datetime import datetime

import sqlalchemy
import mysql.connector as connection
import pandas as pd
import logging as lg


class Mysql_Database():
    def load_data(self, host, username, password, schema_name):
        try:
            file_path = "bankAccountdde24ad.json"
            df = pd.read_json(file_path)
            self.create_schema(host, username, password, schema_name)
            engine = sqlalchemy.create_engine(
                "mysql+pymysql://" + username + ":" + password + "@" + host + "/" + schema_name)
            df.to_sql(name="bank_account", con=engine, schema=schema_name, if_exists="replace")
            return ("Data loaded successfully!!!")
        except Exception as e:
            return str(e)

    def create_schema(self, host, username, password, schema_name):
        try:
            mydb = connection.connect(host=host, user=username,
                                      passwd=password, use_pure=True)
            query = f"CREATE DATABASE IF NOT EXISTS {schema_name}"
            lg.info(query)
            cursor = mydb.cursor()
            cursor.execute(query)
            mydb.close()
            return ("Database Created")
        except Exception as e:
            mydb.close()
            return str(e)

    def get_transactions_by_date(self, host, username, password, schema_name, date):
        try:
            mydb = connection.connect(host=host, user=username,
                                      passwd=password, use_pure=True, database=schema_name)
            try:
                datetime.strptime(date, "%d-%m-%y")
                print("This is the correct date string format.")
            except ValueError as e:
                print("This is the incorrect date string format. It should be %d-%m-%Y")
                return str(e)
            query = f"SELECT * FROM bank_account where Date = DATE_FORMAT('{date}','%d-%m-%y %00:%00:%00');"
            cursor = mydb.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            mydb.close()
            return result
        except Exception as e:
            mydb.close()
            return str(e)

    def get_balance_by_date(self, host, username, password, schema_name, date):
        try:
            mydb = connection.connect(host=host, user=username,
                                      passwd=password, use_pure=True, database=schema_name)
            try:
                datetime.strptime(date, "%d-%m-%y")
                print("This is the correct date string format.")
            except ValueError as e:
                print("This is the incorrect date string format. It should be %d-%m-%Y")
                return str(e)
            query = f"SELECT `Balance AMT` FROM bank_account where Date = DATE_FORMAT('{date}','%d-%m-%y %00:%00:%00');"
            cursor = mydb.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            mydb.close()
            return result
        except Exception as e:
            mydb.close()
            return str(e)

    def get_details_by_id(self, host, username, password, schema_name, id):
        try:
            mydb = connection.connect(host=host, user=username,
                                      passwd=password, use_pure=True, database=schema_name)

            query = f"SELECT * FROM bank_account where `Account No` = '{id}';"
            print(query)
            cursor = mydb.cursor()
            cursor.execute(query)
            result = cursor.fetchall()
            mydb.close()
            return result
        except Exception as e:
            mydb.close()
            return str(e)

    def get_last_index(self, host, username, password, schema_name):
        try:
            mydb = connection.connect(host=host, user=username,
                                      passwd=password, use_pure=True, database=schema_name)
            query = f"SELECT `index` from bank_account ORDER BY `index` DESC LIMIT 1;"
            print(query)
            cursor = mydb.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            mydb.close()
            return result
        except Exception as e:
            mydb.close()
            return str(e)

    def get_last_balance_from_id(self, host, username, password, schema_name, account_no):
        try:
            mydb = connection.connect(host=host, user=username,
                                      passwd=password, use_pure=True, database=schema_name)
            query = f"SELECT `Balance AMT` FROM Callify.bank_account where `Account No` = '{account_no}' ORDER BY `index` DESC LIMIT 1 ;"
            print(query)
            cursor = mydb.cursor()
            cursor.execute(query)
            result = cursor.fetchone()
            mydb.close()
            return result
        except Exception as e:
            mydb.close()
            return str(e)

    def deposit(self, host, username, password, schema_name, account_no, amount, date, transaction_details):
        try:
            mydb = connection.connect(host=host, user=username,
                                      passwd=password, use_pure=True, database=schema_name)
            print(account_no)
            print(amount)
            print(date)
            print(transaction_details)
            index = int(self.get_last_index(host, username, password, schema_name)[0]) + 1
            print(index)
            balance = float(
                self.get_last_balance_from_id(host, username, password, schema_name, account_no)[0].replace(',',
                                                                                                            '')) + float(
                amount)
            print(balance)
            query = f"INSERT INTO Callify.bank_account(`index`,`Account No`, `Date`, `Transaction Details`,`Value Date`,`Withdrawal AMT`, `Deposit AMT`, `Balance AMT`) VALUES({index},'{account_no}', DATE_FORMAT('{date}','%d-%m-%y %00:%00:%00'), '{transaction_details}', DATE_FORMAT('{date}','%y-%b-%d'), '','{amount}','{balance}');"
            print(query)
            cursor = mydb.cursor()
            cursor.execute(query)
            mydb.commit()
            mydb.close()
            return ("Value Inserted")
        except Exception as e:
            mydb.close()
            return str(e)
