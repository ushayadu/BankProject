from flask import Flask, jsonify, request
from common.mysql_database import Mysql_Database

app = Flask(__name__)
host = "mysql-database.chgvgul4nzuu.us-east-2.rds.amazonaws.com"
username = "root"
password = "Ankur1234"
schema_name = "Callify"
mysqldb = Mysql_Database()


@app.route("/health-check", methods=["GET"])
def health_check():
    return jsonify("Server is up!!!")


@app.route("/load-data", methods=["GET"])
def load_data():
    result = mysqldb.load_data(host, username, password, schema_name)
    return jsonify(result)


@app.route("/transactions/<date>", methods=["GET"])
def get_transaction_by_date(date):
    transactions = mysqldb.get_transactions_by_date(host, username, password, schema_name, date)
    return jsonify(transactions)


@app.route("/balance/<date>", methods=["GET"])
def get_balance_by_date(date):
    balance = mysqldb.get_balance_by_date(host, username, password, schema_name, date)
    return jsonify(balance)


@app.route("/details/<id>", methods=["GET"])
def get_details_by_date(id):
    details = mysqldb.get_details_by_id(host, username, password, schema_name, id)
    return jsonify(details)


@app.route("/deposit", methods=["POST"])
def deposit():
    if request.method == "POST":
        account_no = request.json.get("account_no")
        amount = request.json.get("amount")
        date = request.json.get("date")
        transaction_details = request.json.get("transaction_details")
        result = mysqldb.deposit(host, username, password, schema_name, account_no, amount, date, transaction_details)
        return jsonify(result)
    else:
        return jsonify("This method is not allowed!!!")


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
