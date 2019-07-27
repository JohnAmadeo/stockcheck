from flask import Flask, render_template
from stockcheck import checkStock
import os

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, World!'

@app.route('/stock/<stock>')
def getStockData(stock):
    return render_template(
        'stock.html',
        data={'stats': checkStock(stock), 'stock': stock.upper()}
    )

if __name__ == '__main__':
    # Bind to PORT if defined, otherwise default to 5000.
    port = int(os.environ.get('PORT', 5000))
    app.run(host='0.0.0.0', port=port)