from flask import Flask, jsonify, request
import sqlite3
import threading

app = Flask(__name__)

connections = threading.local()

def get_db():
    if not hasattr(connections, 'db'):
        connections.db = sqlite3.connect('order.db')
        connections.db.row_factory = sqlite3.Row
    return connections.db

@app.teardown_appcontext
def close_db(error):
    if hasattr(connections, 'db'):
        connections.db.close()

@app.route('/buy/<id>/', methods=['PUT'])
def buy_book(id):
    # Check if the provided ID is a valid integer
    try:
        id = int(id)
    except ValueError:
        return jsonify({"message": "Book ID must be a number"}), 400

    order_db = get_db()
    catalog_db = sqlite3.connect('D:\\dosProject\\proj\\bazar_project\\catalog_microservice\\catalog.db')

    with catalog_db:
        cursor = catalog_db.cursor()
        cursor.execute("SELECT * FROM books WHERE id=?", (id,))
        book = cursor.fetchone()
        if book:
            # Access elements of the tuple by their index
            if book[2] > 0:  # Assuming quantity is at index 2
                new_quantity = book[2] - 1
                cursor.execute("UPDATE books SET quantity=? WHERE id=?", (new_quantity, id))
            else:
                return jsonify({"message": "Book out of stock", "success": False}), 400
        else:
            return jsonify({"message": "Book not found", "success": False}), 404

    with order_db:
        cursor = order_db.cursor()
        cursor.execute("INSERT INTO orders (book_id) VALUES (?)", (id,))
    
    return jsonify({"message": "Book purchased successfully", "success": True}), 200

if __name__ == '__main__':
    app.run(debug=True, port=5002)
