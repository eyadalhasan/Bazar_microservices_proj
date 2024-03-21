from flask import Flask, jsonify, request
import sqlite3
import threading

app = Flask(__name__)

connections = threading.local()

def get_catalog_db():
    if not hasattr(connections, 'catalog_db'):
        connections.catalog_db = sqlite3.connect('D:\\dosProject\\proj\\bazar_project\\catalog_microservice\\catalog.db')
        connections.catalog_db.row_factory = sqlite3.Row
    return connections.catalog_db

def get_order_db():
    if not hasattr(connections, 'order_db'):
        connections.order_db = sqlite3.connect('D:\\dosProject\\proj\\bazar_project\\order_microservice\\order.db')
        connections.order_db.row_factory = sqlite3.Row
    return connections.order_db

@app.teardown_appcontext
def close_db(error):
    if hasattr(connections, 'catalog_db'):
        connections.catalog_db.close()
    if hasattr(connections, 'order_db'):
        connections.order_db.close()

# Lookup a book using its ID
@app.route('/lookup/<id>', methods=['GET'])
def lookup_book(id):
    # Check if id is an integer
    if not id.isdigit():
        return jsonify({"message": "Book ID must be a number"}), 400

    catalog_db = get_catalog_db()
    with catalog_db:
        cursor = catalog_db.cursor()
        cursor.execute("SELECT * FROM books WHERE id=?", (int(id),))
        book = cursor.fetchone()
        if book:
            return jsonify(dict(book)), 200
        else:
            return jsonify({"message": "Book not found"}), 404

# Search for books using their topic or return all books if 'all' is provided as topic
@app.route('/search/<string:topic>', methods=['GET'])
def search_books(topic):
    catalog_db = get_catalog_db()
    with catalog_db:
        cursor = catalog_db.cursor()
        if topic.lower() == 'all':
            cursor.execute("SELECT * FROM books")
        else:
            cursor.execute("SELECT * FROM books WHERE topic LIKE ?", ('%' + topic + '%',))
        books = cursor.fetchall()
        if books:
            return jsonify([dict(book) for book in books]), 200
        else:
            return jsonify({"message": "No books found"}), 404

# Buy a book
@app.route('/buy/<int:id>/', methods=['PUT'])
def buy_book(id):
    catalog_db = get_catalog_db()
    order_db = get_order_db()

    with catalog_db:
        cursor = catalog_db.cursor()
        cursor.execute("SELECT * FROM books WHERE id=?", (id,))
        book = cursor.fetchone()
        if book:
            if book['quantity'] > 0:
                new_quantity = book['quantity'] - 1
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
    app.run(debug=True, port=5000)
