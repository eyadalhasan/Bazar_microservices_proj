from flask import Flask, jsonify, request
import sqlite3
import threading

app = Flask(__name__)

# Create a thread-local storage object to hold connections
connections = threading.local()

# Function to create a new SQLite connection for each thread
def get_db():
    if not hasattr(connections, 'db'):
        connections.db = sqlite3.connect('catalog.db')
        connections.db.row_factory = sqlite3.Row
    return connections.db

# Close the database connection after each request to release resources
@app.teardown_appcontext
def close_db(error):
    if hasattr(connections, 'db'):
        connections.db.close()

@app.route('/query/item/<id>', methods=['GET'])
def query_book_by_id(id):
    # Check if id is an integer
    if not id.isdigit():
        return jsonify({"message": "Book ID must be a number"}), 400

    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT * FROM books WHERE id=?", (id,))
    book = cursor.fetchone()
    cursor.close()
    if book:
        return jsonify(dict(book)), 200
    else:
        return jsonify({"error": "Book not found"}), 404

@app.route('/query/topic/<topic>', methods=['GET'])
def query_books_by_topic(topic):
    db = get_db()
    cursor = db.cursor()
    cursor.execute("SELECT * FROM books WHERE topic=?", (topic,))
    books = cursor.fetchall()
    cursor.close()
    return jsonify([dict(book) for book in books])

@app.route('/update/<int:id>', methods=['PUT'])
def update_book(id):
    # Example of updating book data (price and quantity)
    # Extract new price and quantity from request data
    new_price = request.json.get('price')
    new_quantity = request.json.get('quantity')

    if new_price is None and new_quantity is None:
        return jsonify({"error": "No data provided for update"}), 400

    db = get_db()
    cursor = db.cursor()

    # Update the book with the provided ID
    if new_price is not None:
        cursor.execute("UPDATE books SET price=? WHERE id=?", (new_price, id))
    if new_quantity is not None:
        cursor.execute("UPDATE books SET quantity=? WHERE id=?", (new_quantity, id))

    db.commit()
    cursor.close()

    # Fetch and return the updated book details
    cursor = db.cursor()
    cursor.execute("SELECT * FROM books WHERE id=?", (id,))
    book = cursor.fetchone()
    cursor.close()

    if book:
        return jsonify(dict(book)), 200
    else:
        return jsonify({"error": "Book not found"}), 404

if __name__ == '__main__':
    app.run(debug=True, port=5001)
