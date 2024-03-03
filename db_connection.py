import psycopg2
from psycopg2.extras import DictCursor

def connectDataBase():
    DB_NAME = "CPP"
    DB_USER = "postgres"
    DB_PASS = "123"
    DB_HOST = "localhost"
    DB_PORT = "5432"

    try:
        conn = psycopg2.connect(database=DB_NAME,
                                user=DB_USER,
                                password=DB_PASS,
                                host=DB_HOST,
                                port=DB_PORT,
                                cursor_factory=DictCursor)
        
        create_tables(conn)
        return conn
    except psycopg2.Error as e:
        print(f"Database connection error: {e}")
        raise

def create_tables(conn):
    try:
        cur = conn.cursor()
        # Create Categories table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Categories (
                id_cat SERIAL PRIMARY KEY,
                name VARCHAR(255) UNIQUE NOT NULL
            );
        """)

        # Create Terms table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Terms (
                term VARCHAR(255) PRIMARY KEY,
                num_chars INTEGER NOT NULL
            );
        """)

        # Create Documents table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Documents (
                doc_number SERIAL PRIMARY KEY,
                text TEXT,
                title VARCHAR(255),
                num_chars INTEGER,
                date DATE,
                category VARCHAR(255) REFERENCES Categories(name)
            );
        """)

        # Create Index table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS Index (
                doc_number INTEGER REFERENCES Documents(doc_number),
                term VARCHAR(255) REFERENCES Terms(term),
                term_count INTEGER,
                PRIMARY KEY (doc_number, term)
            );
        """)

        conn.commit()
    except psycopg2.Error as e:
        print(f"Error creating tables: {e}")
        raise
    finally:
        cur.close()

def createCategory(cur, catId, catName):
    cur.execute("INSERT INTO Categories (id_cat, name) VALUES (%s, %s)", (catId, catName))

def createDocument(cur, docId, docText, docTitle, docDate, docCat):
    try:
        # Step 1: Check if the entered category exists in the Categories table
        cur.execute("SELECT name FROM Categories WHERE name = %s", (docCat,))
        result = cur.fetchone()

        if result:
            # The category exists, use its name in the Documents table
            category_name = result['name']

            # Step 2: Insert the document in the database. For num_chars, discard spaces and punctuation marks.
            num_chars = len(''.join(c for c in docText if c.isalnum()))
            cur.execute("""
                INSERT INTO Documents (doc_number, text, title, num_chars, date, category)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (docId, docText, docTitle, num_chars, docDate, category_name))


            # Step 3: Update the potential new terms.
            # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms.
            terms = set([term.lower().strip('.,?!') for term in docText.split()])

            # 3.2 For each term identified, check if the term already exists in the database
            for term in terms:
                cur.execute("INSERT INTO Terms (term, num_chars) VALUES (%s, %s) ON CONFLICT DO NOTHING", (term, len(term)))

            # Step 4: Update the index
            # 4.1 Find all terms that belong to the document
            # 4.2 Create a data structure that stores how many times (count) each term appears in the document
            term_count = {}
            for term in terms:
                term_count[term] = term_count.get(term, 0) + docText.lower().count(term)
                #print(term, term_count[term])

            # 4.3 Insert the term and its corresponding count into the database
            for term, count in term_count.items():
                cur.execute("""
                    INSERT INTO Index (doc_number, term, term_count)
                    VALUES (%s, %s, %s) ON CONFLICT (doc_number, term) DO UPDATE SET term_count = Index.term_count + %s
                """, (docId, term, count, count))

        else:
            print(f"Error: Category '{docCat}' does not exist in the Categories table.")

        

    except psycopg2.Error as e:
        print(f"Error creating document: {e}")
        raise


def deleteDocument(cur, docId):
    try:
        # 1 Query the index based on the document to identify terms
        cur.execute("""
            SELECT term, term_count
            FROM Index
            WHERE doc_number = %s
        """, (docId,))

        term_results = cur.fetchall()

        # 1.1 For each term identified, delete its occurrences in the index for that document
        for term_row in term_results:
            term = term_row['term']
            cur.execute("""
                DELETE FROM Index
                WHERE doc_number = %s AND term = %s
            """, (docId, term))

            # 1.2 Check if there are no more occurrences of the term in another document.
            # If this happens, delete the term from the database.
            cur.execute("""
                SELECT COUNT(*) AS term_count
                FROM Index
                WHERE term = %s
            """, (term,))

            term_count = cur.fetchone()['term_count']

            if term_count == 0:
                # Delete the term from the Terms table if it no longer appears in any document
                cur.execute("DELETE FROM Terms WHERE term = %s", (term,))

        # 2 Delete the document from the database
        cur.execute("DELETE FROM Documents WHERE doc_number = %s", (docId,))

    except psycopg2.Error as e:
        print(f"Error deleting document: {e}")
        raise

    #finally:
        # Close the cursor in the finally block to ensure it is always closed
        #cur.close()


def updateDocument(cur, docId, docText, docTitle, docDate, docCat):
    try:
        # 1. Delete the document
        deleteDocument(cur, docId)

        # 2. Create the document with the same id
        createDocument(cur, docId, docText, docTitle, docDate, docCat)

    except psycopg2.Error as e:
        print(f"Error updating document: {e}")
        raise

def getIndex(cur):
    try:
        # Query the database to return the documents where each term occurs
        cur.execute("""
            SELECT term, title, term_count
            FROM Index
            JOIN Documents ON Index.doc_number = Documents.doc_number
        """)

        result = {}
        for row in cur.fetchall():
            term = row['term']
            title = row['title']
            term_count = row['term_count']

            if term not in result:
                result[term] = f"{title}:{term_count}"
            else:
                result[term] += f", {title}:{term_count}"

        return result

    except psycopg2.Error as e:
        print(f"Error getting inverted index: {e}")
        raise


