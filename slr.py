import os
import sqlite3
import rispy
import argparse
from collections import defaultdict
import re

# Directory paths for sources
directories = ['scopus', 'ieee']  # Example directories

# Database connection
conn = sqlite3.connect('literature_review.db')
cursor = conn.cursor()

# Step 1: Extract unique RIS fields from sources
def extract_ris_fields():
    fields = set()
    for directory in directories:
        path = os.path.join(os.getcwd(), directory)
        if os.path.exists(path):
            files = [f for f in os.listdir(path) if f.endswith('.ris')]
            for file_name in files:
                with open(os.path.join(path, file_name), 'r', encoding='utf-8') as file:
                    entries = rispy.load(file)
                    for entry in entries:
                        fields.update(entry.keys())
    return fields

# create tables
def create_table():
    # Get all unique RIS fields
    ris_fields = extract_ris_fields()

    # Step 2: Create database table with all possible fields
    fields_sql = ", ".join([f"[{field}] TEXT" for field in ris_fields])
    extra_columns = " source TEXT, unique_id INTEGER PRIMARY KEY AUTOINCREMENT, is_duplicate INTEGER DEFAULT 0, is_keyword_match INTEGER DEFAULT 0"
    create_table_sql = f"CREATE TABLE IF NOT EXISTS articles ({fields_sql}, {extra_columns})"
    cursor.execute(create_table_sql)

# Create sequence table if not exists
def initialize_sequence_table():
    cursor.execute("CREATE TABLE IF NOT EXISTS sequences (name TEXT PRIMARY KEY, next_val INTEGER)")
    cursor.execute("INSERT OR IGNORE INTO sequences (name, next_val) VALUES ('article_id_seq', 1)")
    conn.commit()

# initialize_sequence_table()

# Generate next sequence value
def get_next_sequence(name):
    cursor.execute("SELECT next_val FROM sequences WHERE name = ?", (name,))
    next_val = cursor.fetchone()[0]
    cursor.execute("UPDATE sequences SET next_val = next_val + 1 WHERE name = ?", (name,))
    conn.commit()
    return next_val

# Step 3: Insert data into the database
def insert_data():
    for directory in directories:
        path = os.path.join(os.getcwd(), directory)
        if os.path.exists(path):
            files = [f for f in os.listdir(path) if f.endswith('.ris')]
            for file_name in files:
                with open(os.path.join(path, file_name), 'r', encoding='utf-8') as file:
                    entries = rispy.load(file)
                    for entry in entries:
                        columns = list(entry.keys()) + ['source', 'unique_id']
                        values = []
                        for value in entry.values():
                            if isinstance(value, list):
                                values.append(", ".join(map(str, value)))
                            elif isinstance(value, dict):
                                values.append(str(value))
                            else:
                                values.append(str(value))
                        unique_id = get_next_sequence('article_id_seq')
                        values += [directory, unique_id]
                        placeholders = ', '.join(['?'] * len(columns))
                        sql = f"INSERT INTO articles ({', '.join([f'[{col}]' for col in columns])}) VALUES ({placeholders})"
                        cursor.execute(sql, values)
    conn.commit()

def filter_articles(query):
    def parse_query(query):
        query = query.replace('_', ' ')
        query = re.sub(r'\bAND\b', '&&', query, flags=re.IGNORECASE)
        query = re.sub(r'\bOR\b', '||', query, flags=re.IGNORECASE)
        return query

    def build_sql(query):
        query = query.strip('[]').strip()
        pattern = r"\)\s+(AND|OR)\s+\("
        match = re.search(pattern, query, flags=re.IGNORECASE)

        # Detect main boolean operator between grouped expressions
        main_bool_opr = f" {match.group(1)} " if match else " AND "

        groups = re.findall(r'\((.*?)\)', query)
        conditions = []
        
        for group in groups:
            parts = re.split(r'\s*\|\|\s*', group)  # Split by OR first
            title_conditions, abstract_conditions, keyword_conditions = [], [], []
            bool_opr = " OR "  # Default
            
            for kw in parts:
                kw = parse_query(kw.strip())
                sub_parts = re.split(r'\s*&&\s*', kw)  # Split by AND second
                
                if len(sub_parts) > 1:
                    bool_opr = " AND "
                else:
                    sub_parts = re.split(r'\s*\|\|\s*', kw)  # Split by OR second
                for sub_kw in sub_parts:
                        title_conditions.append(f"LOWER(title) LIKE '%{sub_kw.lower()}%'")
                        abstract_conditions.append(f"LOWER(abstract) LIKE '%{sub_kw.lower()}%'")
                        keyword_conditions.append(f"LOWER(keywords) LIKE '%{sub_kw.lower()}%'")
            
            conditions.append(
                f"(({bool_opr.join(title_conditions)}) OR "
                f"({bool_opr.join(abstract_conditions)}) OR "
                f"({bool_opr.join(keyword_conditions)}))"
            )
        
        return main_bool_opr.join(conditions)

    sql_condition = build_sql(query)
    sql = f"SELECT unique_id FROM articles WHERE {sql_condition}"
    print(f"sql {sql}")
    cursor.execute(sql)
    matches = cursor.fetchall()
    print(f"matches {matches} len {len(matches)}")
    cursor.execute("UPDATE articles SET is_keyword_match = 0")
    for match in matches:
        print(f"unique_id {match[0]}")
        cursor.execute("UPDATE articles SET is_keyword_match = 1 WHERE unique_id = ?", (match[0],))
    sql = f"SELECT * FROM articles WHERE is_keyword_match = 1"
    print(f"sql matches {sql}")
    cursor.execute(sql)
    matches = cursor.fetchall()
    conn.commit()
    return matches

# Step 5: Mark duplicates
def mark_duplicates():
    cursor.execute("SELECT unique_id, title, doi FROM articles")
    articles = cursor.fetchall()

    seen = set()
    duplicates = []

    for unique_id, title, doi in articles:
        identifier = title.strip().lower()
        # identifier = (title.strip().lower(), doi.strip().lower() if doi else None)
        if identifier in seen:
            duplicates.append(unique_id)
            # print(f"identifier {identifier}")
        else:
            seen.add(identifier)

    for dup_id in duplicates:
        cursor.execute("UPDATE articles SET is_duplicate = 1 WHERE unique_id = ?", (dup_id,))
    conn.commit()

# mark_duplicates()

# Step 6: Generate output summary
def generate_summary():
    cursor.execute("SELECT COUNT(*) FROM articles")
    total_articles = cursor.fetchone()[0]
    summary = defaultdict(int)
    for directory in directories:
        cursor.execute("SELECT COUNT(*) FROM articles WHERE source = ?", (directory,))
        summary[directory] = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM articles WHERE is_duplicate = 1")
    total_duplicates = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM articles WHERE is_duplicate = 0")
    total_unique = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM articles WHERE is_keyword_match = 1")
    total_keyword_match = cursor.fetchone()[0]
    cursor.execute("SELECT COUNT(*) FROM articles WHERE is_keyword_match = 1 AND is_duplicate = 0")
    relevant = cursor.fetchone()[0]

    print("Summary:")
    print(f"Total Articles: {total_articles}")
    for source, count in summary.items():
        print(f"Source: {source}, Total Articles: {count}")
    print(f"Total Duplicates: {total_duplicates}")
    print(f"Total Unique Articles: {total_unique}")
    print(f"Total Keyword Matches: {total_keyword_match}")
    print(f"Relevant Articles: {relevant}")

# Example usage
# keywords_to_filter = ['blockchain', 'data recovery']
# filtered_articles = filter_articles(keywords_to_filter, logical_operator='AND')
# generate_summary()
# Command-line argument parsing

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--query", help="Execute custom SQL query")
    parser.add_argument("-ov", "--overwrite", action="store_true", help="Overwrite database")
    parser.add_argument("-t", "--table_structure", action="store_true", help="Show table structure")
    parser.add_argument("-d", "--duplicates", action="store_true", help="Filter duplicates and generate summary")
    parser.add_argument("-k", "--keywords", help="Filter articles by keywords and generate summary")
    parser.add_argument("-c", "--clear", action="store_true", help="Clear the database")
    args = parser.parse_args()

    if args.clear:
        cursor.execute("DROP TABLE IF EXISTS articles")
        cursor.execute("DROP TABLE IF EXISTS sequences")
        conn.commit()
        print("Database cleared.")

    if args.overwrite:
        cursor.execute("DROP TABLE IF EXISTS articles")
        cursor.execute("DROP TABLE IF EXISTS sequences")
        conn.commit()
        create_table()
        initialize_sequence_table()
        insert_data()
        print("Database overwritten.")

    if args.table_structure:
        cursor.execute("PRAGMA table_info(articles)")
        print(cursor.fetchall())

    if args.duplicates:
        mark_duplicates()
        cursor.execute("SELECT * FROM articles WHERE is_duplicate = 1")
        print(cursor.fetchall())
        generate_summary()

    if args.keywords:
        filtered = filter_articles(args.keywords)
        print(filtered)
        generate_summary()

    if args.query:
        cursor.execute(args.query)
        print(cursor.fetchall())

    conn.close()
