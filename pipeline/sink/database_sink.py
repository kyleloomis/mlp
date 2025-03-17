"""
Database manager for MLP pipeline with improved schema design
"""

import sqlite3
from typing import Dict, Any, Union, List

import pandas as pd

from common.database_manager import DatabaseManager
from pipeline.config.run_configuration import RunConfiguration


class DatabaseSink(DatabaseManager):
    """Manager for SQLite database operations with improved schema for nested data"""

    def __init__(self, config: RunConfiguration):
        super().__init__(config.db_path)
        self.setup()

    def setup(self):
        """Initialize the database schema with proper relationships"""
        self.logger.info(f"Initializing database at {self.db_path}")

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Create the main firms table
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS firms (
                firm_crd_nb INTEGER PRIMARY KEY,
                sec_nb TEXT,
                business_name TEXT,
                full_legal_name TEXT,
                address TEXT,
                phone_number TEXT,
                employee_count INTEGER,
                signatory TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')

            # Create compensation arrangements table with FK relationship
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS compensation_arrangements (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_crd_nb INTEGER NOT NULL,
                arrangement TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (firm_crd_nb) REFERENCES firms(firm_crd_nb) ON DELETE CASCADE
            )
            ''')

            # Create client types table with FK relationship
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS client_types (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_crd_nb INTEGER NOT NULL,
                client_type TEXT NOT NULL,
                aum_value INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (firm_crd_nb) REFERENCES firms(firm_crd_nb) ON DELETE CASCADE
            )
            ''')

            # Create private funds table with FK relationship
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS private_funds (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                firm_crd_nb INTEGER NOT NULL,
                fund_name TEXT NOT NULL,
                fund_id TEXT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (firm_crd_nb) REFERENCES firms(firm_crd_nb) ON DELETE CASCADE,
                UNIQUE(firm_crd_nb, fund_id)
            )
            ''')

            # Create indexes for better query performance
            cursor.execute(
                'CREATE INDEX IF NOT EXISTS idx_compensation_firm_crd ON compensation_arrangements(firm_crd_nb)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_client_types_firm_crd ON client_types(firm_crd_nb)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_private_funds_firm_crd ON private_funds(firm_crd_nb)')

            # Enable foreign keys
            cursor.execute('PRAGMA foreign_keys = ON')

            conn.commit()

    def write(self, firm_data: Dict[int, Dict[str, Any]]):
        """Store firm data in the database with proper handling of nested fields

        Args:
            firm_data: Dictionary with firm CRD as key and firm data as value
        """
        self.logger.info(f"Storing data for {len(firm_data)} firms")

        with sqlite3.connect(self.db_path) as conn:
            # Enable foreign keys
            conn.execute('PRAGMA foreign_keys = ON')
            cursor = conn.cursor()

            for firm_crd, data in firm_data.items():
                # Start a transaction
                cursor.execute('BEGIN TRANSACTION')
                try:
                    # Insert or update firm data in the firms table
                    cursor.execute('''
                    INSERT INTO firms 
                    (firm_crd_nb, sec_nb, business_name, full_legal_name, address, phone_number, employee_count, signatory, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    ON CONFLICT(firm_crd_nb) DO UPDATE SET
                        sec_nb = excluded.sec_nb,
                        business_name = excluded.business_name,
                        full_legal_name = excluded.full_legal_name,
                        address = excluded.address,
                        phone_number = excluded.phone_number,
                        employee_count = excluded.employee_count,
                        signatory = excluded.signatory,
                        updated_at = CURRENT_TIMESTAMP
                    ''', (
                        firm_crd,
                        data.get('sec_nb'),
                        data.get('business_name'),
                        data.get('full_legal_name'),
                        data.get('address'),
                        data.get('phone_number'),
                        data.get('employee_count'),
                        data.get('signatory')
                    ))

                    # Handle compensation arrangements (expects a string that can be split)
                    self._write_compensation_arrangements(cursor, firm_crd, data.get('compensation_arrangements', ''))

                    # Handle client types (expects a dictionary)
                    self._write_client_types(cursor, firm_crd, data.get('client_types', {}))

                    # Handle private funds (expects a dictionary of fund name -> fund id)
                    self._write_private_funds(cursor, firm_crd, data.get('private_funds', {}))

                    # Commit the transaction
                    cursor.execute('COMMIT')
                except Exception as e:
                    # Rollback in case of error
                    cursor.execute('ROLLBACK')
                    self.logger.error(f"Error storing data for firm {firm_crd}: {str(e)}")
                    raise e

    def _write_compensation_arrangements(self, cursor: sqlite3.Cursor, firm_crd: str, arrangements: Union[str, list]):
        """Store compensation arrangements in the database

        Args:
            cursor: SQLite cursor
            firm_crd: Firm CRD number
            arrangements: List of compensation arrangements or a comma-separated string
        """
        # Delete existing arrangements for this firm
        cursor.execute('DELETE FROM compensation_arrangements WHERE firm_crd_nb = ?', (firm_crd,))

        # Ensure arrangements are always a list
        if isinstance(arrangements, list):
            arrangement_list = [arr.strip() for arr in arrangements if isinstance(arr, str)]
        elif isinstance(arrangements, str):
            arrangement_list = [arr.strip() for arr in arrangements.split(',') if arr.strip()]
        else:
            arrangement_list = []

        # Insert into DB
        for arrangement in arrangement_list:
            cursor.execute(
                '''INSERT INTO compensation_arrangements (firm_crd_nb, arrangement) VALUES (?, ?)''',
                (firm_crd, arrangement)
            )

    def _write_client_types(self, cursor: sqlite3.Cursor, firm_crd: str, client_types: Dict[str, Union[int, str]]):
        """Store client types and AUM values in the database

        Args:
            cursor: SQLite cursor
            firm_crd: Firm CRD number
            client_types: Dictionary of client type -> AUM value
        """
        # Delete existing client types for this firm
        cursor.execute('DELETE FROM client_types WHERE firm_crd_nb = ?', (firm_crd,))

        # Insert new client types
        for client_type, aum_value in client_types.items():
            # Convert AUM value to integer if it's not already
            if isinstance(aum_value, str):
                try:
                    aum_value = int(aum_value.replace(',', ''))
                except ValueError:
                    aum_value = 0

            cursor.execute('''
            INSERT INTO client_types (firm_crd_nb, client_type, aum_value)
            VALUES (?, ?, ?)
            ''', (firm_crd, client_type, aum_value))

    def _write_private_funds(self, cursor: sqlite3.Cursor, firm_crd: str, private_funds: Dict[str, str]):
        """Store private funds in the database

        Args:
            cursor: SQLite cursor
            firm_crd: Firm CRD number
            private_funds: Dictionary of fund name -> fund ID
        """
        # Delete existing private funds for this firm
        cursor.execute('DELETE FROM private_funds WHERE firm_crd_nb = ?', (firm_crd,))

        # Insert new private funds
        for fund_name, fund_id in private_funds.items():
            cursor.execute('''
            INSERT INTO private_funds (firm_crd_nb, fund_name, fund_id)
            VALUES (?, ?, ?)
            ''', (firm_crd, fund_name, fund_id))

    def query_all(self) -> pd.DataFrame:
        """Query all data for reporting with SQL joins returning normalized data"""
        self.logger.info("Querying data from all tables...")

        with sqlite3.connect(self.db_path) as conn:
            query = '''
            SELECT 
                f.firm_crd_nb,
                f.sec_nb,
                f.business_name,
                f.full_legal_name,
                f.address,
                f.phone_number,
                f.employee_count,
                f.signatory,
                f.created_at,
                f.updated_at,
                ca.arrangement,
                ct.client_type,
                ct.aum_value,
                pf.fund_name,
                pf.fund_id
            FROM firms f
            LEFT JOIN compensation_arrangements ca ON f.firm_crd_nb = ca.firm_crd_nb
            LEFT JOIN client_types ct ON f.firm_crd_nb = ct.firm_crd_nb
            LEFT JOIN private_funds pf ON f.firm_crd_nb = pf.firm_crd_nb
            ORDER BY f.firm_crd_nb
            '''

            result = pd.read_sql_query(query, conn)

            # Fill NaN values for string columns only
            for col in result.select_dtypes(include=['object']).columns:
                result[col].fillna('None', inplace=True)

            return result

    def fetch_compensation(self, firm_crd_nb: int) -> List[str]:
        """Fetch compensation arrangements for a firm"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT arrangement FROM compensation_arrangements WHERE firm_crd_nb = ?", (firm_crd_nb,))
            return [row[0] for row in cursor.fetchall()]

    def fetch_client_types(self, firm_crd_nb: int) -> Dict[str, int]:
        """Fetch client types and AUM"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT client_type, aum_value FROM client_types WHERE firm_crd_nb = ?", (firm_crd_nb,))
            return {row[0]: row[1] for row in cursor.fetchall()}

    def fetch_private_funds(self, firm_crd_nb: int) -> List[Dict[str, str]]:
        """Fetch private funds"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT fund_name, fund_id FROM private_funds WHERE firm_crd_nb = ?", (firm_crd_nb,))
            return [{"name": row[0], "identification_number": row[1]} for row in cursor.fetchall()]
