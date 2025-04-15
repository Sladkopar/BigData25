#!/usr/bin/env python3
from cassandra.cluster import Cluster
import logging


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
logger = logging.getLogger(__name__)


def initialize_cassandra():
    try:
        # Connect to Cassandra
        cluster = Cluster(
            ['cassandra-server'],
            port=9042
        )
        session = cluster.connect()


        logger.info("Successfully connected to Cassandra")


        # Create keyspace
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS search_index
            WITH replication = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }
        """)
        logger.info("Created keyspace 'search_index'")


        # Switch to the keyspace
        session.set_keyspace('search_index')


        # Create tables
        tables = {
            'terms': """
                CREATE TABLE IF NOT EXISTS terms (
                    term text PRIMARY KEY,
                    document_frequency int
                )
            """,
            'document_index': """
                CREATE TABLE IF NOT EXISTS document_index (
                    term text,
                    doc_id text,
                    term_frequency int,
                    positions list<int>,
                    PRIMARY KEY (term, doc_id)
                )
            """,
            'document_stats': """
                CREATE TABLE IF NOT EXISTS document_stats (
                    doc_id text PRIMARY KEY,
                    doc_length int,
                    avg_term_length float
                )
            """
        }


        for table_name, query in tables.items():
            session.execute(query)
            logger.info(f"Created table '{table_name}'")


        logger.info("Cassandra schema initialization completed successfully")
        return True


    except Exception as e:
        logger.error(f"Error initializing Cassandra: {str(e)}")
        return False
    finally:
        if 'cluster' in locals():
            cluster.shutdown()


if __name__ == "__main__":
    logger.info("Starting Cassandra initialization...")
    if initialize_cassandra():
        logger.info("Initialization completed successfully")
    else:
        logger.error("Initialization failed")
        sys.exit(1)