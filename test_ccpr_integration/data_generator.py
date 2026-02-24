#!/usr/bin/env python3
"""
CCPR Integration Test - Data Generator

Generate test data for various table types and indexes.
"""

import random
import string
import json
import uuid
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any
import logging

logger = logging.getLogger(__name__)


def random_string(length: int = 10) -> str:
    """Generate random string"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def random_text(min_words: int = 5, max_words: int = 20) -> str:
    """Generate random text for fulltext index testing"""
    words = [
        "database", "index", "search", "query", "optimization", "performance",
        "replication", "cluster", "distributed", "storage", "memory", "cache",
        "transaction", "commit", "rollback", "consistency", "availability",
        "partition", "shard", "vector", "embedding", "machine", "learning",
        "artificial", "intelligence", "data", "analytics", "processing",
        "中文", "数据库", "索引", "搜索", "查询", "优化", "复制", "集群",
        "分布式", "存储", "内存", "缓存", "事务", "提交", "回滚", "一致性"
    ]
    num_words = random.randint(min_words, max_words)
    return " ".join(random.choices(words, k=num_words))


def random_vector(dim: int = 8) -> str:
    """Generate random vector for IVF index testing"""
    vec = [round(random.uniform(-1, 1), 4) for _ in range(dim)]
    return json.dumps(vec)


def random_email() -> str:
    """Generate random email"""
    domains = ["gmail.com", "yahoo.com", "outlook.com", "example.com"]
    return f"{random_string(8)}@{random.choice(domains)}"


def random_date(start_year: int = 2020, end_year: int = 2025) -> str:
    """Generate random date"""
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).strftime('%Y-%m-%d')


def escape_sql_string(s: str) -> str:
    """Escape string for SQL"""
    if s is None:
        return "NULL"
    return s.replace("'", "''").replace("\\", "\\\\")


# =============================================================================
# Table Definitions
# =============================================================================

class TableDefinition:
    """Base class for table definitions"""
    
    def __init__(self, name: str, db_name: str):
        self.name = name
        self.db_name = db_name
        self.full_name = f"`{db_name}`.`{name}`"
    
    def get_create_sql(self) -> str:
        raise NotImplementedError
    
    def get_index_sqls(self) -> List[str]:
        return []
    
    def generate_insert_sql(self, count: int = 1) -> str:
        raise NotImplementedError
    
    def generate_insert_values(self, count: int = 1) -> List[Tuple]:
        raise NotImplementedError


class BasicTable(TableDefinition):
    """Basic table with primary key and secondary index"""
    
    def get_create_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {self.full_name} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                value INT DEFAULT 0,
                status VARCHAR(20) DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
    
    def get_index_sqls(self) -> List[str]:
        return [
            f"CREATE INDEX idx_{self.name}_name ON {self.full_name} (name)",
            f"CREATE INDEX idx_{self.name}_status ON {self.full_name} (status)"
        ]
    
    def generate_insert_sql(self, count: int = 1) -> str:
        values = []
        for _ in range(count):
            name = escape_sql_string(random_string(20))
            value = random.randint(0, 10000)
            status = random.choice(['active', 'inactive', 'pending', 'archived'])
            values.append(f"('{name}', {value}, '{status}')")
        
        return f"INSERT INTO {self.full_name} (name, value, status) VALUES {', '.join(values)}"


class FulltextTable(TableDefinition):
    """Table with fulltext index for text search"""
    
    def get_create_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {self.full_name} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(500) NOT NULL,
                content TEXT,
                summary TEXT,
                author VARCHAR(100),
                category VARCHAR(50),
                tags VARCHAR(500),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
    
    def get_index_sqls(self) -> List[str]:
        return [
            f"CREATE FULLTEXT INDEX ftidx_{self.name}_content ON {self.full_name} (title, content)",
            f"CREATE FULLTEXT INDEX ftidx_{self.name}_summary ON {self.full_name} (summary)",
            f"CREATE INDEX idx_{self.name}_author ON {self.full_name} (author)"
        ]
    
    def generate_insert_sql(self, count: int = 1) -> str:
        values = []
        for _ in range(count):
            title = escape_sql_string(f"Article {random_string(10)} - {random_text(3, 8)}")
            content = escape_sql_string(random_text(30, 100))
            summary = escape_sql_string(random_text(10, 30))
            author = escape_sql_string(f"Author_{random_string(5)}")
            category = random.choice(['tech', 'science', 'business', 'sports', 'entertainment'])
            tags = escape_sql_string(','.join(random.sample(['hot', 'trending', 'featured', 'archive', 'draft'], k=random.randint(1, 3))))
            values.append(f"('{title}', '{content}', '{summary}', '{author}', '{category}', '{tags}')")
        
        return f"INSERT INTO {self.full_name} (title, content, summary, author, category, tags) VALUES {', '.join(values)}"


class VectorTable(TableDefinition):
    """Table with IVF vector index"""
    
    def __init__(self, name: str, db_name: str, vector_dim: int = 8):
        super().__init__(name, db_name)
        self.vector_dim = vector_dim
    
    def get_create_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {self.full_name} (
                id VARCHAR(64) PRIMARY KEY,
                text_content VARCHAR(500),
                embedding VECF32({self.vector_dim}),
                score FLOAT DEFAULT 0.0,
                category VARCHAR(50),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
    
    def get_index_sqls(self) -> List[str]:
        return [
            f"CREATE INDEX idx_{self.name}_vec USING IVFFLAT ON {self.full_name} (embedding) LISTS = 16 OP_TYPE 'vector_l2_ops'",
            f"CREATE INDEX idx_{self.name}_category ON {self.full_name} (category)"
        ]
    
    def generate_insert_sql(self, count: int = 1) -> str:
        values = []
        for _ in range(count):
            vid = str(uuid.uuid4())[:16]
            text_content = escape_sql_string(random_text(5, 15))
            embedding = random_vector(self.vector_dim)
            score = round(random.uniform(0, 1), 4)
            category = random.choice(['electronics', 'books', 'clothing', 'home', 'sports'])
            values.append(f"('{vid}', '{text_content}', '{embedding}', {score}, '{category}')")
        
        return f"INSERT INTO {self.full_name} (id, text_content, embedding, score, category) VALUES {', '.join(values)}"


class UniqueIndexTable(TableDefinition):
    """Table with unique indexes"""
    
    def get_create_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {self.full_name} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                email VARCHAR(255) NOT NULL,
                username VARCHAR(100) NOT NULL,
                phone VARCHAR(20),
                status VARCHAR(20) DEFAULT 'active',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
    
    def get_index_sqls(self) -> List[str]:
        return [
            f"CREATE UNIQUE INDEX uidx_{self.name}_email ON {self.full_name} (email)",
            f"CREATE UNIQUE INDEX uidx_{self.name}_username ON {self.full_name} (username)",
            f"CREATE INDEX idx_{self.name}_phone ON {self.full_name} (phone)"
        ]
    
    def generate_insert_sql(self, count: int = 1) -> str:
        values = []
        for _ in range(count):
            email = escape_sql_string(random_email())
            username = escape_sql_string(f"user_{random_string(10)}")
            phone = f"+1{random.randint(1000000000, 9999999999)}"
            status = random.choice(['active', 'inactive', 'suspended'])
            values.append(f"('{email}', '{username}', '{phone}', '{status}')")
        
        return f"INSERT INTO {self.full_name} (email, username, phone, status) VALUES {', '.join(values)}"


class ForeignKeyTable(TableDefinition):
    """Table with foreign key constraints"""
    
    def __init__(self, name: str, db_name: str, parent_table: str):
        super().__init__(name, db_name)
        self.parent_table = parent_table
    
    def get_create_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {self.full_name} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                parent_id BIGINT NOT NULL,
                name VARCHAR(100),
                value INT DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (parent_id) REFERENCES `{self.db_name}`.`{self.parent_table}` (id)
            )
        """
    
    def get_index_sqls(self) -> List[str]:
        return [
            f"CREATE INDEX idx_{self.name}_parent ON {self.full_name} (parent_id)"
        ]


class CompositeTable(TableDefinition):
    """Table with composite indexes and multiple data types"""
    
    def get_create_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {self.full_name} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                user_id INT NOT NULL,
                product_id INT NOT NULL,
                quantity INT DEFAULT 1,
                price DECIMAL(10, 2),
                status VARCHAR(20) DEFAULT 'pending',
                order_date DATE,
                notes TEXT,
                metadata JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
    
    def get_index_sqls(self) -> List[str]:
        return [
            f"CREATE INDEX idx_{self.name}_user_product ON {self.full_name} (user_id, product_id)",
            f"CREATE INDEX idx_{self.name}_status_date ON {self.full_name} (status, order_date)",
            f"CREATE INDEX idx_{self.name}_price ON {self.full_name} (price)"
        ]
    
    def generate_insert_sql(self, count: int = 1) -> str:
        values = []
        for _ in range(count):
            user_id = random.randint(1, 10000)
            product_id = random.randint(1, 5000)
            quantity = random.randint(1, 10)
            price = round(random.uniform(1, 1000), 2)
            status = random.choice(['pending', 'paid', 'shipped', 'delivered', 'cancelled'])
            order_date = random_date()
            notes = escape_sql_string(random_text(5, 15))
            metadata = escape_sql_string(json.dumps({"source": random.choice(["web", "mobile", "api"])}))
            values.append(f"({user_id}, {product_id}, {quantity}, {price}, '{status}', '{order_date}', '{notes}', '{metadata}')")
        
        return f"INSERT INTO {self.full_name} (user_id, product_id, quantity, price, status, order_date, notes, metadata) VALUES {', '.join(values)}"


class HybridSearchTable(TableDefinition):
    """Table with both fulltext and vector indexes for hybrid search"""
    
    def __init__(self, name: str, db_name: str, vector_dim: int = 8):
        super().__init__(name, db_name)
        self.vector_dim = vector_dim
    
    def get_create_sql(self) -> str:
        return f"""
            CREATE TABLE IF NOT EXISTS {self.full_name} (
                id BIGINT AUTO_INCREMENT PRIMARY KEY,
                title VARCHAR(500) NOT NULL,
                description TEXT,
                embedding VECF32({self.vector_dim}),
                category VARCHAR(50),
                price DECIMAL(10, 2),
                rating FLOAT DEFAULT 0.0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """
    
    def get_index_sqls(self) -> List[str]:
        return [
            f"CREATE FULLTEXT INDEX ftidx_{self.name}_text ON {self.full_name} (title, description)",
            f"CREATE INDEX idx_{self.name}_vec USING IVFFLAT ON {self.full_name} (embedding) LISTS = 8 OP_TYPE 'vector_cosine_ops'",
            f"CREATE INDEX idx_{self.name}_category ON {self.full_name} (category)",
            f"CREATE INDEX idx_{self.name}_price ON {self.full_name} (price)"
        ]
    
    def generate_insert_sql(self, count: int = 1) -> str:
        values = []
        for _ in range(count):
            title = escape_sql_string(f"Product {random_string(8)} - {random_text(3, 6)}")
            description = escape_sql_string(random_text(20, 50))
            embedding = random_vector(self.vector_dim)
            category = random.choice(['electronics', 'books', 'clothing', 'home', 'sports', 'toys'])
            price = round(random.uniform(5, 500), 2)
            rating = round(random.uniform(1, 5), 1)
            values.append(f"('{title}', '{description}', '{embedding}', '{category}', {price}, {rating})")
        
        return f"INSERT INTO {self.full_name} (title, description, embedding, category, price, rating) VALUES {', '.join(values)}"


def create_test_tables(db_name: str) -> Dict[str, TableDefinition]:
    """Create all test table definitions"""
    return {
        "basic": BasicTable("basic_table", db_name),
        "fulltext": FulltextTable("fulltext_table", db_name),
        "vector": VectorTable("vector_table", db_name),
        "unique": UniqueIndexTable("unique_table", db_name),
        "composite": CompositeTable("composite_table", db_name),
        "hybrid": HybridSearchTable("hybrid_table", db_name),
    }


# =============================================================================
# Alter Table Operations
# =============================================================================

def generate_add_column_sql(table_name: str, db_name: str) -> str:
    """Generate ALTER TABLE ADD COLUMN SQL"""
    col_name = f"col_{random_string(6)}"
    col_type = random.choice([
        "VARCHAR(100)",
        "INT DEFAULT 0",
        "TEXT",
        "DECIMAL(10,2)",
        "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    ])
    return f"ALTER TABLE `{db_name}`.`{table_name}` ADD COLUMN `{col_name}` {col_type}"


def generate_drop_column_sql(table_name: str, db_name: str, column_name: str) -> str:
    """Generate ALTER TABLE DROP COLUMN SQL"""
    return f"ALTER TABLE `{db_name}`.`{table_name}` DROP COLUMN `{column_name}`"


def generate_add_index_sql(table_name: str, db_name: str, columns: List[str]) -> str:
    """Generate CREATE INDEX SQL"""
    idx_name = f"idx_{random_string(8)}"
    cols = ", ".join([f"`{c}`" for c in columns])
    return f"CREATE INDEX `{idx_name}` ON `{db_name}`.`{table_name}` ({cols})"


def generate_drop_index_sql(table_name: str, db_name: str, index_name: str) -> str:
    """Generate DROP INDEX SQL"""
    return f"DROP INDEX `{index_name}` ON `{db_name}`.`{table_name}`"


def generate_rename_column_sql(table_name: str, db_name: str, 
                                old_name: str, new_name: str, col_type: str) -> str:
    """Generate ALTER TABLE RENAME COLUMN SQL"""
    return f"ALTER TABLE `{db_name}`.`{table_name}` CHANGE COLUMN `{old_name}` `{new_name}` {col_type}"


def generate_modify_comment_sql(table_name: str, db_name: str, comment: str) -> str:
    """Generate ALTER TABLE COMMENT SQL"""
    return f"ALTER TABLE `{db_name}`.`{table_name}` COMMENT = '{escape_sql_string(comment)}'"
