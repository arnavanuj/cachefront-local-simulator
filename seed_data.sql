CREATE USER IF NOT EXISTS 'app_user'@'%' IDENTIFIED BY 'app_password';
CREATE USER IF NOT EXISTS 'debezium'@'%' IDENTIFIED BY 'dbz';

GRANT ALL PRIVILEGES ON appdb.* TO 'app_user'@'%';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
GRANT ALL PRIVILEGES ON appdb.* TO 'debezium'@'%';
FLUSH PRIVILEGES;

CREATE TABLE IF NOT EXISTS users (
    id INT PRIMARY KEY,
    name VARCHAR(128) NOT NULL,
    email VARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO users (id, name, email, status) VALUES
    (1, 'Alice Nguyen', 'alice@example.com', 'active'),
    (2, 'Bob Smith', 'bob@example.com', 'active'),
    (3, 'Carlos Diaz', 'carlos@example.com', 'inactive'),
    (4, 'Diana Patel', 'diana@example.com', 'active'),
    (5, 'Ethan Clark', 'ethan@example.com', 'suspended')
ON DUPLICATE KEY UPDATE
    name = VALUES(name),
    email = VALUES(email),
    status = VALUES(status);
