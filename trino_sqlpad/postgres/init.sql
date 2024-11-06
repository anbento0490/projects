-- create a nasdaq_order_book schema
CREATE SCHEMA nasdaq_order_book;

SET search_path TO nasdaq_order_book;

-- create a table named message_types
CREATE TABLE message_types (
    message_type CHAR(1) PRIMARY KEY,
    order_book_impact TEXT NOT NULL
);

-- populate message_types table
INSERT INTO message_types (message_type, order_book_impact) VALUES
('A', 'New unattributed limit order'),
('D', 'Order canceled'),
('U', 'Order canceled and replaced'),
('E', 'Full or partial execution; possibly multiple messages for the same original order'),
('X', 'Modified after partial cancellation'),
('F', 'Add attributed order'),
('P', 'Trade message (non-cross)'),
('C', 'Executed in whole or in part at a price different from the initial display price'),
('Q', 'Cross trade message'),
('S', 'System event message');
