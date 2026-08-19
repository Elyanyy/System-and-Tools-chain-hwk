CREATE SCHEMA IF NOT EXISTS news;

CREATE TABLE news.google_news (
    id SERIAL PRIMARY KEY,
    lastBuildDate TIMESTAMP,
    title TEXT NOT NULL,
    link TEXT NOT NULL,
    pubDate TIMESTAMP,
    description TEXT,
    source TEXT
);
