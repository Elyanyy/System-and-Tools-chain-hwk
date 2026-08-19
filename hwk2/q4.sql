ALTER TABLE news.google_news
ADD COLUMN category VARCHAR(50);
UPDATE news.google_news
SET category = 'technology'
WHERE category IS NULL;