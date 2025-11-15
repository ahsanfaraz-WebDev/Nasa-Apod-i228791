-- Switch to the postgres database (Astronomer's default)
\c postgres;

-- Create schema if needed
CREATE SCHEMA IF NOT EXISTS public;

-- NASA APOD Data Table
CREATE TABLE IF NOT EXISTS public.apod_data (
    id SERIAL PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    title VARCHAR(500) NOT NULL,
    url TEXT NOT NULL,
    explanation TEXT,
    media_type VARCHAR(50) DEFAULT 'image',
    hdurl TEXT,
    copyright VARCHAR(200),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster date queries (newest first)
CREATE INDEX IF NOT EXISTS idx_apod_date ON public.apod_data(date DESC);

-- Index for media type filtering
CREATE INDEX IF NOT EXISTS idx_apod_media_type ON public.apod_data(media_type);

-- Index for full-text search on title
CREATE INDEX IF NOT EXISTS idx_apod_title ON public.apod_data USING gin(to_tsvector('english', title));

-- Table and column comments for documentation
COMMENT ON TABLE public.apod_data IS 'NASA Astronomy Picture of the Day data repository';
COMMENT ON COLUMN public.apod_data.id IS 'Auto-incrementing primary key';
COMMENT ON COLUMN public.apod_data.date IS 'Date of the APOD entry (unique)';
COMMENT ON COLUMN public.apod_data.title IS 'Title of the astronomy picture';
COMMENT ON COLUMN public.apod_data.url IS 'URL to the standard resolution image or video';
COMMENT ON COLUMN public.apod_data.explanation IS 'Detailed scientific explanation';
COMMENT ON COLUMN public.apod_data.media_type IS 'Type of media: image or video';
COMMENT ON COLUMN public.apod_data.hdurl IS 'URL to high-definition version (if available)';
COMMENT ON COLUMN public.apod_data.copyright IS 'Copyright holder or Public Domain';

-- Grant permissions to postgres user
GRANT ALL PRIVILEGES ON TABLE public.apod_data TO postgres;
GRANT USAGE, SELECT ON SEQUENCE public.apod_data_id_seq TO postgres;

-- Insert a sample row for testing (optional - will be replaced by pipeline)
INSERT INTO public.apod_data (date, title, url, explanation, media_type, copyright)
VALUES (
    '2025-01-01',
    'Test Entry - Database Initialized',
    'https://apod.nasa.gov/apod/image/2501/sample.jpg',
    'This is a test entry created during database initialization. It will be replaced by actual APOD data from the pipeline.',
    'image',
    'Public Domain'
)
ON CONFLICT (date) DO NOTHING;

-- Display success message
DO $$
BEGIN
    RAISE NOTICE '✓ Database initialized successfully!';
    RAISE NOTICE '✓ Table: public.apod_data created';
    RAISE NOTICE '✓ Indexes created for optimal query performance';
    RAISE NOTICE '✓ Ready to receive APOD data from pipeline';
END $$;