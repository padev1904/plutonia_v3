from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("news", "0001_initial"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
                CREATE OR REPLACE FUNCTION update_article_search_vector()
                RETURNS trigger AS $$
                BEGIN
                    NEW.search_vector :=
                        setweight(to_tsvector('english', COALESCE(NEW.title, '')), 'A') ||
                        setweight(to_tsvector('english', COALESCE(NEW.summary, '')), 'B') ||
                        setweight(to_tsvector('english', COALESCE(NEW.enrichment_context, '')), 'C');
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

                CREATE TRIGGER article_search_vector_update
                    BEFORE INSERT OR UPDATE OF title, summary, enrichment_context
                    ON news_article
                    FOR EACH ROW
                    EXECUTE FUNCTION update_article_search_vector();
            """,
            reverse_sql="""
                DROP TRIGGER IF EXISTS article_search_vector_update ON news_article;
                DROP FUNCTION IF EXISTS update_article_search_vector();
            """,
        ),
    ]
