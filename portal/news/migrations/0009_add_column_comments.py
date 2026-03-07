from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("news", "0008_newsletter_gmail_uid_original_sent_at"),
    ]

    operations = [
        migrations.RunSQL(
            sql="""
                COMMENT ON COLUMN news_newsletter.gmail_uid IS 'IMAP UID do email no Gmail';
                COMMENT ON COLUMN news_newsletter.original_sent_at IS 'Data UTC em que o sender original enviou o email';
                COMMENT ON COLUMN news_newsletter.original_sender_email IS 'Email do sender original da newsletter';
                COMMENT ON COLUMN news_newsletter.original_sender_name IS 'Nome do sender original da newsletter';
                COMMENT ON COLUMN news_newsletter.original_sent_at_raw IS 'Header Date original em formato bruto';
            """,
            reverse_sql="""
                COMMENT ON COLUMN news_newsletter.gmail_uid IS NULL;
                COMMENT ON COLUMN news_newsletter.original_sent_at IS NULL;
                COMMENT ON COLUMN news_newsletter.original_sender_email IS NULL;
                COMMENT ON COLUMN news_newsletter.original_sender_name IS NULL;
                COMMENT ON COLUMN news_newsletter.original_sent_at_raw IS NULL;
            """,
        ),
    ]
