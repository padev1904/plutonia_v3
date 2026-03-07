from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("news", "0007_article_editorial_review_fields"),
    ]

    operations = [
        migrations.AddField(
            model_name="newsletter",
            name="gmail_uid",
            field=models.CharField(blank=True, db_index=True, default="", max_length=50),
        ),
        migrations.AddField(
            model_name="newsletter",
            name="original_sender_email",
            field=models.CharField(blank=True, default="", max_length=255),
        ),
        migrations.AddField(
            model_name="newsletter",
            name="original_sender_name",
            field=models.CharField(blank=True, default="", max_length=255),
        ),
        migrations.AddField(
            model_name="newsletter",
            name="original_sent_at",
            field=models.DateTimeField(blank=True, db_index=True, null=True),
        ),
        migrations.AddField(
            model_name="newsletter",
            name="original_sent_at_raw",
            field=models.CharField(blank=True, default="", max_length=255),
        ),
        migrations.AlterModelOptions(
            name="newsletter",
            options={"ordering": ["original_sent_at", "received_at"]},
        ),
        migrations.RemoveIndex(
            model_name="newsletter",
            name="news_newsle_status_87dfb3_idx",
        ),
        migrations.AddIndex(
            model_name="newsletter",
            index=models.Index(
                fields=["status", "original_sent_at", "received_at"],
                name="news_news_status_orig_recv_idx",
            ),
        ),
    ]
