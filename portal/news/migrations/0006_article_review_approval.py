from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("news", "0005_alter_newsletter_status"),
    ]

    operations = [
        migrations.AddField(
            model_name="article",
            name="is_review_approved",
            field=models.BooleanField(db_index=True, default=False),
        ),
        migrations.AddField(
            model_name="article",
            name="review_approved_at",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddIndex(
            model_name="article",
            index=models.Index(fields=["is_review_approved", "-published_at"], name="news_art_approved_pub_idx"),
        ),
    ]
